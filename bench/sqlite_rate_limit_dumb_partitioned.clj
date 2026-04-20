(ns sqlite-rate-limit-dumb-partitioned
  (:require
   [babashka.fs :as fs]
   [next.jdbc :as jdbc]
   [sturdy.sqlite.core :as sqlite.core]
   [sturdy.sqlite.ops :as sqlite.ops]
   [taoensso.telemere :as t])
  (:import
   (java.sql SQLException DriverManager Connection)
   (java.util UUID)
   (java.util.concurrent Executors TimeUnit Callable Future)))

(set! *warn-on-reflection* true)

(def ^:const hour-ms 3600000)
(def ^:const limit-per-hour 25000)

(defn now-ms []
  (System/currentTimeMillis))

(defn random-org-id []
  (UUID/randomUUID))

(defn uuid->hex
  ^String [^UUID u]
  (str u))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; shared-db benchmark

(defn setup-shared-db!
  [db-dir]
  (let [{:keys [datasource close-fn]}
        (sqlite.core/make-datasource "bench-rate-limit" db-dir :high-performance)]
    (jdbc/execute! datasource ["DROP TABLE IF EXISTS api_requests"])
    (jdbc/execute! datasource ["
CREATE TABLE api_requests (
  organization_id BLOB NOT NULL,
  timestamp_ms    INTEGER NOT NULL
)
"])
    (jdbc/execute! datasource ["
CREATE INDEX idx_api_requests_org_ts
    ON api_requests(organization_id, timestamp_ms)
"])
    {:ds datasource
     :close-fn close-fn}))

(defn- admit-request-shared-once!
  [tx org-id now-ms]
  (let [window-start (- now-ms hour-ms)
        res (jdbc/execute-one!
             tx
             ["
INSERT INTO api_requests (organization_id, timestamp_ms)
SELECT ?, ?
WHERE (
    SELECT COUNT(*)
    FROM api_requests
    WHERE organization_id = ?
      AND timestamp_ms > ?
) < ?
RETURNING 1
"
              org-id
              now-ms
              org-id
              window-start
              limit-per-hour])]
    (boolean res)))

(defn admit-request-shared-retrying!
  [ds org-id now-ms]
  (sqlite.ops/retry-sqlite
   #(sqlite.ops/with-immediate-transaction [tx ds]
      (admit-request-shared-once! tx org-id now-ms))
   :retries 10
   :base-delay-ms 2))

(defn admit-request-shared-strict!
  [ds org-id now-ms]
  (sqlite.ops/with-immediate-transaction [tx ds]
    (admit-request-shared-once! tx org-id now-ms)))

(defn prune-shared!
  [ds cutoff-ms]
  (sqlite.ops/retry-sqlite
   #(jdbc/execute-one!
     ds
     ["DELETE FROM api_requests WHERE timestamp_ms <= ?" cutoff-ms])
   :retries 10
   :base-delay-ms 2))

(defn safe-admit-shared!
  [{:keys [ds org-id t retry?]}]
  (try
    {:status (if ((if retry?
                    admit-request-shared-retrying!
                    admit-request-shared-strict!)
                  ds org-id t)
               :admit
               :reject)}
    (catch SQLException e
      (let [code (sqlite.ops/primary-err-code e)]
        (if (#{sqlite.ops/err-busy sqlite.ops/err-locked} code)
          {:status :busy}
          (throw e))))))

(defn run-serial-shared!
  [{:keys [ds org-ids n-ops prune-every]}]
  (let [start-ns (System/nanoTime)]
    (loop [i 0
           admits 0
           rejects 0
           busies 0]
      (if (= i n-ops)
        (let [elapsed-ns (- (System/nanoTime) start-ns)
              elapsed-s  (/ elapsed-ns 1e9)]
          {:mode        :shared-serial
           :n-ops       n-ops
           :admits      admits
           :rejects     rejects
           :busies      busies
           :elapsed-ms  (/ elapsed-ns 1e6)
           :ops-per-sec (/ n-ops elapsed-s)})
        (let [org-id           (nth org-ids (mod i (count org-ids)))
              t                (now-ms)
              {:keys [status]} (safe-admit-shared!
                                {:ds ds :org-id org-id :t t :retry? false})]
          (when (and prune-every (pos? prune-every) (zero? (mod (inc i) prune-every)))
            (prune-shared! ds (- t hour-ms)))
          (recur (inc i)
                 (if (= status :admit)  (inc admits) admits)
                 (if (= status :reject) (inc rejects) rejects)
                 (if (= status :busy)   (inc busies) busies)))))))

(defn worker-task-shared
  [ds org-ids n-ops prune-every retry?]
  (fn []
    (loop [i 0
           admits 0
           rejects 0
           busies 0]
      (if (= i n-ops)
        {:admits admits :rejects rejects :busies busies}
        (let [org-id           (nth org-ids (mod i (count org-ids)))
              t                (now-ms)
              {:keys [status]} (safe-admit-shared!
                                {:ds ds :org-id org-id :t t :retry? retry?})]
          (when (and prune-every (pos? prune-every) (zero? (mod (inc i) prune-every)))
            (prune-shared! ds (- t hour-ms)))
          (recur (inc i)
                 (if (= status :admit)  (inc admits) admits)
                 (if (= status :reject) (inc rejects) rejects)
                 (if (= status :busy)   (inc busies) busies)))))))

(defn run-concurrent-shared!
  [{:keys [ds org-ids n-threads ops-per-thread prune-every retry?]}]
  (let [pool      (Executors/newFixedThreadPool n-threads)
        tasks     (mapv (fn [_]
                          ^Callable
                          (reify Callable
                            (call [_]
                              ((worker-task-shared ds org-ids ops-per-thread prune-every retry?)))))
                        (range n-threads))
        start-ns  (System/nanoTime)
        futures   (.invokeAll pool tasks)
        results   (mapv #(.get ^Future %) futures)
        _         (.shutdown pool)
        _         (.awaitTermination pool 1 TimeUnit/MINUTES)
        elapsed-ns (- (System/nanoTime) start-ns)
        total-ops (* n-threads ops-per-thread)]
    {:mode        (if retry? :shared-concurrent-retrying :shared-concurrent-strict)
     :n-threads   n-threads
     :n-ops       total-ops
     :admits      (reduce + (map :admits results))
     :rejects     (reduce + (map :rejects results))
     :busies      (reduce + (map :busies results))
     :elapsed-ms  (/ elapsed-ns 1e6)
     :ops-per-sec (/ total-ops (/ elapsed-ns 1e9))}))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; per-org-db benchmark

(defn org-db-path
  [db-dir org-id]
  (fs/path db-dir "orgs" (str (uuid->hex org-id) ".db")))

(defn init-org-db!
  [db-dir org-id]
  (let [db-path (org-db-path db-dir org-id)]
    (fs/create-dirs (fs/parent db-path))
    (with-open [conn (DriverManager/getConnection (str "jdbc:sqlite:" (str db-path)))]
      (jdbc/execute! conn ["PRAGMA journal_mode=WAL"])
      (jdbc/execute! conn ["PRAGMA synchronous=NORMAL"])
      (jdbc/execute! conn ["PRAGMA busy_timeout=5000"])
      (jdbc/execute! conn ["PRAGMA temp_store=MEMORY"])
      (jdbc/execute! conn ["CREATE TABLE IF NOT EXISTS api_requests (
                              timestamp_ms INTEGER NOT NULL
                            )"])
      (jdbc/execute! conn ["CREATE INDEX IF NOT EXISTS idx_api_requests_ts
                            ON api_requests(timestamp_ms)"])))
  {:jdbc-url (str "jdbc:sqlite:" (str (org-db-path db-dir org-id)))})

(defn setup-per-org-dbs!
  [db-dir org-ids]
  (let [org->db (into {}
                      (map (fn [org-id]
                             [org-id (init-org-db! db-dir org-id)]))
                      org-ids)]
    {:org->db org->db
     :close-fn (fn [] nil)}))

(defn with-org-conn
  [jdbc-url f]
  (with-open [^Connection conn (DriverManager/getConnection jdbc-url)]
    (f conn)))

(defn- admit-request-org-once!
  [tx now-ms]
  (let [window-start (- now-ms hour-ms)
        res (jdbc/execute-one!
             tx
             ["
INSERT INTO api_requests (timestamp_ms)
SELECT ?
WHERE (
    SELECT COUNT(*)
    FROM api_requests
    WHERE timestamp_ms > ?
) < ?
RETURNING 1
"
              now-ms
              window-start
              limit-per-hour])]
    (boolean res)))

(defn admit-request-org-retrying!
  [jdbc-url now-ms]
  (sqlite.ops/retry-sqlite
   #(with-org-conn jdbc-url
      (fn [conn]
        (sqlite.ops/with-immediate-transaction [tx conn]
          (admit-request-org-once! tx now-ms))))
   :retries 10
   :base-delay-ms 2))

(defn admit-request-org-strict!
  [jdbc-url now-ms]
  (with-org-conn jdbc-url
    (fn [conn]
      (sqlite.ops/with-immediate-transaction [tx conn]
        (admit-request-org-once! tx now-ms)))))

(defn prune-org!
  [jdbc-url cutoff-ms]
  (sqlite.ops/retry-sqlite
   #(with-org-conn jdbc-url
      (fn [conn]
        (jdbc/execute-one!
         conn
         ["DELETE FROM api_requests WHERE timestamp_ms <= ?" cutoff-ms])))
   :retries 10
   :base-delay-ms 2))

(defn safe-admit-org!
  [{:keys [jdbc-url t retry?]}]
  (try
    {:status (if ((if retry?
                    admit-request-org-retrying!
                    admit-request-org-strict!)
                  jdbc-url t)
               :admit
               :reject)}
    (catch SQLException e
      (let [code (sqlite.ops/primary-err-code e)]
        (if (#{sqlite.ops/err-busy sqlite.ops/err-locked} code)
          {:status :busy}
          (throw e))))))

(defn run-serial-per-org!
  [{:keys [org->db org-ids n-ops prune-every]}]
  (let [start-ns (System/nanoTime)]
    (loop [i 0
           admits 0
           rejects 0
           busies 0]
      (if (= i n-ops)
        (let [elapsed-ns (- (System/nanoTime) start-ns)
              elapsed-s  (/ elapsed-ns 1e9)]
          {:mode        :per-org-serial
           :n-ops       n-ops
           :admits      admits
           :rejects     rejects
           :busies      busies
           :elapsed-ms  (/ elapsed-ns 1e6)
           :ops-per-sec (/ n-ops elapsed-s)})
        (let [org-id           (nth org-ids (mod i (count org-ids)))
              jdbc-url         (:jdbc-url (get org->db org-id))
              t                (now-ms)
              {:keys [status]} (safe-admit-org!
                                {:jdbc-url jdbc-url :t t :retry? false})]
          (when (and prune-every (pos? prune-every) (zero? (mod (inc i) prune-every)))
            (prune-org! jdbc-url (- t hour-ms)))
          (recur (inc i)
                 (if (= status :admit)  (inc admits) admits)
                 (if (= status :reject) (inc rejects) rejects)
                 (if (= status :busy)   (inc busies) busies)))))))

(defn worker-task-per-org
  [org->db org-ids n-ops prune-every retry?]
  (fn []
    (loop [i 0
           admits 0
           rejects 0
           busies 0]
      (if (= i n-ops)
        {:admits admits :rejects rejects :busies busies}
        (let [org-id           (nth org-ids (mod i (count org-ids)))
              jdbc-url         (:jdbc-url (get org->db org-id))
              t                (now-ms)
              {:keys [status]} (safe-admit-org!
                                {:jdbc-url jdbc-url :t t :retry? retry?})]
          (when (and prune-every (pos? prune-every) (zero? (mod (inc i) prune-every)))
            (prune-org! jdbc-url (- t hour-ms)))
          (recur (inc i)
                 (if (= status :admit)  (inc admits) admits)
                 (if (= status :reject) (inc rejects) rejects)
                 (if (= status :busy)   (inc busies) busies)))))))

(defn run-concurrent-per-org!
  [{:keys [org->db org-ids n-threads ops-per-thread prune-every retry?]}]
  (let [pool      (Executors/newFixedThreadPool n-threads)
        tasks     (mapv (fn [thread-idx]
                          ^Callable
                          (reify Callable
                            (call [_]
                              ;; Spread workers across org IDs so they are less likely
                              ;; to hammer the same DB file in lockstep.
                              (let [rotated-org-ids (vec (concat (drop (mod thread-idx (count org-ids)) org-ids)
                                                                 (take (mod thread-idx (count org-ids)) org-ids)))]
                                ((worker-task-per-org org->db rotated-org-ids ops-per-thread prune-every retry?))))))
                        (range n-threads))
        start-ns  (System/nanoTime)
        futures   (.invokeAll pool tasks)
        results   (mapv #(.get ^Future %) futures)
        _         (.shutdown pool)
        _         (.awaitTermination pool 1 TimeUnit/MINUTES)
        elapsed-ns (- (System/nanoTime) start-ns)
        total-ops (* n-threads ops-per-thread)]
    {:mode        (if retry? :per-org-concurrent-retrying :per-org-concurrent-strict)
     :n-threads   n-threads
     :n-ops       total-ops
     :admits      (reduce + (map :admits results))
     :rejects     (reduce + (map :rejects results))
     :busies      (reduce + (map :busies results))
     :elapsed-ms  (/ elapsed-ns 1e6)
     :ops-per-sec (/ total-ops (/ elapsed-ns 1e9))}))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; entrypoint

(defn bench
  [{:keys [db-dir n-orgs n-ops n-threads ops-per-thread prune-every]
    :or   {db-dir         ".bench/sqlite-rate-limit"
           n-orgs         100
           n-ops          50000
           n-threads      8
           ops-per-thread 10000
           prune-every    nil}}]
  (doseq [h (keys (t/get-handlers))]
    (t/remove-handler! h))
  (fs/delete-tree db-dir)
  (fs/create-dirs db-dir)
  (let [org-ids               (vec (repeatedly n-orgs random-org-id))
        {:keys [ds close-fn]} (setup-shared-db! db-dir)
        {:keys [org->db close-fn close-fn-per-org] :as per-org}
        (setup-per-org-dbs! db-dir org-ids)]
    (try
      (println "\n== Shared DB / serial ==")
      (prn (run-serial-shared!
            {:ds ds
             :org-ids org-ids
             :n-ops n-ops
             :prune-every prune-every}))

      (println "\n== Shared DB / concurrent / strict ==")
      (prn (run-concurrent-shared!
            {:ds ds
             :org-ids org-ids
             :n-threads n-threads
             :ops-per-thread ops-per-thread
             :prune-every prune-every
             :retry? false}))

      (println "\n== Shared DB / concurrent / retrying ==")
      (prn (run-concurrent-shared!
            {:ds ds
             :org-ids org-ids
             :n-threads n-threads
             :ops-per-thread ops-per-thread
             :prune-every prune-every
             :retry? true}))

      (println "\n== Per-org DB / serial ==")
      (prn (run-serial-per-org!
            {:org->db org->db
             :org-ids org-ids
             :n-ops n-ops
             :prune-every prune-every}))

      (println "\n== Per-org DB / concurrent / strict ==")
      (prn (run-concurrent-per-org!
            {:org->db org->db
             :org-ids org-ids
             :n-threads n-threads
             :ops-per-thread ops-per-thread
             :prune-every prune-every
             :retry? false}))

      (println "\n== Per-org DB / concurrent / retrying ==")
      (prn (run-concurrent-per-org!
            {:org->db org->db
             :org-ids org-ids
             :n-threads n-threads
             :ops-per-thread ops-per-thread
             :prune-every prune-every
             :retry? true}))
      (finally
        (close-fn)))))
