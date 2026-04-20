(ns sqlite-rate-limit-dumb
  (:require
   [babashka.fs :as fs]
   [next.jdbc :as jdbc]
   [sturdy.sqlite.core :as sqlite.core]
   [sturdy.sqlite.ops :as sqlite.ops]
   [taoensso.telemere :as t])
  (:import
   (java.sql SQLException)
   (java.util UUID)
   (java.util.concurrent Executors TimeUnit Callable Future)))

(set! *warn-on-reflection* true)

(def ^:const hour-ms 3600000)
(def ^:const limit-per-hour 25000)

(defn now-ms []
  (System/currentTimeMillis))

(defn random-org-id []
  (UUID/randomUUID))

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
     ["DELETE FROM api_requests WHERE timestamp_ms <= ?" cutoff-ms])))

(defn safe-admit!
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
    (loop [i        0
           admits   0
           rejects  0
           busies   0]
      (if (= i n-ops)
        (let [elapsed-ns (- (System/nanoTime) start-ns)
              elapsed-s  (/ elapsed-ns 1e9)]
          {:admits         admits
           :rejects        rejects
           :busies         busies
           :elapsed-ms     (/ elapsed-ns 1e6)
           :ops-per-sec    (/ n-ops elapsed-s)})
        (let [org-id (nth org-ids (mod i (count org-ids)))
              t      (now-ms)
              {:keys [status]} (safe-admit! {:ds ds :org-id org-id :t t :retry? false})]
          (when (and prune-every (pos? prune-every) (zero? (mod (inc i) prune-every)))
            (prune-shared! ds (- t hour-ms)))
          (recur (inc i)
                 (if (= status :admit)  (inc admits)  admits)
                 (if (= status :reject) (inc rejects) rejects)
                 (if (= status :busy)   (inc busies)  busies)))))))

(defn worker-task-shared
  [ds org-ids n-ops prune-every retry?]
  (fn []
    (loop [i       0
           admits  0
           rejects 0
           busies  0]
      (if (= i n-ops)
        {:admits admits :rejects rejects :busies busies}
        (let [org-id (nth org-ids (mod i (count org-ids)))
              t      (now-ms)
              {:keys [status]} (safe-admit! {:ds ds
                                             :org-id org-id
                                             :t t
                                             :retry? retry?})]
          (when (and prune-every (pos? prune-every) (zero? (mod (inc i) prune-every)))
            (prune-shared! ds (- t hour-ms)))
          (recur (inc i)
                 (if (= status :admit)  (inc admits) admits)
                 (if (= status :reject) (inc rejects) rejects)
                 (if (= status :busy)   (inc busies) busies)))))))

(defn run-concurrent-shared!
  [{:keys [ds org-ids n-threads ops-per-thread prune-every retry?]}]
  (let [pool     (Executors/newFixedThreadPool n-threads)
        tasks    (mapv (fn [_]
                         ^Callable
                         (reify Callable
                           (call [_]
                             ((worker-task-shared ds org-ids ops-per-thread prune-every retry?)))))
                       (range n-threads))
        start-ns (System/nanoTime)
        futures  (.invokeAll pool tasks)
        results  (mapv #(.get ^Future %) futures)
        _        (.shutdown pool)
        _        (.awaitTermination pool 1 TimeUnit/MINUTES)
        elapsed-ns (- (System/nanoTime) start-ns)
        total-ops  (* n-threads ops-per-thread)]
    {:mode         (if retry? :shared-concurrent-retrying :shared-concurrent-strict)
     :n-threads    n-threads
     :n-ops        total-ops
     :admits       (reduce + (map :admits results))
     :rejects      (reduce + (map :rejects results))
     :busies       (reduce + (map :busies results))
     :elapsed-ms   (/ elapsed-ns 1e6)
     :ops-per-sec  (/ total-ops (/ elapsed-ns 1e9))}))

(defn bench
  [{:keys [db-dir n-orgs n-ops n-threads ops-per-thread prune-every]
    :or   {db-dir         ".bench/sqlite-rate-limit"
           n-orgs         100
           n-ops          50000
           n-threads      8
           ops-per-thread 10000
           prune-every    1000}}]
  (doseq [h (keys (t/get-handlers))]
    (t/remove-handler! h))
  (fs/create-dirs db-dir)
  (let [{:keys [ds close-fn]} (setup-shared-db! db-dir)
        org-ids (vec (repeatedly n-orgs random-org-id))]
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

      (finally
        (close-fn)))))
