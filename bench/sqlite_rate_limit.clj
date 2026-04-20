(ns sqlite-rate-limit
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

(def ^:const minute-ms 60000)
(def ^:const hour-ms 3600000)
(def ^:const limit-per-hour 25000)

(defn now-ms []
  (System/currentTimeMillis))

(defn random-org-id []
  (UUID/randomUUID))

(defn bucket-start-ms
  ^long [^long now-ms]
  (* (quot now-ms minute-ms) minute-ms))

(defn oldest-live-bucket-ms
  ^long [^long now-ms]
  (- (bucket-start-ms now-ms) (* 59 minute-ms)))

(defn setup-shared-bucket-db!
  [db-dir]
  (let [{:keys [datasource close-fn]}
        (sqlite.core/make-datasource "bench-rate-limit-buckets" db-dir :high-performance)]
    (jdbc/execute! datasource ["DROP TABLE IF EXISTS api_minute_buckets"])
    (jdbc/execute! datasource ["CREATE TABLE api_minute_buckets (
                                 organization_id BLOB    NOT NULL,
                                 bucket_start_ms INTEGER NOT NULL,
                                 request_count   INTEGER NOT NULL DEFAULT 0,
                                 PRIMARY KEY (organization_id, bucket_start_ms)
                               )"])
    {:ds datasource
     :close-fn close-fn}))

(defn- admit-request-bucketed-once!
  [tx org-id now-ms]
  (let [bucket-ms    (bucket-start-ms now-ms)
        window-start (oldest-live-bucket-ms now-ms)
        row          (jdbc/execute-one!
                      tx
                      ["SELECT COALESCE(SUM(request_count), 0) AS total
                        FROM api_minute_buckets
                        WHERE organization_id = ?
                          AND bucket_start_ms >= ?"
                       org-id
                       window-start])
        total        (long (:total row 0))]
    (if (< total limit-per-hour)
      (do
        (jdbc/execute-one!
         tx
         ["INSERT INTO api_minute_buckets (organization_id, bucket_start_ms, request_count)
           VALUES (?, ?, 1)
           ON CONFLICT (organization_id, bucket_start_ms)
           DO UPDATE SET request_count = request_count + 1"
          org-id
          bucket-ms])
        true)
      false)))

(defn admit-request-bucketed-retrying!
  [ds org-id now-ms]
  (sqlite.ops/retry-sqlite
   #(sqlite.ops/with-immediate-transaction [tx ds]
      (admit-request-bucketed-once! tx org-id now-ms))
   :retries 10
   :base-delay-ms 2))

(defn admit-request-bucketed-strict!
  [ds org-id now-ms]
  (sqlite.ops/with-immediate-transaction [tx ds]
    (admit-request-bucketed-once! tx org-id now-ms)))

(defn prune-buckets!
  [ds now-ms]
  (let [cutoff (- (bucket-start-ms now-ms) hour-ms)]
    (jdbc/execute-one!
     ds
     ["DELETE FROM api_minute_buckets
       WHERE bucket_start_ms < ?"
      cutoff])))

(defn safe-admit!
  [{:keys [ds org-id t retry?]}]
  (try
    {:status (if ((if retry?
                    admit-request-bucketed-retrying!
                    admit-request-bucketed-strict!)
                  ds org-id t)
               :admit
               :reject)}
    (catch SQLException e
      (let [code (sqlite.ops/primary-err-code e)]
        (if (#{sqlite.ops/err-busy sqlite.ops/err-locked} code)
          {:status :busy}
          (throw e))))))

(defn run-serial!
  [{:keys [ds org-ids n-ops prune-every retry?]}]
  (let [start-ns (System/nanoTime)]
    (loop [i 0
           admits 0
           rejects 0
           busies 0]
      (if (= i n-ops)
        (let [elapsed-ns (- (System/nanoTime) start-ns)]
          {:mode        (if retry? :bucketed-serial-retrying :bucketed-serial-strict)
           :n-ops       n-ops
           :admits      admits
           :rejects     rejects
           :busies      busies
           :elapsed-ms  (/ elapsed-ns 1e6)
           :ops-per-sec (/ n-ops (/ elapsed-ns 1e9))})
        (let [org-id         (nth org-ids (mod i (count org-ids)))
              t              (now-ms)
              {:keys [status]} (safe-admit! {:ds ds :org-id org-id :t t :retry? retry?})]
          (when (and prune-every (pos? prune-every) (zero? (mod (inc i) prune-every)))
            (prune-buckets! ds t))
          (recur (inc i)
                 (if (= status :admit)  (inc admits) admits)
                 (if (= status :reject) (inc rejects) rejects)
                 (if (= status :busy)   (inc busies) busies)))))))

(defn worker-task
  [ds org-ids n-ops prune-every retry?]
  (fn []
    (loop [i 0
           admits 0
           rejects 0
           busies 0]
      (if (= i n-ops)
        {:admits admits :rejects rejects :busies busies}
        (let [org-id         (nth org-ids (mod i (count org-ids)))
              t              (now-ms)
              {:keys [status]} (safe-admit! {:ds ds :org-id org-id :t t :retry? retry?})]
          (when (and prune-every (pos? prune-every) (zero? (mod (inc i) prune-every)))
            (prune-buckets! ds t))
          (recur (inc i)
                 (if (= status :admit)  (inc admits) admits)
                 (if (= status :reject) (inc rejects) rejects)
                 (if (= status :busy)   (inc busies) busies)))))))

(defn run-concurrent!
  [{:keys [ds org-ids n-threads ops-per-thread prune-every retry?]}]
  (let [pool      (Executors/newFixedThreadPool n-threads)
        tasks     (mapv (fn [_]
                          ^Callable
                          (reify Callable
                            (call [_]
                              ((worker-task ds org-ids ops-per-thread prune-every retry?)))))
                        (range n-threads))
        start-ns  (System/nanoTime)
        futures   (.invokeAll pool tasks)
        results   (mapv #(.get ^Future %) futures)
        _         (.shutdown pool)
        _         (.awaitTermination pool 1 TimeUnit/MINUTES)
        elapsed-ns (- (System/nanoTime) start-ns)
        total-ops (* n-threads ops-per-thread)]
    {:mode        (if retry? :bucketed-concurrent-retrying :bucketed-concurrent-strict)
     :n-threads   n-threads
     :n-ops       total-ops
     :admits      (reduce + (map :admits results))
     :rejects     (reduce + (map :rejects results))
     :busies      (reduce + (map :busies results))
     :elapsed-ms  (/ elapsed-ns 1e6)
     :ops-per-sec (/ total-ops (/ elapsed-ns 1e9))}))

(defn bench
  [{:keys [db-dir n-orgs n-ops n-threads ops-per-thread prune-every]
    :or   {db-dir         ".bench/sqlite-rate-limit-buckets"
           n-orgs         100
           n-ops          50000
           n-threads      8
           ops-per-thread 10000
           prune-every    1000}}]
  (doseq [h (keys (t/get-handlers))]
    (t/remove-handler! h))
  (fs/create-dirs db-dir)
  (let [{:keys [ds close-fn]} (setup-shared-bucket-db! db-dir)
        org-ids (vec (repeatedly n-orgs random-org-id))]
    (try
      (println "\n== Bucketed / serial / strict ==")
      (prn (run-serial!
            {:ds ds
             :org-ids org-ids
             :n-ops n-ops
             :prune-every prune-every
             :retry? false}))

      (println "\n== Bucketed / concurrent / strict ==")
      (prn (run-concurrent!
            {:ds ds
             :org-ids org-ids
             :n-threads n-threads
             :ops-per-thread ops-per-thread
             :prune-every prune-every
             :retry? false}))

      (println "\n== Bucketed / concurrent / retrying ==")
      (prn (run-concurrent!
            {:ds ds
             :org-ids org-ids
             :n-threads n-threads
             :ops-per-thread ops-per-thread
             :prune-every prune-every
             :retry? true}))
      (finally
        (close-fn)))))
