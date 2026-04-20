(ns sqlite-rate-limit
  (:require
   [babashka.fs :as fs]
   [next.jdbc :as jdbc]
   [sturdy.sqlite.core :as sqlite.core]
   [sturdy.sqlite.ops :as sqlite.ops]
   [sturdy.sqlite.types :as types]
   [taoensso.telemere :as t])
  (:import
   (java.util UUID)
   (java.util.concurrent Executors TimeUnit Callable Future)))

(set! *warn-on-reflection* true)

(def ^:const minute-ms 60000)
(def ^:const hour-ms 3600000)
(def ^:const limit-per-hour 25000)

(defn now-ms [] (System/currentTimeMillis))
(defn random-org-id [] (UUID/randomUUID))

(defn bucket-start-ms ^long [^long now-ms]
  (* (quot now-ms minute-ms) minute-ms))

(defn oldest-live-bucket-ms ^long [^long now-ms]
  (- (bucket-start-ms now-ms) (* 59 minute-ms)))

(def b-opts (types/make-builder-opts {}))

(defn setup-db! [db-name db-dir profile-key batch-size]
  (let [sys (sqlite.core/make-datasource db-name db-dir profile-key
                                         {:batch-size batch-size
                                          :builder-opts b-opts})
        ds  (:datasource sys)]
    (jdbc/execute! ds ["DROP TABLE IF EXISTS api_minute_buckets"])
    (jdbc/execute! ds ["
CREATE TABLE api_minute_buckets (
  organization_id BLOB    NOT NULL,
  bucket_start_ms INTEGER NOT NULL,
  request_count   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (organization_id, bucket_start_ms)
)"])
    sys))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Unified SQL Statements

(defn admit-sql
  "A single atomic statement that checks the rate limit and UPSERTs if allowed.
   Returns 1 updated row if admitted, or 0 if rejected by the WHERE clause."
  [org-id bucket-ms window-start limit]
  ["INSERT INTO api_minute_buckets (organization_id, bucket_start_ms, request_count)
    SELECT ?, ?, 1
    WHERE (SELECT COALESCE(SUM(request_count), 0)
           FROM api_minute_buckets
           WHERE organization_id = ? AND bucket_start_ms >= ?) < ?
    ON CONFLICT (organization_id, bucket_start_ms)
    DO UPDATE SET request_count = request_count + 1"
   org-id bucket-ms org-id window-start limit])

(defn prune-sql [now-ms]
  (let [cutoff (- (bucket-start-ms now-ms) hour-ms)]
    ["DELETE FROM api_minute_buckets WHERE bucket_start_ms < ?" cutoff]))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Worker Tasks

(defn batched-worker-task [{:keys [write-fn write-async-fn]} org-ids n-ops prune-every]
  (fn []
    (loop [i 0 admits 0 rejects 0 errors 0]
      (if (= i n-ops)
        {:admits admits :rejects rejects :errors errors}
        (let [org-id (nth org-ids (mod i (count org-ids)))
              t      (now-ms)
              b-ms   (bucket-start-ms t)
              w-ms   (oldest-live-bucket-ms t)
              sql    (admit-sql org-id b-ms w-ms limit-per-hour)

              ;; Use the queue for writes
              status (try
                       (let [res (write-fn sql)]
                         (if (pos? (-> res first :next.jdbc/update-count)) :admit :reject))
                       (catch Exception _ :error))]

          ;; Push a prune command into the queue (fire-and-forget)
          (when (and prune-every (pos? prune-every) (zero? (mod (inc i) prune-every)))
            (write-async-fn (prune-sql t)))

          (recur (inc i)
                 (if (= status :admit) (inc admits) admits)
                 (if (= status :reject) (inc rejects) rejects)
                 (if (= status :error) (inc errors) errors)))))))

(defn unbatched-worker-task [{:keys [datasource]} org-ids n-ops prune-every]
  (fn []
    (loop [i 0 admits 0 rejects 0 errors 0]
      (if (= i n-ops)
        {:admits admits :rejects rejects :errors errors}
        (let [org-id (nth org-ids (mod i (count org-ids)))
              t      (now-ms)
              b-ms   (bucket-start-ms t)
              w-ms   (oldest-live-bucket-ms t)
              sql    (admit-sql org-id b-ms w-ms limit-per-hour)

              ;; Use immediate transactions and retry logic
              status (try
                       (sqlite.ops/retry-sqlite
                        (fn []
                          (sqlite.ops/with-immediate-transaction [tx datasource]
                            (let [res (jdbc/execute! tx sql b-opts)]
                              (if (pos? (-> res first :next.jdbc/update-count)) :admit :reject)))))
                       (catch Exception _ :error))]

          ;; Prune synchronously using immediate transactions
          (when (and prune-every (pos? prune-every) (zero? (mod (inc i) prune-every)))
            (try
              (sqlite.ops/retry-sqlite
               (fn []
                 (sqlite.ops/with-immediate-transaction [tx datasource]
                   (jdbc/execute! tx (prune-sql t) b-opts))))
              (catch Exception _)))

          (recur (inc i)
                 (if (= status :admit) (inc admits) admits)
                 (if (= status :reject) (inc rejects) rejects)
                 (if (= status :error) (inc errors) errors)))))))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Benchmark Harness

(defn run-concurrent! [{:keys [mode sys worker-fn-maker org-ids n-threads ops-per-thread prune-every]}]
  (let [pool       (Executors/newFixedThreadPool n-threads)
        tasks      (mapv (fn [_]
                           (reify Callable
                             (call [_]
                               ((worker-fn-maker sys org-ids ops-per-thread prune-every)))))
                         (range n-threads))
        start-ns   (System/nanoTime)
        futures    (.invokeAll pool tasks)
        results    (mapv #(.get ^Future %) futures)
        _          (.shutdown pool)
        _          (.awaitTermination pool 1 TimeUnit/MINUTES)
        elapsed-ns (- (System/nanoTime) start-ns)
        total-ops  (* n-threads ops-per-thread)]
    {:mode        mode
     :n-threads   n-threads
     :n-ops       total-ops
     :admits      (reduce + (map :admits results))
     :rejects     (reduce + (map :rejects results))
     :errors      (reduce + (map :errors results))
     :elapsed-ms  (/ elapsed-ns 1e6)
     :ops-per-sec (float (/ total-ops (/ elapsed-ns 1e9)))}))

(defn bench
  [{:keys [db-dir n-orgs n-threads ops-per-thread prune-every batch-size]
    :or   {db-dir         ".bench/sqlite-rate-limit"
           n-orgs         100
           n-threads      500
           ops-per-thread 160
           prune-every    1000
           batch-size     500}}]

  ;; Suppress telemetry logs so they don't spam the benchmark output
  (doseq [h (keys (t/get-handlers))]
    (t/remove-handler! h))
  (fs/delete-tree db-dir)
  (fs/create-dirs db-dir)

  (let [org-ids (vec (repeatedly n-orgs random-org-id))]

    (try
      (println "\n== Unbatched (BEGIN IMMEDIATE + Retry Jitter) ==")
      (let [sys (setup-db! "unbatched" db-dir :low-resource batch-size)]
        (prn (run-concurrent! {:mode            :unbatched
                               :sys             sys
                               :worker-fn-maker unbatched-worker-task
                               :org-ids         org-ids
                               :n-threads       n-threads
                               :ops-per-thread  ops-per-thread
                               :prune-every     prune-every}))
        ((:close-fn sys)))

      (println "\n== Batched (Single Writer Queue) ==")
      (let [sys (setup-db! "batched" db-dir :low-resource batch-size)]
        (prn (run-concurrent! {:mode            :batched
                               :sys             sys
                               :worker-fn-maker batched-worker-task
                               :org-ids         org-ids
                               :n-threads       n-threads
                               :ops-per-thread  ops-per-thread
                               :prune-every     prune-every}))
        ((:close-fn sys)))

      (finally
        (fs/delete-tree db-dir)))))
