(ns sqlite-rate-limit-batched
  (:require
   [babashka.fs :as fs]
   [clojure.core.async :as a]
   [next.jdbc :as jdbc]
   [sturdy.sqlite.core :as sqlite.core]
   [sturdy.sqlite.ops :as sqlite.ops]
   [taoensso.telemere :as t])
  (:import
   (java.util UUID)
   (java.util.concurrent Executors TimeUnit Callable Future)))

;;; example usage
;;;
;; (defn wrap-rate-limit
;;   "Ring middleware for dynamic-batched rate limiting."
;;   [handler req-ch]
;;   (fn [request]
;;     ;; Extract the org-id from the request (e.g., from a JWT or API key)
;;     (let [org-id (:org-id request)
;;           now    (System/currentTimeMillis)
;;           ;; This blocks the Ring thread until the background batcher replies
;;           status (safe-admit-async! req-ch org-id now)]

;;       (if (= status :admit)
;;         ;; Let the request through to your API
;;         (handler request)
;;         ;; Reject it immediately
;;         {:status 429
;;          :headers {"Content-Type" "text/plain"}
;;          :body "Too Many Requests"}))))

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

(defn setup-db! [db-dir]
  (let [{:keys [datasource close-fn]}
        (sqlite.core/make-datasource "bench-rate-limit-batched" db-dir :high-performance)]
    (jdbc/execute! datasource ["DROP TABLE IF EXISTS api_minute_buckets"])
    (jdbc/execute! datasource ["
CREATE TABLE api_minute_buckets (
  organization_id BLOB    NOT NULL,
  bucket_start_ms INTEGER NOT NULL,
  request_count   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (organization_id, bucket_start_ms)
)"])
    {:ds datasource :close-fn close-fn}))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Database Logic (Runs inside the batched transaction)
;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- admit-request-inside-tx!
  "Executes the read and UPSERT for a single request inside an active transaction."
  [tx org-id now-ms]
  (let [bucket-ms    (bucket-start-ms now-ms)
        window-start (oldest-live-bucket-ms now-ms)
        row          (jdbc/execute-one! tx
                      ["SELECT COALESCE(SUM(request_count), 0) AS total
                        FROM api_minute_buckets
                        WHERE organization_id = ? AND bucket_start_ms >= ?"
                       org-id window-start])
        total        (long (:total row 0))]
    (if (< total limit-per-hour)
      (do
        (jdbc/execute-one! tx
          ["INSERT INTO api_minute_buckets (organization_id, bucket_start_ms, request_count)
            VALUES (?, ?, 1)
            ON CONFLICT (organization_id, bucket_start_ms)
            DO UPDATE SET request_count = request_count + 1"
           org-id bucket-ms])
        :admit)
      :reject)))

(defn- prune-inside-tx!
  "Executes the DELETE operation inside the active transaction."
  [tx now-ms]
  (let [cutoff (- (bucket-start-ms now-ms) hour-ms)]
    (jdbc/execute-one! tx ["DELETE FROM api_minute_buckets WHERE bucket_start_ms < ?" cutoff])))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; core.async Batching Engine
;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn start-batch-writer!
  "Starts a background thread that pulls from req-ch and executes in time/size bounded batches."
  [ds max-batch-size max-wait-ms]
  (let [req-ch (a/chan 50000)]
    (a/thread
      (loop []
        ;; Block indefinitely until at least one request arrives
        (when-let [first-req (a/<!! req-ch)]
          ;; Once we have a request, start a 10ms timer
          (let [timeout-ch (a/timeout max-wait-ms)
                batch      (loop [acc [first-req]
                                  n   1]
                             (if (= n max-batch-size)
                               acc
                               ;; Race the incoming requests against the timeout
                               (let [[val port] (a/alts!! [req-ch timeout-ch])]
                                 (if (= port timeout-ch)
                                   acc ;; Timer popped, flush the batch now
                                   (if val
                                     (recur (conj acc val) (inc n))
                                     acc)))))] ;; Channel closed, flush batch

            ;; Execute all operations in one physical commit. No retries needed!
            (try
              (sqlite.ops/with-immediate-transaction [tx ds]
                (doseq [req batch]
                  (case (:type req)
                    :admit (let [result (admit-request-inside-tx! tx (:org-id req) (:now-ms req))]
                             (a/put! (:resp-ch req) result))
                    :prune (prune-inside-tx! tx (:now-ms req)))))
              (catch Exception e
                ;; Only inform the :admit callers about errors, prune is fire-and-forget
                (doseq [req batch]
                  (when (= (:type req) :admit)
                    (a/put! (:resp-ch req) :error)))))
            (recur)))))
    req-ch))

(defn safe-admit-async!
  "Pushes an admit request to the queue and blocks until it is processed."
  [req-ch org-id t]
  (let [resp-ch (a/promise-chan)]
    (a/>!! req-ch {:type :admit :org-id org-id :now-ms t :resp-ch resp-ch})
    (a/<!! resp-ch)))

(defn async-prune!
  "Pushes a prune command to the queue. Returns immediately (fire and forget)."
  [req-ch t]
  (a/>!! req-ch {:type :prune :now-ms t}))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Benchmark Harness
;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn worker-task [req-ch org-ids n-ops prune-every]
  (fn []
    (loop [i 0 admits 0 rejects 0 errors 0]
      (if (= i n-ops)
        {:admits admits :rejects rejects :errors errors}
        (let [org-id (nth org-ids (mod i (count org-ids)))
              t      (now-ms)
              status (safe-admit-async! req-ch org-id t)]

          ;; Push a prune command into the single-writer queue
          (when (and prune-every (pos? prune-every) (zero? (mod (inc i) prune-every)))
            (async-prune! req-ch t))

          (recur (inc i)
                 (if (= status :admit) (inc admits) admits)
                 (if (= status :reject) (inc rejects) rejects)
                 (if (= status :error) (inc errors) errors)))))))

(defn run-concurrent-batched!
  [{:keys [req-ch org-ids n-threads ops-per-thread prune-every]}]
  (let [pool       (Executors/newFixedThreadPool n-threads)
        tasks      (mapv (fn [_]
                           (reify Callable
                             (call [_]
                               ((worker-task req-ch org-ids ops-per-thread prune-every)))))
                         (range n-threads))
        start-ns   (System/nanoTime)
        futures    (.invokeAll pool tasks)
        results    (mapv #(.get ^Future %) futures)
        _          (.shutdown pool)
        _          (.awaitTermination pool 1 TimeUnit/MINUTES)
        elapsed-ns (- (System/nanoTime) start-ns)
        total-ops  (* n-threads ops-per-thread)]
    {:mode        :dynamic-batching
     :n-threads   n-threads
     :n-ops       total-ops
     :admits      (reduce + (map :admits results))
     :rejects     (reduce + (map :rejects results))
     :errors      (reduce + (map :errors results))
     :elapsed-ms  (/ elapsed-ns 1e6)
     :ops-per-sec (/ total-ops (/ elapsed-ns 1e9))}))

(defn bench
  [{:keys [db-dir n-orgs n-threads ops-per-thread prune-every batch-size max-wait-ms]
    :or   {db-dir         ".bench/sqlite-rate-limit-batched"
           n-orgs         100
           n-threads      500
           ops-per-thread 160
           prune-every    1000
           batch-size     500
           max-wait-ms    10}}]
  (doseq [h (keys (t/get-handlers))]
    (t/remove-handler! h))
  (fs/delete-tree db-dir)
  (fs/create-dirs db-dir)

  (let [{:keys [ds close-fn]} (setup-db! db-dir)
        org-ids               (vec (repeatedly n-orgs random-org-id))
        req-ch                (start-batch-writer! ds batch-size max-wait-ms)]
    (try
      (println "\n== Bucketed / core.async Batched (With Async Prune) ==")
      (prn (run-concurrent-batched!
             {:req-ch         req-ch
              :org-ids        org-ids
              :n-threads      n-threads
              :ops-per-thread ops-per-thread
              :prune-every    prune-every}))
      (finally
        (a/close! req-ch)
        (close-fn)))))
