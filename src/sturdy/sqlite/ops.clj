(ns sturdy.sqlite.ops
  (:require
   [clojure.core.async :as a]
   [next.jdbc :as jdbc]
   [taoensso.telemere :as t])
  (:import
   (java.sql SQLException)
   (org.sqlite SQLiteException)
   (java.sql Connection Statement)
   (javax.sql DataSource)))

(set! *warn-on-reflection* true)

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Codes

;;; see https://sqlite.org/rescode.html

;; Primary Codes
(def err-busy       5)
(def err-locked     6)
(def err-constraint 19)

;; Extended Constraint Codes
(def err-constraint-notnull    1299)
(def err-constraint-primarykey 1555)
(def err-constraint-trigger    1811)
(def err-constraint-unique     2067)
(def err-constraint-datatype   3091)
(def err-constraint-foreignkey 787)

(defn err-code
  "Returns the full (extended) SQLite error code by bypassing standard JDBC
   and accessing the native SQLite driver result code."
  [^SQLException e]
  (if (instance? SQLiteException e)
    ;; Access the public `code` field on the internal SQLiteErrorCode enum
    (.-code (.getResultCode ^SQLiteException e))
    (.getErrorCode e)))

(defn primary-err-code
  "Masks the extended bits to return only the primary SQLite error category.
   Safe to use on both standard SQLExceptions and extended SQLiteExceptions."
  [^SQLException e]
  (bit-and (err-code e) 0xFF))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Busy retry wrapper

(defn retry-sqlite
  "Retries (f) up to `retries` times on ANY flavor of SQLITE_BUSY (5) or SQLITE_LOCKED (6).
   Uses randomized jitter for the delay to prevent thundering herd collisions."
  [f & {:keys [retries base-delay-ms] :or {retries 3, base-delay-ms 1000}}]
  (loop [n 0]
    (let [result
          (try
            {:ok (f)}
            (catch SQLException e
              ;; Use primary-err-code here so we catch extended busy states
              ;; like SQLITE_BUSY_TIMEOUT (773) as well as standard SQLITE_BUSY (5)
              (let [primary-code (primary-err-code e)]
                (if (and (< n retries)
                         (#{err-busy err-locked} primary-code))
                  (let [jitter   (- (rand-int base-delay-ms) (/ base-delay-ms 2))
                        sleep-ms (+ base-delay-ms jitter)]
                    (t/log! {:level :warn
                             :id    ::sqlite-busy
                             :msg   "SQLite contention detected. Retrying..."
                             :data  {:attempt   (inc n)
                                     :max       retries
                                     :err-code  (err-code e)
                                     :sleep-ms  sleep-ms
                                     :error     (.getMessage e)}})
                    (Thread/sleep (long sleep-ms))
                    :retry)
                  (throw e)))))]
      (if (= result :retry)
        (recur (inc n))
        (:ok result)))))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; with-immediate-transaction

(defprotocol ImmediateTransactable
  (-transact-immediate [this opts f]))

(defn- exec-sql!
  [^Connection conn ^String sql]
  (with-open [^Statement stmt (.createStatement conn)]
    (.execute stmt sql)))

(extend-protocol ImmediateTransactable
  DataSource
  (-transact-immediate [ds opts f]
    (with-open [^Connection conn (jdbc/get-connection ds opts)]
      (-transact-immediate conn opts f)))

  Connection
  (-transact-immediate [conn opts f]
    (when-not (.getAutoCommit conn)
      (throw (ex-info "Cannot start BEGIN IMMEDIATE on a connection that is already in a transaction."
                      {})))
    (try
      (exec-sql! conn "BEGIN IMMEDIATE")
      (let [tx  (jdbc/with-options conn opts)
            res (f tx)]
        (exec-sql! conn "COMMIT")
        res)
      (catch Throwable t
        (try
          (exec-sql! conn "ROLLBACK")
          (catch Throwable _))
        (throw t))))

  clojure.lang.IPersistentMap
  (-transact-immediate [m opts f]
    (let [opts (merge (:opts m) opts)]
      (-transact-immediate (:connectable m) opts f))))

(defmacro with-immediate-transaction
  [bindings & body]
  (let [[sym connectable opts] bindings]
    `(-transact-immediate ~connectable ~(or opts {}) (fn [~sym] ~@body))))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; core.async Batching Engine

(defn- pull-batch
  [first-req req-ch max-batch-size max-wait-ms]
  ;; start a timeout AFTER first request comes in
  (let [timeout-ch (a/timeout max-wait-ms)]
    (loop [acc [first-req]
           n   1]
      (if (= n max-batch-size)
        acc
        (let [[val port] (a/alts!! [req-ch timeout-ch])]
          (if (and (= port req-ch) val)
            (recur (conj acc val) (inc n))
            ;; timeout or channel closed -> send batch
            acc))))))

(defn- handle-async-error!
  [e sql-vec msg async-error-fn log?]
  (when log?
    (t/log! {:level :error
             :id    ::async-write-error
             :msg   msg
             :error e
             :data  {:sql (first sql-vec)}}))
  (when async-error-fn
    (try
      (async-error-fn e {:sql-vec sql-vec})
      (catch Exception cb-e
        (t/log! {:level :error
                 :id    ::async-error-fn-failed
                 :msg   "User async-error-fn callback threw an exception"
                 :error cb-e})))))

(defn- handle-global-error!
  [global-e batch async-error-fn]
  (let [async-reqs (filter #(nil? (:resp-ch %)) batch)]
    (when (seq async-reqs)
      (t/log! {:level :error
               :id    ::async-global-write-error
               :msg   "Async SQL batch write failed due to global transaction error"
               :error global-e
               :data  {:failed-count (count async-reqs)
                       :sample-sql   (mapv #(first (:sql-vec %)) (take 5 async-reqs))}})))
  (doseq [{:keys [sql-vec resp-ch]} batch]
    (if resp-ch
      (a/put! resp-ch global-e)
      (when async-error-fn
        (handle-async-error! global-e sql-vec nil async-error-fn false)))))

(defn- write-batch!
  [tx batch global-builder-opts async-error-fn]
  (reduce
   (fn [results {:keys [sql-vec opts resp-ch]}]
     (try
       (let [merged-opts (merge global-builder-opts opts)
             res         (jdbc/execute! tx sql-vec merged-opts)]
         (cond-> results resp-ch (conj [resp-ch res])))
       (catch Exception e
         (if resp-ch
           (conj results [resp-ch e])
           (do
             (handle-async-error! e sql-vec "Async SQL write failed (inner error)" async-error-fn true)
             results)))))
   []
   batch))

(defn- deliver-results!
  [results]
  (doseq [[resp-ch result] results]
    (a/put! resp-ch result)))

(defn start-batch-writer!
  "Starts a background thread that pulls from req-ch and executes in time/size bounded batches.
   Returns a map with the input channel (:req-ch) and the thread completion channel (:worker-ch)."
  ([ds max-batch-size max-wait-ms global-builder-opts]
   (start-batch-writer! ds max-batch-size max-wait-ms global-builder-opts nil))
  ([ds max-batch-size max-wait-ms global-builder-opts async-error-fn]
   (let [req-ch (a/chan 10000)]
     {:req-ch req-ch
      :worker-ch
      (a/thread
        (loop []
          ;; block until a request comes, so we don't spin on timeouts needlessly
          (when-let [first-req (a/<!! req-ch)]
            (let [batch (pull-batch first-req req-ch max-batch-size max-wait-ms)]
              (try
                (let [res (with-immediate-transaction [tx ds]
                            (write-batch! tx batch global-builder-opts async-error-fn))]
                  ;; transaction succeeded -> deliver results
                  (deliver-results! res))
                ;; catch COMMIT failure
                (catch Exception global-e
                  (handle-global-error! global-e batch async-error-fn)))
              (recur)))))})))

(defn close-batch-writer!
  [{:keys [req-ch worker-ch]}]
  (a/close! req-ch)
  (a/<!! worker-ch))

(defn execute-batched!
  "Pushes a standard JDBC sql-vec to the batch writer and blocks for the result.
   Acts as a drop-in replacement for (jdbc/execute! ds [\"...\"] opts)."
  [batch-sys sql-vec & [opts]]
  (let [resp-ch (a/promise-chan)
        req     {:sql-vec sql-vec :opts opts :resp-ch resp-ch}]
    (if (a/>!! (:req-ch batch-sys) req)
      (let [res (a/<!! resp-ch)]
        (if (instance? Throwable res)
          (throw res)
          res))
      (throw (ex-info "SQLite batch writer is closed" {:sql-vec sql-vec})))))

(defn execute-batched-async!
  "Fire-and-forget version. Returns immediately."
  [batch-sys sql-vec & [opts]]
  (when-not (a/>!! (:req-ch batch-sys) {:sql-vec sql-vec :opts opts})
    (throw (ex-info "SQLite batch writer is closed" {:sql-vec sql-vec}))))
