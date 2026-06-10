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
                    (Thread/sleep ^long (long sleep-ms))
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

(defn start-batch-writer!
  "Starts a background thread that pulls from req-ch and executes in time/size bounded batches.
   Returns a map with the input channel (:req-ch) and the thread completion channel (:worker-ch)."
  [ds max-batch-size max-wait-ms global-builder-opts]
  (let [req-ch (a/chan 10000)]
    {:req-ch req-ch
     :worker-ch
     (a/thread
       (loop []
         (when-let [first-req (a/<!! req-ch)]
           (let [timeout-ch (a/timeout max-wait-ms)
                 batch      (loop [acc [first-req]
                                   n   1]
                              (if (= n max-batch-size)
                                acc
                                (let [[val port] (a/alts!! [req-ch timeout-ch])]
                                  (if (and (= port req-ch) val)
                                    (recur (conj acc val) (inc n))
                                    acc))))]
             (try
               (with-immediate-transaction [tx ds]
                 (doseq [{:keys [sql-vec opts resp-ch]} batch]
                   (try
                     (let [merged-opts (merge global-builder-opts opts)
                           res         (jdbc/execute! tx sql-vec merged-opts)]
                       (when resp-ch (a/put! resp-ch res)))
                     (catch Exception e
                       (when resp-ch (a/put! resp-ch e))))))
               (catch Exception global-e
                 (doseq [{:keys [resp-ch]} batch]
                   (when resp-ch (a/put! resp-ch global-e)))))
             (recur)))))}))

(defn close-batch-writer!
  [{:keys [req-ch worker-ch]}]
  (a/close! req-ch)
  (a/<!! worker-ch))

(defn execute-batched!
  "Pushes a standard JDBC sql-vec to the batch writer and blocks for the result.
   Acts as a drop-in replacement for (jdbc/execute! ds [\"...\"] opts)."
  [batch-sys sql-vec & [opts]]
  (let [resp-ch (a/promise-chan)]
    (a/>!! (:req-ch batch-sys) {:sql-vec sql-vec :opts opts :resp-ch resp-ch})
    (let [res (a/<!! resp-ch)]
      (if (instance? Throwable res)
        (throw res)
        res))))

(defn execute-batched-async!
  "Fire-and-forget version. Returns immediately."
  [batch-sys sql-vec & [opts]]
  (a/>!! (:req-ch batch-sys) {:sql-vec sql-vec :opts opts}))
