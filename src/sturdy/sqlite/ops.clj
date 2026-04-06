(ns sturdy.sqlite.ops
  (:require
   [taoensso.telemere :as t])
  (:import
   (java.sql SQLException)
   (org.sqlite SQLiteException)))

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
