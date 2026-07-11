(ns sturdy.sqlite.ops-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as sql]
   [sturdy.sqlite.ops :as ops]
   [sturdy.sqlite.core :as core]
   [sturdy.sqlite.test :refer [with-test-db]]
   [sturdy.sqlite.test-support :as ts])
  (:import
   (java.sql Connection SQLException)))

(set! *warn-on-reflection* true)

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; retry-sqlite

(deftest retry-sqlite-test
  (ts/with-quiet-logging
   (testing "Retries exactly up to the limit on SQLITE_BUSY (5)"
     (let [attempts (atom 0)]
       (is (thrown? SQLException
                    (ops/retry-sqlite
                     (fn []
                       (swap! attempts inc)
                       (throw (SQLException. "Busy!" "HY000" 5)))
                     :retries 3
                     ;; Set base delay to 1ms so tests run instantly
                     :base-delay-ms 1)))
       (is (= 4 @attempts)
           "Initial try + 3 retries = 4 total attempts")))

    (testing "Succeeds if it recovers before the limit is reached"
      (let [attempts (atom 0)
            result   (ops/retry-sqlite
                      (fn []
                        (swap! attempts inc)
                        (if (< @attempts 3)
                          (throw (SQLException. "Locked!" "HY000" 6))
                          :success))
                      :retries 3
                      :base-delay-ms 1)]
        (is (= :success result))
        (is (= 3 @attempts))))

    (testing "Fails immediately on non-busy SQL errors"
      (let [attempts (atom 0)]
        (is (thrown? SQLException
                     (ops/retry-sqlite
                      (fn []
                        (swap! attempts inc)
                        ;; Generic error code (e.g., 1 for syntax error)
                        (throw (SQLException. "Syntax error" "HY000" 1)))
                      :retries 3
                      :base-delay-ms 1)))
        (is (= 1 @attempts) "Must not retry on standard SQL errors")))))

(deftest retry-sqlite-option-validation-test
  (testing "Retry options reject invalid values before invoking the operation"
    (doseq [retries [-1 1.5 nil]]
      (let [called? (atom false)]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"retries"
             (ops/retry-sqlite #(reset! called? true) :retries retries))
            (str "Invalid retries: " (pr-str retries)))
        (is (false? @called?))))

    (doseq [base-delay-ms [0 -1 1.5 nil]]
      (let [called? (atom false)]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"base-delay-ms"
             (ops/retry-sqlite #(reset! called? true)
                               :base-delay-ms base-delay-ms))
            (str "Invalid base-delay-ms: " (pr-str base-delay-ms)))
        (is (false? @called?))))))

(deftest batch-writer-option-validation-test
  (testing "Batching options reject invalid values before starting a worker"
    (doseq [batch-size [0 -1 1.5 nil]]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"max-batch-size"
           (ops/start-batch-writer! ::ds batch-size 10 {}))
          (str "Invalid max-batch-size: " (pr-str batch-size))))

    (doseq [batch-wait-ms [-1 1.5 nil]]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"max-wait-ms"
           (ops/start-batch-writer! ::ds 10 batch-wait-ms {}))
          (str "Invalid max-wait-ms: " (pr-str batch-wait-ms))))))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; SQLite error code extraction

(deftest extended-error-codes-test
  (testing "Extracts extended error codes natively from the JDBC driver"
    (with-test-db [{:keys [datasource]} "error-codes-db"]
      ;; 1. Setup a table with a UNIQUE constraint
      (jdbc/execute! datasource ["CREATE TABLE unique_test (id INTEGER PRIMARY KEY, email TEXT UNIQUE)"])

      ;; 2. Insert the first valid row
      (jdbc/execute! datasource ["INSERT INTO unique_test (email) VALUES ('test@example.com')"])

      ;; 3. Attempt to insert a duplicate and catch the exception
      (let [captured-ex (try
                          (jdbc/execute! datasource ["INSERT INTO unique_test (email) VALUES ('test@example.com')"])
                          nil
                          (catch SQLException e
                            e))]

        (is (some? captured-ex)
            "Must throw a SQLException on constraint violation")

        ;; 4. The crucial assertions!
        (is (= ops/err-constraint-unique (ops/err-code captured-ex))
            "Must extract the extended 2067 code, successfully bypassing the driver's masking")

        (is (= ops/err-constraint (ops/primary-err-code captured-ex))
            "Must correctly strip the extended bits to return the primary 19 code")))))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; immediate transaction tests

;; Shared-cache memory DB so two separate JDBC connections can see the same DB.
;; busy_timeout kept short so contention tests fail quickly.
(def ^:private test-db-url
  "jdbc:sqlite:file:ops_test?mode=memory&cache=shared&busy_timeout=50")

(defn- setup-db!
  [ds]
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS tx_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL)"])
  (jdbc/execute! ds ["DELETE FROM tx_test"]))

(defn- fetch-val
  [ds id]
  (some-> (jdbc/execute-one! ds ["SELECT val FROM tx_test WHERE id = ?" id])
          :tx_test/val))

(defn- row-count
  [ds]
  (:n (jdbc/execute-one! ds ["SELECT COUNT(*) AS n FROM tx_test"])))

(defn- helper-insert!
  [tx id val]
  (sql/insert! tx :tx_test {:id id :val val}))

(defn- with-shared-test-db
  [f]
  ;; Anchor connection keeps the shared in-memory DB alive for the test body.
  (with-open [_anchor (jdbc/get-connection test-db-url)]
    (let [ds (jdbc/get-datasource test-db-url)]
      (setup-db! ds)
      (f ds))))

(deftest immediate-transaction-commit-test
  (with-shared-test-db
    (fn [ds]
      (testing "Commits data successfully"
        (ops/with-immediate-transaction [tx ds]
          (sql/insert! tx :tx_test {:id 1 :val "commit-test"}))

        (is (= "commit-test" (fetch-val ds 1)))
        (is (= 1 (row-count ds)))))))

(deftest immediate-transaction-helper-functions-test
  (with-shared-test-db
    (fn [ds]
      (testing "The same tx object can be passed to helper functions that write"
        (ops/with-immediate-transaction [tx ds]
          (helper-insert! tx 10 "a")
          (helper-insert! tx 11 "b"))

        (is (= "a" (fetch-val ds 10)))
        (is (= "b" (fetch-val ds 11)))
        (is (= 2 (row-count ds)))))))

(deftest immediate-transaction-rollback-on-clj-exception-test
  (with-shared-test-db
    (fn [ds]
      (testing "Rolls back all writes when body throws a Clojure exception"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"Boom"
             (ops/with-immediate-transaction [tx ds]
               (sql/insert! tx :tx_test {:id 2 :val "rollback-test"})
               (throw (ex-info "Boom" {})))))

        (is (nil? (fetch-val ds 2)))
        (is (= 0 (row-count ds)))))))

(deftest immediate-transaction-rollback-on-sql-exception-test
  (with-shared-test-db
    (fn [ds]
      (testing "Rolls back earlier writes when a later SQL statement fails"
        (is (thrown? SQLException
                     (ops/with-immediate-transaction [tx ds]
                       (sql/insert! tx :tx_test {:id 20 :val "first"})
                       ;; duplicate PK -> real JDBC/SQLite failure
                       (sql/insert! tx :tx_test {:id 20 :val "duplicate"}))))

        (is (nil? (fetch-val ds 20)))
        (is (= 0 (row-count ds)))))))

(deftest immediate-transaction-nesting-guard-test
  (with-shared-test-db
    (fn [ds]
      (testing "Cannot start BEGIN IMMEDIATE inside an existing JDBC transaction"
        (is (thrown-with-msg?
             Exception
             #"already in a transaction"
             (jdbc/with-transaction [outer-tx ds]
               (ops/with-immediate-transaction [inner-tx outer-tx]
                 (jdbc/execute! inner-tx ["SELECT 1"])))))))))

(deftest immediate-transaction-raw-connection-test
  (with-shared-test-db
    (fn [ds]
      (testing "Works when given a raw java.sql.Connection"
        (with-open [^Connection conn (jdbc/get-connection ds)]
          (ops/with-immediate-transaction [tx conn]
            (sql/insert! tx :tx_test {:id 30 :val "conn-test"})))

        (is (= "conn-test" (fetch-val ds 30)))
        (is (= 1 (row-count ds)))))))

(deftest immediate-transaction-wrapped-connectable-test
  (with-shared-test-db
    (fn [ds]
      (testing "Works with wrapped connectable maps"
        (ops/with-immediate-transaction [tx {:connectable ds :opts {}}]
          (sql/insert! tx :tx_test {:id 40 :val "wrapped-map"}))

        (is (= "wrapped-map" (fetch-val ds 40)))
        (is (= 1 (row-count ds)))))))

(deftest immediate-transaction-concurrency-begin-lock-test
  (with-shared-test-db
    (fn [ds]
      (testing "BEGIN IMMEDIATE acquires the write lock before any writes occur"
        (with-open [conn1 (jdbc/get-connection ds)
                    conn2 (jdbc/get-connection ds)]
          (let [tx1-started (promise)
                tx1-finish  (promise)
                f1          (future
                              (ops/with-immediate-transaction [_tx1 conn1]
                                ;; No writes here on purpose.
                                ;; The point is to prove BEGIN IMMEDIATE itself takes the lock.
                                (deliver tx1-started true)
                                @tx1-finish))]

            @tx1-started

            (let [captured-ex (try
                                (ops/with-immediate-transaction [tx2 conn2]
                                  (jdbc/execute! tx2 ["SELECT 1"]))
                                nil
                                (catch SQLException e
                                  e))]
              (is (some? captured-ex)
                  "Second writer must fail while first IMMEDIATE tx is open")
              (is (#{ops/err-busy ops/err-locked}
                   (ops/primary-err-code captured-ex))
                  "Failure must be SQLITE_BUSY or SQLITE_LOCKED")
              (is (re-find #"SQLITE_(BUSY|LOCKED)" (.getMessage ^SQLException captured-ex))))

            (deliver tx1-finish true)
            @f1))))))

(deftest immediate-transaction-concurrency-recovery-test
  (with-shared-test-db
    (fn [ds]
      (testing "A connection remains usable after contention once the first tx finishes"
        (with-open [conn1 (jdbc/get-connection ds)
                    conn2 (jdbc/get-connection ds)]
          (let [tx1-started (promise)
                tx1-finish  (promise)
                f1          (future
                              (ops/with-immediate-transaction [_tx1 conn1]
                                (deliver tx1-started true)
                                @tx1-finish))]

            @tx1-started

            ;; First attempt should fail due to the lock.
            (is (thrown? SQLException
                         (ops/with-immediate-transaction [tx2 conn2]
                           (sql/insert! tx2 :tx_test {:id 50 :val "should-fail"}))))

            ;; Let tx1 complete, then prove conn2 / ds can be used normally again.
            (deliver tx1-finish true)
            @f1

            (ops/with-immediate-transaction [tx2 conn2]
              (sql/insert! tx2 :tx_test {:id 51 :val "after-recovery"}))

            (is (nil? (fetch-val ds 50)))
            (is (= "after-recovery" (fetch-val ds 51)))
            (is (= 1 (row-count ds)))))))))

(deftest immediate-transaction-usable-after-rollback-test
  (with-shared-test-db
    (fn [ds]
      (testing "Datasource remains usable after a rolled-back transaction"
        (is (thrown? SQLException
                     (ops/with-immediate-transaction [tx ds]
                       (sql/insert! tx :tx_test {:id 60 :val "first"})
                       (sql/insert! tx :tx_test {:id 60 :val "duplicate"}))))

        ;; A fresh transaction should still work after the failed one.
        (ops/with-immediate-transaction [tx ds]
          (sql/insert! tx :tx_test {:id 61 :val "healthy"}))

        (is (nil? (fetch-val ds 60)))
        (is (= "healthy" (fetch-val ds 61)))
        (is (= 1 (row-count ds)))))))

(deftest closed-system-test
  (testing "Calling write-fn and write-async-fn after close throws descriptive exception"
    (let [sys (core/make-in-memory-datasource "closed-system-test")]
      ((:close-fn sys))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"SQLite batch writer is closed"
           ((:write-fn sys) ["INSERT INTO foo VALUES (1)"])))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"SQLite batch writer is closed"
           ((:write-async-fn sys) ["INSERT INTO foo VALUES (1)"]))))))

(deftest async-error-fn-test
  (ts/with-quiet-logging
    (testing "Async write failures invoke optional async-error-fn callback"
      (let [error-called (promise)
            db-opts      {:async-error-fn (fn [e ctx]
                                            (deliver error-called {:exception e :context ctx}))}]
        (with-test-db [{:keys [datasource write-async-fn]} "async-err-test" db-opts]
          (jdbc/execute! datasource ["CREATE TABLE pk_test (id INTEGER PRIMARY KEY)"])
          (jdbc/execute! datasource ["INSERT INTO pk_test VALUES (1)"])
          
          ;; Duplicate PK -> triggers constraint violation asynchronously
          (write-async-fn ["INSERT INTO pk_test VALUES (1)"])
          
          (let [res (deref error-called 1000 :timeout)]
            (is (not= :timeout res) "The error callback should have been called within 1 second")
            (is (instance? Exception (:exception res)))
            (is (= ["INSERT INTO pk_test VALUES (1)"] (:sql-vec (:context res))))))))))
