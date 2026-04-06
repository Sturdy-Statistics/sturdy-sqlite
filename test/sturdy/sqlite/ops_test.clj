(ns sturdy.sqlite.ops-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [next.jdbc :as jdbc]
   [sturdy.sqlite.ops :as ops]
   [sturdy.sqlite.test :refer [with-test-db]]
   [sturdy.sqlite.test-support :as ts])
  (:import
   (java.sql SQLException)))

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
       (is (= 4 @attempts) "Initial try + 3 retries = 4 total attempts")))

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

(deftest extended-error-codes-test
  (testing "Extracts extended error codes natively from the JDBC driver"
    (with-test-db [ds "error-codes-db"]
      ;; 1. Setup a table with a UNIQUE constraint
      (jdbc/execute! ds ["CREATE TABLE unique_test (id INTEGER PRIMARY KEY, email TEXT UNIQUE)"])

      ;; 2. Insert the first valid row
      (jdbc/execute! ds ["INSERT INTO unique_test (email) VALUES ('test@example.com')"])

      ;; 3. Attempt to insert a duplicate and catch the exception
      (let [captured-ex (try
                          (jdbc/execute! ds ["INSERT INTO unique_test (email) VALUES ('test@example.com')"])
                          nil
                          (catch SQLException e
                            e))]

        (is (some? captured-ex) "Must throw a SQLException on constraint violation")

        ;; 4. The crucial assertions!
        (is (= ops/err-constraint-unique (ops/err-code captured-ex))
            "Must extract the extended 2067 code, successfully bypassing the driver's masking")

        (is (= ops/err-constraint (ops/primary-err-code captured-ex))
            "Must correctly strip the extended bits to return the primary 19 code")))))
