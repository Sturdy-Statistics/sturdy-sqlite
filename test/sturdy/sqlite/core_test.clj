(ns sturdy.sqlite.core-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [sturdy.sqlite.core :as core]
   [sturdy.sqlite.test-support :as ts]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [babashka.fs :as fs])
  (:import
   (java.io Closeable)))

(set! *warn-on-reflection* true)

(deftest close-datasource-test
  (ts/with-quiet-logging
    (testing "Graceful handling of close-datasource! errors"
      (let [dummy-ds (reify Closeable
                       (close [_] (throw (ex-info "Simulated close error" {}))))]
        (is (nil? (core/close-datasource! "testdb" dummy-ds))
            "Should catch error and not throw"))))

  (ts/with-quiet-logging
    (testing "Graceful handling of close-in-memory-datasource! errors"
      (let [dummy-ds (reify Closeable
                       (close [_] (throw (ex-info "Simulated DS close error" {}))))
            dummy-anchor (reify Closeable
                           (close [_] (throw (ex-info "Simulated anchor close error" {}))))]
        (is (nil? (core/close-in-memory-datasource! "testdb" dummy-ds dummy-anchor))
            "Should catch both errors and not throw")))))

(deftest physical-profiles-test
  (ts/with-quiet-logging
    (let [db-dir "target/test-db"]
      (try
        (doseq [profile [:general-purpose :write-intensive :analytics :auth :low-resource]]
          (testing (str "Profile: " profile)
            (let [sys (core/make-datasource (str "test-" (name profile)) db-dir profile)]
              (is (some? (:datasource sys)))
              (is (fn? (:close-fn sys)))
              ((:close-fn sys)))))
        (finally
          (fs/delete-tree db-dir))))))

(deftest in-memory-rejection-test
  (testing "in-memory profile is rejected by make-datasource"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Use make-in-memory-datasource instead"
         (core/make-datasource "mem-test" "target/test-db" :in-memory)))))

(deftest datasource-batch-option-validation-test
  (testing "Invalid batching options fail before creating filesystem resources"
    (let [db-dir "target/invalid-batch-options"]
      (fs/delete-tree db-dir)
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"max-batch-size"
           (core/make-datasource "invalid" db-dir :general-purpose
                                 {:batch-size 0})))
      (is (not (fs/exists? db-dir))))

    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"max-wait-ms"
         (core/make-in-memory-datasource "invalid"
                                         {:batch-wait-ms -1})))))

(deftest unknown-profile-test
  (testing "Unknown profiles throw with available keys"
    (let [err (try
                (core/make-datasource "unknown-test" "target/test-db" :non-existent)
                nil
                (catch clojure.lang.ExceptionInfo e
                  (ex-data e)))]
      (is (some? err))
      (is (= :non-existent (:profile err)))
      (is (coll? (:available err)))
      (is (some #{:general-purpose} (:available err))))))

(deftest in-memory-retention-test
  (let [db-name "memory-retention-test"
        sys1 (core/make-in-memory-datasource db-name)]
    (try
      ;; Create table and insert a row
      (jdbc/execute! (:datasource sys1) ["CREATE TABLE foo (id INT)"])
      (jdbc/execute! (:datasource sys1) ["INSERT INTO foo VALUES (1)"])

      ;; Read back using the same datasource
      (is (= [{:id 1}] (jdbc/execute! (:datasource sys1) ["SELECT id FROM foo"] {:builder-fn rs/as-unqualified-maps})))

      ;; Close the first system (which closes the anchor and the pool)
      ((:close-fn sys1))

      ;; Start a new system with the same name, the table should NOT exist
      (let [sys2 (core/make-in-memory-datasource db-name)]
        (try
          (is (thrown? java.sql.SQLException
                       (jdbc/execute! (:datasource sys2) ["SELECT id FROM foo"])))
          (finally
            ((:close-fn sys2)))))
      (catch Throwable t
        ((:close-fn sys1))
        (throw t)))))
