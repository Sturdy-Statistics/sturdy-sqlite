(ns sturdy.sqlite.core-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [sturdy.sqlite.core :as core]
   [sturdy.sqlite.test-support :as ts]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [babashka.fs :as fs])
  (:import
   (com.zaxxer.hikari HikariDataSource)
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

(deftest in-memory-db-name-normalization-test
  (testing "Path components and extensions are removed from the in-memory database name"
    (let [sys (core/make-in-memory-datasource "nested/example.db")]
      (try
        (let [^HikariDataSource ds (:datasource sys)]
          (is (= "SQLite-example-Pool" (.getPoolName ds)))
          (is (re-find #"/example\?mode=memory" (.getJdbcUrl ds))))
        (finally
          ((:close-fn sys)))))))

(defn- pragma [connection setting]
  (-> (jdbc/execute-one! connection [(str "PRAGMA " setting)] {:builder-fn rs/as-unqualified-lower-maps})
      vals first))

(deftest every-physical-connection-receives-the-profile
  (doseq [[profile size cache mmap temp] [[:analytics 10 -64000 268435456 2]
                                           [:auth 5 -16000 134217728 2]
                                           [:write-intensive 2 -4000 0 1]
                                           [:general-purpose 5 -32000 268435456 2]
                                           [:low-resource 2 -4000 0 1]]]
    (testing (name profile)
      (let [dir (fs/create-temp-dir {:prefix "sqlite-profile-"})
            db (core/make-datasource "settings" (str dir) profile)
            connections (atom [])]
        (try
          ;; Hold all checkouts simultaneously to require distinct physical connections.
          (dotimes [_ size] (swap! connections conj (.getConnection ^HikariDataSource (:datasource db))))
          (doseq [connection @connections]
            (is (= "wal" (pragma connection "journal_mode")))
            (is (= 1 (pragma connection "foreign_keys")))
            (is (= 1 (pragma connection "synchronous")))
            (is (= 5000 (pragma connection "busy_timeout")))
            (is (= temp (pragma connection "temp_store")))
            (is (= mmap (pragma connection "mmap_size")))
            (is (= cache (pragma connection "cache_size"))))
          (finally
            (doseq [^java.sql.Connection connection @connections] (.close connection))
            ((:close-fn db))
            (fs/delete-tree dir)))))))

(deftest wal-allows-a-writer-during-a-reader-snapshot
  (let [dir (fs/create-temp-dir {:prefix "sqlite-wal-"})
        db (core/make-datasource "concurrency" (str dir) :general-purpose)]
    (try
      (jdbc/execute! (:datasource db) ["CREATE TABLE facts (value INTEGER)"])
      (jdbc/execute! (:datasource db) ["INSERT INTO facts VALUES (1)"])
      (with-open [^java.sql.Connection reader (.getConnection ^HikariDataSource (:datasource db))
                  ^java.sql.Connection writer (.getConnection ^HikariDataSource (:datasource db))]
        (.setAutoCommit reader false)
        (is (= 1 (-> (jdbc/execute-one! reader ["SELECT COUNT(*) AS n FROM facts"]) vals first)))
        (jdbc/execute! writer ["INSERT INTO facts VALUES (2)"])
        (is (= 1 (-> (jdbc/execute-one! reader ["SELECT COUNT(*) AS n FROM facts"]) vals first)))
        (.rollback reader)
        (is (= 2 (-> (jdbc/execute-one! writer ["SELECT COUNT(*) AS n FROM facts"]) vals first))))
      (finally ((:close-fn db)) (fs/delete-tree dir)))))

(deftest in-memory-profile-retains-shared-data-and-configures-both-connections
  (let [db (core/make-in-memory-datasource (str (random-uuid)))
        ^HikariDataSource ds (:datasource db)]
    (try
      ;; The public pool has two connections, one held internally as an anchor.
      (with-open [^java.sql.Connection c (.getConnection ds)]
        (is (= "memory" (pragma c "journal_mode")))
        (is (= 1 (pragma c "foreign_keys")))
        (is (= 0 (pragma c "synchronous")))
        (is (= 5000 (pragma c "busy_timeout")))
        (is (= 2 (pragma c "temp_store")))
        (jdbc/execute! c ["CREATE TABLE retained (value INTEGER)"])
        (jdbc/execute! c ["INSERT INTO retained VALUES (42)"]))
      (.softEvictConnections (.getHikariPoolMXBean ds))
      (with-open [^java.sql.Connection c (.getConnection ds)]
        (is (= 42 (-> (jdbc/execute-one! c ["SELECT value FROM retained"]) vals first)))
        (is (= 1 (pragma c "foreign_keys")))
        (is (= 0 (pragma c "synchronous")))
        (is (= 5000 (pragma c "busy_timeout"))))
      (finally ((:close-fn db))))))
