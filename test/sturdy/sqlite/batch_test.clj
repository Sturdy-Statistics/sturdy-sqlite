(ns sturdy.sqlite.batch-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [next.jdbc :as jdbc]
   [sturdy.sqlite.test :refer [with-test-db]]
   [sturdy.sqlite.types :as types]))

(set! *warn-on-reflection* true)

(deftest batch-writer-test
  (let [b-opts  (types/make-builder-opts {})
        db-opts {:builder-opts b-opts}]

    (testing "Synchronous write-fn returns results and routes errors correctly"
      (with-test-db [{:keys [datasource write-fn]} "sync-batch-test" db-opts]

        (jdbc/execute! datasource ["CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT UNIQUE)"])

        (let [res (write-fn ["INSERT INTO users (username) VALUES (?)" "alice"]
                            {:return-keys true})]
          (is (= 1 (-> res first (get (keyword "last-insert-rowid()"))))
              "Should return metadata from the batched transaction"))

        (let [rows (jdbc/execute! datasource ["SELECT * FROM users"] b-opts)]
          (is (= 1 (count rows)))
          (is (= "alice" (-> rows first :username))))

        (is (thrown? Exception
                     (write-fn ["INSERT INTO users (username) VALUES (?)" "alice"]))
            "Should throw an exception back to the caller on constraint violation")))

    (testing "Asynchronous write-async-fn processes background queue"
      (with-test-db [{:keys [datasource write-async-fn]} "async-batch-test" db-opts]

        (jdbc/execute! datasource ["CREATE TABLE logs (msg TEXT)"])

        (write-async-fn ["INSERT INTO logs (msg) VALUES (?)" "hello background worker"])

        (Thread/sleep 50)

        (let [rows (jdbc/execute! datasource ["SELECT * FROM logs"] b-opts)]
          (is (= 1 (count rows)))
          (is (= "hello background worker" (-> rows first :msg))))))))
