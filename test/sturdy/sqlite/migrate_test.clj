(ns sturdy.sqlite.migrate-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [sturdy.sqlite.migrate :as migrate]
   [sturdy.sqlite.test :as db-test]
   [next.jdbc :as jdbc]
   [ragtime.repl :as rr]))

(set! *warn-on-reflection* true)

;; Need a fake test migration in a unique classpath prefix, but we don't have one readily available.
;; Or we can just mock ragtime-repl.
(deftest migrate-rollback-test
  (testing "migrate! and rollback! call ragtime with correct config"
    (let [ragtime-migrate-called (atom false)
          ragtime-rollback-called (atom false)]
      (with-redefs [rr/migrate  (fn [cfg]
                                  (reset! ragtime-migrate-called true)
                                  (is (= "testdb" (get-in cfg [:reporter-data :db-name] "testdb")))
                                  ;; test the reporter function by calling it
                                  (let [reporter (:reporter cfg)]
                                    (reporter nil :up "001-test")
                                    (reporter nil :down "001-test")))
                    rr/rollback (fn [_cfg]
                                  (reset! ragtime-rollback-called true))]
        (migrate/migrate! "testdb" {} "test-migrations")
        (is @ragtime-migrate-called)

        (migrate/rollback! "testdb" {} "test-migrations")
        (is @ragtime-rollback-called))))

  (testing "Real migration execution on test DB"
    (db-test/with-test-db [{:keys [datasource]} "test-migrations-db" {:classpath-prefix "test-migrations"}]
      ;; with-test-db automatically runs migrate! using the classpath-prefix, so the table should exist
      (let [res (jdbc/execute! datasource ["SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'"])]
        (is (= 1 (count res)) "test_table should have been created"))

      ;; Now roll it back
      (migrate/rollback! "test-migrations-db" datasource "test-migrations")
      (let [res (jdbc/execute! datasource ["SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'"])]
        (is (= 0 (count res)) "test_table should have been dropped")))))
