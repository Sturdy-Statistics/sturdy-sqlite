(ns sturdy.sqlite.migrate-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [sturdy.sqlite.migrate :as migrate]
   [sturdy.sqlite.test :as db-test]
   [sturdy.sqlite.types :as types]
   [next.jdbc :as jdbc]))

(set! *warn-on-reflection* true)

;; Need a fake test migration in a unique classpath prefix, but we don't have one readily available.
;; Or we can just mock ragtime-repl.
(deftest migrate-rollback-test
  (testing "migrate! and rollback! call ragtime with correct config"
    (let [ragtime-migrate-called (atom false)
          ragtime-rollback-called (atom false)]
      (with-redefs [ragtime.repl/migrate  (fn [cfg]
                                            (reset! ragtime-migrate-called true)
                                            (is (= "testdb" (get-in cfg [:reporter-data :db-name] "testdb")))
                                            ;; test the reporter function by calling it
                                            (let [reporter (:reporter cfg)]
                                              (reporter nil :up "001-test")
                                              (reporter nil :down "001-test")))
                    ragtime.repl/rollback (fn [cfg]
                                            (reset! ragtime-rollback-called true))]
        (migrate/migrate! "testdb" {} "test-migrations")
        (is @ragtime-migrate-called)

        (migrate/rollback! "testdb" {} "test-migrations")
        (is @ragtime-rollback-called)))))
