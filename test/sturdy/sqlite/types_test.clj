(ns sturdy.sqlite.types-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [sturdy.sqlite.test :refer [with-test-db]]
   [sturdy.sqlite.types :as types]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as sql]
   [babashka.fs :as fs]
   [clojure.edn :as edn]
   [sturdy.sqlite.test-support :as ts]))

(set! *warn-on-reflection* true)

(deftest type-roundtrip-test
  (ts/with-quiet-logging
   (testing "Custom types are successfully written to and read from SQLite"
     (with-test-db [{:keys [datasource]} "type-test-db"]
       ;; 1. Set up a dummy schema
       (jdbc/execute! datasource ["
        CREATE TABLE test_types (
          id BLOB PRIMARY KEY,
          status TEXT,
          is_active INTEGER,
          metadata TEXT,
          file_path TEXT
        )"])

       ;; 2. Create our dynamic builder opts
       (let [base-dir (fs/create-temp-dir)
             opts     (types/make-builder-opts
                       {:uuid-col?           #{"id"}
                        :enum-col?           #{"status"}
                        :bool-col?           #{"is-active"}
                        :json-col?           #{"metadata"}
                        :json-parse-fn       edn/read-string ;; Dependency-free mock!
                        :path-col?           #{"file-path"}
                        :path-base-dir       base-dir
                        :auto-convert-paths? true})

             test-uuid (random-uuid)
             test-path (fs/path base-dir "uploads" "test.txt")]

         (try
           ;; 3. Insert Clojure native types
           (sql/insert! datasource :test_types
                        {:id        test-uuid
                         :status    :processing
                         :is_active true
                         :metadata  (pr-str {:retries 5})
                         :file_path test-path}
                        opts)

           ;; 4. Read them back and assert exact equality
           (let [row (first (sql/find-by-keys datasource :test_types {:id test-uuid} opts))]
             (is (= test-uuid (:id row)))
             (is (= :processing (:status row)) "Keyword survived")
             (is (= true (:is-active row)) "Boolean survived")
             (is (= {:retries 5} (:metadata row)) "Parsed data survived")
             (is (= test-path (:file-path row)) "Absolute path reconstructed"))

           (finally
             (fs/delete-tree base-dir)
             (reset! types/path-base-dir* nil))))))))
