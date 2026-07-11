(ns sturdy.sqlite.batch-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.core.async :as a]
   [next.jdbc :as jdbc]
   [sturdy.sqlite.ops :as ops]
   [sturdy.sqlite.test :refer [with-test-db]]
   [sturdy.sqlite.test-support :as ts]
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

        (is (ts/eventually
             #(let [rows (jdbc/execute! datasource ["SELECT * FROM logs"] b-opts)]
                (and (= 1 (count rows))
                     (= "hello background worker" (-> rows first :msg))))
             1000))))))

(deftest batch-partial-failure-test
  (ts/with-quiet-logging
    (testing "A constraint violation on one write does not prevent other writes in the same batch from committing"
      (with-test-db [{:keys [datasource]} "batch-partial-test"]
        (jdbc/execute! datasource ["CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT UNIQUE)"])
        (jdbc/execute! datasource ["INSERT INTO items (id, name) VALUES (1, 'taken')"])

        ;; Start a dedicated batch writer with a long wait so all 4 items
        ;; are guaranteed to be pulled into a single batch.
        (let [batch-sys (ops/start-batch-writer! datasource 100 500 {} nil)
              ch-ok-1   (a/promise-chan)
              ch-ok-2   (a/promise-chan)
              ch-fail   (a/promise-chan)
              ch-ok-3   (a/promise-chan)]
          (try
            ;; Push all items directly onto the channel before the batch fires.
            (a/>!! (:req-ch batch-sys)
                   {:sql-vec ["INSERT INTO items (id, name) VALUES (2, 'alpha')"]
                    :opts {} :resp-ch ch-ok-1})
            (a/>!! (:req-ch batch-sys)
                   {:sql-vec ["INSERT INTO items (id, name) VALUES (3, 'beta')"]
                    :opts {} :resp-ch ch-ok-2})
            (a/>!! (:req-ch batch-sys)
                   {:sql-vec ["INSERT INTO items (id, name) VALUES (4, 'taken')"]  ;; unique conflict!
                    :opts {} :resp-ch ch-fail})
            (a/>!! (:req-ch batch-sys)
                   {:sql-vec ["INSERT INTO items (id, name) VALUES (5, 'gamma')"]
                    :opts {} :resp-ch ch-ok-3})

            (let [r1 (a/<!! ch-ok-1)
                  r2 (a/<!! ch-ok-2)
                  r3 (a/<!! ch-fail)
                  r4 (a/<!! ch-ok-3)]

              ;; The conflicting write should have received an exception
              (is (instance? Exception r3)
                  "The conflicting write should return an exception")

              ;; Writes before and after the failure should succeed
              (is (not (instance? Throwable r1))
                  "Write before the failure should succeed")
              (is (not (instance? Throwable r2))
                  "Write before the failure should succeed")
              (is (not (instance? Throwable r4))
                  "Write AFTER the failure should also succeed")

              ;; Verify the committed database state
              (let [names (->> (jdbc/execute! datasource
                                              ["SELECT name FROM items ORDER BY id"])
                               (mapv :items/name))]
                (is (= ["taken" "alpha" "beta" "gamma"] names)
                    "Seed row + 3 successful inserts; the conflicting row is excluded")))

            (finally
              (ops/close-batch-writer! batch-sys))))))))

(deftest synchronous-results-wait-for-commit-test
  (let [transaction-body-finished (promise)
        allow-commit              (promise)]
    (with-redefs [ops/-transact-immediate
                  (fn [_ds _opts f]
                    (let [result (f ::tx)]
                      (deliver transaction-body-finished true)
                      @allow-commit
                      result))
                  jdbc/execute! (fn [_tx _sql-vec _opts] ::write-result)]
      (let [batch-sys (ops/start-batch-writer! ::ds 1 10 {})
            resp-ch   (a/promise-chan)]
        (try
          (a/>!! (:req-ch batch-sys)
                 {:sql-vec ["INSERT INTO example DEFAULT VALUES"]
                  :opts {} :resp-ch resp-ch})

          @transaction-body-finished
          (is (nil? (a/poll! resp-ch))
              "A synchronous result must not be visible before COMMIT finishes")

          (deliver allow-commit true)
          (is (= ::write-result (a/<!! resp-ch)))
          (finally
            (deliver allow-commit true)
            (ops/close-batch-writer! batch-sys)))))))

(deftest commit-failure-reaches-all-synchronous-callers-test
  (let [commit-error (ex-info "COMMIT failed" {})]
    (with-redefs [ops/-transact-immediate
                  (fn [_ds _opts f]
                    (f ::tx)
                    (throw commit-error))
                  jdbc/execute! (fn [_tx sql-vec _opts] [(second sql-vec)])]
      (let [batch-sys (ops/start-batch-writer! ::ds 2 100 {})
            resp-1    (a/promise-chan)
            resp-2    (a/promise-chan)]
        (try
          (a/>!! (:req-ch batch-sys)
                 {:sql-vec ["INSERT" 1] :opts {} :resp-ch resp-1})
          (a/>!! (:req-ch batch-sys)
                 {:sql-vec ["INSERT" 2] :opts {} :resp-ch resp-2})

          (is (identical? commit-error (a/<!! resp-1)))
          (is (identical? commit-error (a/<!! resp-2)))
          (finally
            (ops/close-batch-writer! batch-sys)))))))
