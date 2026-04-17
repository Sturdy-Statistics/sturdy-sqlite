(ns sturdy.sqlite.immediate-proof-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as sql]
   [sturdy.sqlite.ops :as ops])
  (:import
   (java.io File)
   (java.sql SQLException)))

(set! *warn-on-reflection* true)

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Helpers

(def ^:private busy-timeout-ms 100)
(def ^:private future-timeout-ms 2000)

(defn- temp-db-url
  []
  (let [^File f (File/createTempFile "sqlite-immediate-proof-" ".db")]
    (.deleteOnExit f)
    (str "jdbc:sqlite:" (.getAbsolutePath f)
         "?busy_timeout=" busy-timeout-ms)))

(defn- setup-db!
  [ds]
  ;; Use classic rollback-journal locking semantics for these tests.
  ;; This makes the read->write upgrade behavior easier to reason about.
  (jdbc/execute! ds ["PRAGMA journal_mode=DELETE"])
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS tx_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL)"])
  (jdbc/execute! ds ["DELETE FROM tx_test"])
  (jdbc/execute! ds ["INSERT INTO tx_test (id, val) VALUES (1, 'seed')"]))

(defn- with-file-test-db
  [f]
  (let [url (temp-db-url)
        ds  (jdbc/get-datasource url)]
    (setup-db! ds)
    (f ds)))

(defn- fetch-val
  [ds id]
  (some-> (jdbc/execute-one! ds ["SELECT val FROM tx_test WHERE id = ?" id])
          :tx_test/val))

(defn- await-future
  [f label]
  (let [res (deref f future-timeout-ms ::timeout)]
    (is (not= ::timeout res) label)
    res))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Proof tests

(deftest deferred-read-then-write-can-fail-on-upgrade-test
  (with-file-test-db
    (fn [ds]
      (testing "jdbc/with-transaction can fail mid-transaction when upgrading from read to write"
        (with-open [conn1 (jdbc/get-connection ds)
                    conn2 (jdbc/get-connection ds)]

          (let [tx1-has-read     (promise)
                tx2-has-written  (promise)
                release-tx2      (promise)

                ;; tx1: starts a normal deferred JDBC transaction, performs a read,
                ;; then later attempts to write.
                f1 (future
                     (try
                       (jdbc/with-transaction [tx1 conn1]
                         ;; Start deferred tx and take a read snapshot/lock.
                         (jdbc/execute-one! tx1 ["SELECT val FROM tx_test WHERE id = 1"])
                         (deliver tx1-has-read true)

                         ;; Wait until tx2 has already become the writer.
                         @tx2-has-written

                         ;; This write may fail because SQLite must now upgrade
                         ;; tx1 from read -> write.
                         (sql/insert! tx1 :tx_test {:id 2 :val "from-deferred"})
                         :unexpected-success)
                       (catch SQLException e
                         e)))

                ;; tx2: waits for tx1's read, then grabs the write slot first
                ;; via BEGIN IMMEDIATE and holds it briefly.
                f2 (future
                     @tx1-has-read
                     (ops/with-immediate-transaction [tx2 conn2]
                       (sql/insert! tx2 :tx_test {:id 3 :val "from-writer"})
                       (deliver tx2-has-written true)
                       @release-tx2))]

            (let [res1 (await-future f1 "deferred transaction should complete with an error, not hang")]
              (is (instance? SQLException res1)
                  "Deferred transaction should fail on read->write upgrade")
              (is (#{ops/err-busy ops/err-locked}
                   (ops/primary-err-code res1))
                  "Failure should be SQLITE_BUSY or SQLITE_LOCKED"))

            (deliver release-tx2 true)
            (await-future f2 "writer transaction should finish after release")

            (is (nil? (fetch-val ds 2))
                "Deferred transaction's write must not commit")
            (is (= "from-writer" (fetch-val ds 3))
                "Writer transaction should commit successfully")))))))

(deftest immediate-acquires-write-slot-up-front-test
  (with-file-test-db
    (fn [ds]
      (testing "with-immediate-transaction acquires the write slot before the first write"
        (with-open [conn1 (jdbc/get-connection ds)
                    conn2 (jdbc/get-connection ds)]

          (let [tx1-started  (promise)
                release-tx1  (promise)

                ;; tx1: begins IMMEDIATE but does not write yet.
                f1 (future
                     (ops/with-immediate-transaction [tx1 conn1]
                       ;; No write here on purpose. We want to prove that BEGIN IMMEDIATE
                       ;; itself reserves the write slot.
                       (jdbc/execute-one! tx1 ["SELECT val FROM tx_test WHERE id = 1"])
                       (deliver tx1-started true)
                       @release-tx1))

                ;; tx2: after tx1 has begun IMMEDIATE, a normal deferred transaction
                ;; tries to write and should lose.
                f2 (future
                     @tx1-started
                     (try
                       (jdbc/with-transaction [tx2 conn2]
                         (sql/insert! tx2 :tx_test {:id 2 :val "should-fail"})
                         :unexpected-success)
                       (catch SQLException e
                         e)))]

            (let [res2 (await-future f2 "competing writer should fail promptly, not hang")]
              (is (instance? SQLException res2)
                  "Competing writer should fail while IMMEDIATE tx holds the write slot")
              (is (#{ops/err-busy ops/err-locked}
                   (ops/primary-err-code res2))
                  "Failure should be SQLITE_BUSY or SQLITE_LOCKED"))

            (deliver release-tx1 true)
            (await-future f1 "immediate transaction should finish after release")

            (is (nil? (fetch-val ds 2))
                "Competing writer must not commit")))))))

(deftest immediate-allows-the-holder-to-complete-its-own-write-test
  (with-file-test-db
    (fn [ds]
      (testing "The transaction that acquired BEGIN IMMEDIATE can later write successfully"
        (with-open [conn1 (jdbc/get-connection ds)
                    conn2 (jdbc/get-connection ds)]

          (let [tx1-started    (promise)
                tx2-attempted  (promise)

                f1 (future
                     (ops/with-immediate-transaction [tx1 conn1]
                       ;; Acquire the write slot first.
                       (jdbc/execute-one! tx1 ["SELECT val FROM tx_test WHERE id = 1"])
                       (deliver tx1-started true)

                       ;; Wait for the competing writer to try and fail.
                       @tx2-attempted

                       ;; Now perform our own write successfully.
                       (sql/insert! tx1 :tx_test {:id 4 :val "from-immediate-holder"})
                       :ok))

                f2 (future
                     @tx1-started
                     (let [res
                           (try
                             (jdbc/with-transaction [tx2 conn2]
                               (sql/insert! tx2 :tx_test {:id 5 :val "should-not-win"})
                               :unexpected-success)
                             (catch SQLException e
                               e))]
                       (deliver tx2-attempted true)
                       res))]

            (let [res2 (await-future f2 "competing writer attempt should finish promptly")]
              (is (instance? SQLException res2)
                  "Competing writer should fail")
              (is (#{ops/err-busy ops/err-locked}
                   (ops/primary-err-code res2))))

            (is (= :ok (await-future f1 "immediate holder should finish successfully")))

            (is (= "from-immediate-holder" (fetch-val ds 4))
                "The IMMEDIATE transaction holder should be able to complete its own write")
            (is (nil? (fetch-val ds 5))
                "The competing writer must not commit")))))))
