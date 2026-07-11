(ns sturdy.sqlite.backup-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [sturdy.sqlite.backup :as backup]
   [babashka.fs :as fs]
   [next.jdbc :as jdbc]
   [sturdy.sqlite.test-support :as ts])
  (:import
   (java.nio.file.attribute FileTime)))

(set! *warn-on-reflection* true)

(deftest backup-retention-option-validation-test
  (testing "Invalid keep-days values fail before database or filesystem side effects"
    (doseq [keep-days [0 -1 1.5 nil]]
      (let [side-effects (atom [])]
        (with-redefs [jdbc/execute!  (fn [& _] (swap! side-effects conj :execute))
                      fs/create-dirs (fn [& _] (swap! side-effects conj :create-dirs))]
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo #"keep-days"
               (backup/backup-db! "testdb" {} "/mock/backups"
                                  {:keep-days keep-days}))
              (str "Invalid keep-days: " (pr-str keep-days)))
          (is (empty? @side-effects)))))))

(deftest backup-db-retention-test
  (ts/with-quiet-logging
   (testing "Successfully creates a backup and deletes ONLY backups older than keep-days"
     (let [mock-backup-dir "/mock/backups"
           db-name         "testdb"
           now-ms          (System/currentTimeMillis)
           day-ms          (* 24 60 60 1000)

           ;; The prefix must match the db-name to get picked up by the new prefix filter!
           old-file        (fs/file "/mock/backups/testdb-old.db")
           new-file        (fs/file "/mock/backups/testdb-new.db")

           deleted-files   (atom [])]

       (with-redefs [ ;; Mock the database execution so we don't need a real DB
                     jdbc/execute!         (constantly nil)

                     ;; Mock the directory listing
                     fs/create-dirs        (constantly nil)
                     fs/list-dir           (constantly [old-file new-file])
                     fs/regular-file?      (constantly true)

                     ;; Mock the file timestamps: 31 days old vs 2 days old
                     fs/last-modified-time (fn [f]
                                             (if (= f old-file)
                                               (fs/millis->file-time (- now-ms (* 31 day-ms)))
                                               (fs/millis->file-time (- now-ms (* 2 day-ms)))))
                     fs/file-time->millis  #(-> ^FileTime % .toMillis)

                     ;; Spy on the deletions
                     fs/delete-if-exists   (fn [f] (swap! deleted-files conj f))]

         ;; Call the library function directly
         (backup/backup-db! db-name {} mock-backup-dir {:keep-days 30})

         (is (= 1 (count @deleted-files)) "Should only delete exactly one file")
         (is (= old-file (first @deleted-files)) "MUST delete the 31-day-old file, and spare the 2-day-old file"))))))
