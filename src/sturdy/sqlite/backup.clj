(ns sturdy.sqlite.backup
  (:require
   [clojure.string :as string]
   [babashka.fs :as fs]
   [next.jdbc :as jdbc]
   [taoensso.telemere :as t])
  (:import
   (java.time LocalDateTime)
   (java.time.format DateTimeFormatter)
   (com.zaxxer.hikari HikariDataSource)))

(set! *warn-on-reflection* true)

(defn backup-db!
  "Updates SQLite query planner statistics, creates a point-in-time snapshot of the DB,
   and prunes old backups.

   Opts:
     :keep-days (default: 30) - How many days of backups to retain."
  [^String db-name ^HikariDataSource datasource backup-dir & [{:keys [keep-days] :or {keep-days 30}}]]

  (let [formatter    (DateTimeFormatter/ofPattern "yyyy-MM-dd_HH-mm-ss")
        timestamp    (.format (LocalDateTime/now) formatter)
        file-prefix  (str db-name "-")
        backup-file  (str (fs/path backup-dir (format "%s%s.db" file-prefix timestamp)))

        retention-ms (* keep-days 24 60 60 1000)
        threshold-ms (- (System/currentTimeMillis) retention-ms)]

    ;; 1. Optimize
    (t/log! {:level :info :id ::optimize-db
             :msg "Running SQLite PRAGMA optimize"})
    (jdbc/execute! datasource ["PRAGMA optimize;"])

    ;; 2. Backup
    (fs/create-dirs backup-dir)
    (t/log! {:level :info :id ::backup-db
             :msg "Starting database backup"
             :data {:destination backup-file}})

    ;; VACUUM INTO safely streams a read-snapshot without blocking writers.
    ;; Using parameterized query avoids syntax errors if paths contain quotes.
    (jdbc/execute! datasource ["VACUUM INTO ?" backup-file])

    (t/log! {:level :info :id ::backup-db-success
             :msg "Database backup completed successfully"})

    ;; 3. Prune Old Backups
    (doseq [file (fs/list-dir backup-dir)]
      (let [filename (str (fs/file-name file))]
        (when (and (fs/regular-file? file)
                   (string/starts-with? filename file-prefix)
                   (< (fs/file-time->millis (fs/last-modified-time file)) threshold-ms))

          (t/log! {:level :info :id ::prune-backup
                   :msg (format "Deleting backup older than %d days" keep-days)
                   :data {:file (str file)}})
          (fs/delete-if-exists file))))

    backup-file))
