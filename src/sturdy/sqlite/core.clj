(ns sturdy.sqlite.core
  (:require
   [clojure.string :as string]
   [babashka.fs :as fs]
   [sturdy.sqlite.migrate :as migrate]
   [sturdy.sqlite.backup :as backup]
   [taoensso.telemere :as t])
  (:import
   (com.zaxxer.hikari HikariConfig HikariDataSource)
   (java.sql Connection)
   (java.io Closeable)))

(set! *warn-on-reflection* true)

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration Profiles

(def ^:private profiles
  {:high-performance
   {:pool {:maximumPoolSize 10
           :minimumIdle 2}
    :pragmas ["PRAGMA foreign_keys=ON;"
              "PRAGMA journal_mode=WAL;"
              "PRAGMA synchronous=NORMAL;"
              "PRAGMA busy_timeout=5000;"
              "PRAGMA temp_store=MEMORY;"
              "PRAGMA mmap_size=268435456;" ;; 256 Mb
              "PRAGMA cache_size=-64000;"]} ;; 64 Mb

   :low-resource
   {:pool {:maximumPoolSize 2
           :minimumIdle 1}
    :pragmas ["PRAGMA foreign_keys=ON;"
              "PRAGMA journal_mode=WAL;"
              "PRAGMA synchronous=NORMAL;"
              "PRAGMA busy_timeout=5000;"
              "PRAGMA temp_store=FILE;"     ;; Spill to disk to save RAM
              "PRAGMA mmap_size=0;"         ;; Disable memory mapping
              "PRAGMA cache_size=-4000;"]}  ;; Tiny 4MB cache

   :in-memory
   {:jdbc-url "jdbc:sqlite:file:%s?mode=memory&cache=shared"
    :pool {:maximumPoolSize 5
           :minimumIdle 1}
    :pragmas  ["PRAGMA foreign_keys=ON;"
               "PRAGMA journal_mode=MEMORY;"
               "PRAGMA synchronous=OFF;"
               "PRAGMA busy_timeout=5000;"
               "PRAGMA temp_store=MEMORY;"]}})

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Factory

(defn- build-hk-cfg
  [db-name db-path profile-key]
  (let [profile  (get profiles profile-key)
        _        (when-not profile
                   (throw (ex-info "Unknown SQLite profile"
                                   {:profile profile-key
                                    :available (keys profiles)})))

        url-tmpl (get profile :jdbc-url "jdbc:sqlite:%s")
        jdbc-url (format url-tmpl (str (fs/absolutize db-path)))
        init-sql (string/join " " (:pragmas profile))
        pool     (:pool profile)

        hk-cfg   (HikariConfig.)]

    (.setJdbcUrl           hk-cfg jdbc-url)
    (.setMaximumPoolSize   hk-cfg (:maximumPoolSize pool))
    (.setMinimumIdle       hk-cfg (:minimumIdle pool))
    (.setConnectionInitSql hk-cfg init-sql)
    (.setPoolName          hk-cfg (str "SQLite-" db-name "-Pool"))

    hk-cfg))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; close functions

(defn close-datasource!
  "Helper to safely close a standard physical datasource."
  [^String db-name ^HikariDataSource ds]
  (when (instance? Closeable ds)
    (try
      (.close ^Closeable ds)
      (catch Exception e
        (t/log! {:level :error :id ::ds-close-error :error e
                 :data {:db-name db-name}})))))

(defn close-in-memory-datasource!
  [^String db-name ^HikariDataSource ds ^Connection anchor]

  (try
    (when (instance? Closeable anchor) (.close ^Closeable anchor))
    (catch Exception e
      (t/log! {:level :error :id ::anchor-close-error :error e
               :data {:db-name db-name}})))

  (try
    (when (instance? Closeable ds) (.close ^Closeable ds))
    (catch Exception e
      (t/log! {:level :error :id ::ds-close-error :error e
               :data {:db-name db-name}}))))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

(defn make-datasource
  "Creates a HikariDataSource for the given physical database path.
   Ensures the parent directory exists before initialization.

   Available profiles: :high-performance, :low-resource"
  [db-name db-dir profile-key]

  (when (= :in-memory profile-key)
    (throw (ex-info "The DB will drop if idle. Use make-in-memory-datasource instead."
                    {})))

  (fs/create-dirs db-dir)

  (let [db-name' (-> db-name fs/file-name fs/strip-ext)
        db-path  (fs/path db-dir (str db-name' ".db"))
        cfg      (build-hk-cfg db-name' db-path profile-key)
        ds       (HikariDataSource. cfg)]
   {:datasource ds
    :migrate-fn (fn [classpath-prefix] (migrate/migrate! db-name' ds classpath-prefix))
    :backup-fn  (fn [backup-dir & [opts]]
                  (backup/backup-db! db-name' ds backup-dir opts))
    :close-fn   (fn [] (close-datasource! db-name' ds))}))

(defn make-in-memory-datasource
  "Creates an in-memory HikariDataSource. Automatically checks out an
   'anchor' connection to prevent SQLite from destroying the shared memory
   database when the pool is completely idle.

   `db-name` can be any string (e.g., a random UUID for test isolation).

   Returns a map:
   {:datasource (HikariDataSource)
    :anchor     (java.sql.Connection)
    :close-fn   (fn) - Safely closes the anchor, then the pool}"
  [db-name]
  (let [db-name' (-> db-name fs/file-name fs/strip-ext)
        ds       (HikariDataSource. (build-hk-cfg db-name db-name :in-memory))
        anchor   (.getConnection ds)]

    {:datasource ds
     :migrate-fn (fn [classpath-prefix] (migrate/migrate! db-name' ds classpath-prefix))
     :backup-fn  (fn [backup-dir & [opts]]
                   (backup/backup-db! db-name' ds backup-dir opts))
     :close-fn   (fn [] (close-in-memory-datasource! db-name ds anchor))}))
