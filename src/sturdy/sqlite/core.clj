(ns sturdy.sqlite.core
  (:require
   [clojure.string :as string]
   [babashka.fs :as fs]
   [sturdy.sqlite.migrate :as migrate]
   [sturdy.sqlite.backup :as backup]
   [sturdy.sqlite.ops :as ops]
   [taoensso.telemere :as t])
  (:import
   (com.zaxxer.hikari HikariConfig HikariDataSource)
   (java.sql Connection)
   (java.io Closeable)))

(set! *warn-on-reflection* true)

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration Profiles

(def ^:private profiles
  {:analytics
   ;; Workload: Long-running, complex SELECT queries (aggregations, joins).
   {:pool {:maximumPoolSize 10
           :minimumIdle     10}
    :pragmas ["PRAGMA foreign_keys=ON;"
              "PRAGMA journal_mode=WAL;"
              "PRAGMA synchronous=NORMAL;"
              "PRAGMA busy_timeout=5000;"
              "PRAGMA temp_store=MEMORY;"
              "PRAGMA mmap_size=268435456;" ;; 256 MB
              "PRAGMA cache_size=-64000;"]} ;;  64 MB cache

   :auth
   ;; Workload: fast, highly concurrent point-reads (SELECT * WHERE id = ?).
   {:pool {:maximumPoolSize 5
           :minimumIdle     5}
    :pragmas ["PRAGMA foreign_keys=ON;"
              "PRAGMA journal_mode=WAL;"
              "PRAGMA synchronous=NORMAL;"
              "PRAGMA busy_timeout=5000;"
              "PRAGMA temp_store=MEMORY;"
              "PRAGMA mmap_size=134217728;"  ;; 128 MB
              "PRAGMA cache_size=-16000;"]}  ;;  16 MB cache

   :write-intensive
   ;; Workload: Background queues, metrics ingestion, append-heavy tables.
   {:pool {:maximumPoolSize 2
           :minimumIdle     2}
    :pragmas ["PRAGMA foreign_keys=ON;"
              "PRAGMA journal_mode=WAL;"
              "PRAGMA synchronous=NORMAL;"
              "PRAGMA busy_timeout=5000;"
              "PRAGMA temp_store=FILE;"
              "PRAGMA mmap_size=0;"
              "PRAGMA cache_size=-4000;"]}  ;; 4 MB cache

   :general-purpose
   ;; Workload: Standard CRUD web apps, queue servers, and mixed read/write loads.
   {:pool {:maximumPoolSize 5
           :minimumIdle     5}
    :pragmas ["PRAGMA foreign_keys=ON;"
              "PRAGMA journal_mode=WAL;"
              "PRAGMA synchronous=NORMAL;"
              "PRAGMA busy_timeout=5000;"
              "PRAGMA temp_store=MEMORY;"
              "PRAGMA mmap_size=268435456;"  ;; 256 MB
              "PRAGMA cache_size=-32000;"]}  ;;  32 MB cache

   :low-resource
   {:pool {:maximumPoolSize 2
           :minimumIdle     2}
    :pragmas ["PRAGMA foreign_keys=ON;"
              "PRAGMA journal_mode=WAL;"
              "PRAGMA synchronous=NORMAL;"
              "PRAGMA busy_timeout=5000;"
              "PRAGMA temp_store=FILE;" ;; Spill to disk to save RAM
              "PRAGMA mmap_size=0;"     ;; Disable memory mapping
              "PRAGMA cache_size=-4000;"]} ;; 4MB cache

   :in-memory
   ;; Workload: Ephemeral test suites.
   {:jdbc-url "jdbc:sqlite:file:%s?mode=memory&cache=shared"
    :pool {:maximumPoolSize 2
           :minimumIdle     2}
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

   Available profiles: :general-purpose, :write-intensive, :analytics, :auth, :low-resource

   Opts:
     :batch-size    (default: 500)
     :batch-wait-ms (default: 10)
     :builder-opts  (default: {}) - Global next.jdbc opts

  Returns a map:
   {:datasource     (HikariDataSource)
    :write-fn       (fn [sql-vec & [opts]]): efficient writes. Blocking, waits for the result
    :write-async-fn fire-and-forget variant
    :migrate-fn     (fn [classpath-prefix]): perform ragtime migration.  call on setup
    :backup-fn      (fn [backup-dir]): backup DB
    :close-fn       (fn): - Closes the anchor, then the pool}"
  [db-name db-dir profile-key
   & [{:keys [batch-size batch-wait-ms builder-opts]
       :or {batch-size 500 batch-wait-ms 10 builder-opts {}}}]]

  (when (= :in-memory profile-key)
    (throw (ex-info "The DB will drop if idle. Use make-in-memory-datasource instead."
                    {})))

  (fs/create-dirs db-dir)

  (let [db-name'  (-> db-name fs/file-name fs/strip-ext)
        db-path   (fs/path db-dir (str db-name' ".db"))
        cfg       (build-hk-cfg db-name' db-path profile-key)
        ds        (HikariDataSource. cfg)
        batch-sys (ops/start-batch-writer! ds batch-size batch-wait-ms builder-opts)]

   {:datasource ds
    :write-fn       (fn [sql-vec & [opts]]
                      (ops/execute-batched! batch-sys sql-vec opts))
    :write-async-fn (fn [sql-vec & [opts]]
                      (ops/execute-batched-async! batch-sys sql-vec opts))
    :migrate-fn     (fn [classpath-prefix] (migrate/migrate! db-name' ds classpath-prefix))
    :backup-fn      (fn [backup-dir & [opts]]
                      (backup/backup-db! db-name' ds backup-dir opts))
    :close-fn       (fn []
                      (ops/close-batch-writer! batch-sys)
                      (close-datasource! db-name' ds))}))

(defn make-in-memory-datasource
  "Creates an in-memory HikariDataSource. Automatically checks out an
   'anchor' connection to prevent SQLite from destroying the shared memory
   database when the pool is completely idle.

   `db-name` can be any string (e.g., a random UUID for test isolation).

   Opts:
     :batch-size    (default: 500)
     :batch-wait-ms (default: 10)
     :builder-opts  (default: {}) - Global next.jdbc opts

   Returns a map:
   {:datasource     (HikariDataSource)
    :write-fn       (fn [sql-vec & [opts]]): efficient writes. Blocking, waits for the result
    :write-async-fn fire-and-forget variant
    :migrate-fn     (fn [classpath-prefix]): perform ragtime migration.  call on setup
    :backup-fn      (fn [backup-dir]): backup DB
    :close-fn       (fn): - Closes the anchor, then the pool}"
  [db-name
   & [{:keys [batch-size batch-wait-ms builder-opts]
       :or {batch-size 500 batch-wait-ms 10 builder-opts {}}}]]
  (let [db-name'  (-> db-name fs/file-name fs/strip-ext)
        cfg       (build-hk-cfg db-name db-name :in-memory)
        ds        (HikariDataSource. cfg)
        anchor    (.getConnection ds)
        batch-sys (ops/start-batch-writer! ds batch-size batch-wait-ms builder-opts)]

    {:datasource ds
     :write-fn       (fn [sql-vec & [opts]]
                       (ops/execute-batched! batch-sys sql-vec opts))
     :write-async-fn (fn [sql-vec & [opts]]
                       (ops/execute-batched-async! batch-sys sql-vec opts))
     :migrate-fn (fn [classpath-prefix] (migrate/migrate! db-name' ds classpath-prefix))
     :backup-fn  (fn [backup-dir & [opts]]
                   (backup/backup-db! db-name' ds backup-dir opts))
     :close-fn   (fn []
                   (ops/close-batch-writer! batch-sys)
                   (close-in-memory-datasource! db-name ds anchor))}))
