(ns sturdy.sqlite.migrate
  (:require
   [ragtime.next-jdbc :as ragtime-jdbc]
   [ragtime.repl :as ragtime-repl]
   [taoensso.telemere :as t]))

(set! *warn-on-reflection* true)

(defn- ragtime-config [db-name ds classpath-prefix]
  {:datastore  (ragtime-jdbc/sql-database ds)
   ;; Loads from the classpath (e.g. "migrations" looks in resources/migrations/)
   :migrations (ragtime-jdbc/load-resources classpath-prefix)

   :reporter   (fn [_store op id]
                 (t/log! {:level :info
                          :id    ::migration-step
                          :msg   (str (if (= op :up) "Applying " "Rolling back ") id)
                          :data  {:db-name      db-name
                                  :operation    op
                                  :migration-id id}}))})

(defn migrate!
  "Applies all pending database migrations found on the classpath.
   `classpath-prefix` is typically a string like \"migrations\"."
  [db-name ds classpath-prefix]
  (t/log! {:level :info
           :id ::migrate
           :msg "Applying database migrations..."
           :data {:db-name db-name
                  :prefix  classpath-prefix}})
  (ragtime-repl/migrate (ragtime-config db-name ds classpath-prefix)))

(defn rollback!
  "Rolls back the most recently applied migration."
  [db-name ds classpath-prefix]
  (t/log! {:level :warn
           :id ::rollback
           :msg "Rolling back latest database migration..."
           :data {:db-name db-name
                  :prefix  classpath-prefix}})
  (ragtime-repl/rollback (ragtime-config db-name ds classpath-prefix)))
