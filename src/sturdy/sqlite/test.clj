(ns sturdy.sqlite.test
  (:require
   [sturdy.sqlite.core :as core]))

(defmacro with-test-db
  "Spins up an isolated, in-memory SQLite database with an anchor connection.
   Executes `body` with `ds-binding` bound to the HikariDataSource.
   Safely tears down the pool and anchor afterwards.

   If `classpath-prefix` is provided, automatically runs migrations before
   executing the body."
  [[ds-binding db-name & [classpath-prefix]] & body]
  `(let [system# (core/make-in-memory-datasource ~db-name)
         ~ds-binding (:datasource system#)]
     (try
       (when ~classpath-prefix
         ((:migrate-fn system#) ~classpath-prefix))
       ~@body
       (finally
         ((:close-fn system#))))))
