(ns sturdy.sqlite.test
  (:require
   [sturdy.sqlite.core :as core]))

(defmacro with-test-db
  "Spins up an isolated, in-memory SQLite database with an anchor connection.
   Executes `body` with `sys-binding` bound to the system map.

   `opts` can be a string (treated as `classpath-prefix`) or a map of options
   passed to the database factory (which can include `:classpath-prefix` and `:builder-opts`)."
  [[sys-binding db-name & [opts]] & body]
  `(let [opts#        (if (string? ~opts) {:classpath-prefix ~opts} (or ~opts {}))
         prefix#      (:classpath-prefix opts#)
         db-opts#     (dissoc opts# :classpath-prefix)
         sys#         (core/make-in-memory-datasource ~db-name db-opts#)
         ~sys-binding sys#]
     (try
       (when prefix#
         ((:migrate-fn sys#) prefix#))
       ~@body
       (finally
         ((:close-fn sys#))))))
