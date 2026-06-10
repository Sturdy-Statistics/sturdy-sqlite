(ns hooks.sturdy.sqlite.macros)

(defmacro with-test-db [bindings & body]
  (let [sys-binding (first bindings)
        db-name (second bindings)
        opts (nth bindings 2 nil)]
    `(let [~sys-binding nil
           _# ~db-name
           _# ~opts]
       ~@body)))
