(ns hooks.sturdy.sqlite.test
  (:require [clj-kondo.hooks-api :as api]))

(defn with-test-db [{:keys [node]}]
  (let [args     (rest (:children node))
        bindings (first args)
        body     (rest args)]
    ;; Check if the first argument is actually a vector
    (if (and bindings (= :vector (:type bindings)))
      (let [b-args           (:children bindings)
            sys-binding      (first b-args)   ;; Can be a symbol or a destructuring map
            db-name          (second b-args)
            classpath-prefix (nth b-args 2 nil)

            ;; Construct a valid `let` binding vector: [sys-binding db-name]
            ;; clj-kondo natively understands map destructuring. By pairing the
            ;; `sys-binding` node with `db-name`, clj-kondo will automatically
            ;; register `:datasource`, `:write-fn`, etc., as locals in the body.
            let-bindings     (if (and sys-binding db-name)
                               [sys-binding db-name]
                               [])

            ;; Push the optional prefix into the body so clj-kondo lints it
            ;; (e.g., checks if the string/variable actually exists)
            new-body         (if classpath-prefix
                               (cons classpath-prefix body)
                               body)]

        ;; Return the transformed AST
        {:node (api/list-node
                (list* (api/token-node 'let)
                       (api/vector-node let-bindings)
                       new-body))})

      ;; Fallback if the macro is used incorrectly (e.g., missing the vector)
      {:node node})))
