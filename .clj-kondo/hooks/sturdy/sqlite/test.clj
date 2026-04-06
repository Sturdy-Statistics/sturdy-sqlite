(ns hooks.sturdy.sqlite.test
  (:require [clj-kondo.hooks-api :as api]))

(defn with-test-db [{:keys [node]}]
  (let [args     (rest (:children node))
        bindings (first args)
        body     (rest args)]
    ;; Check if the first argument is actually a vector
    (if (and bindings (= :vector (:type bindings)))
      (let [b-args           (:children bindings)
            ds-binding       (first b-args)
            db-name          (second b-args)
            classpath-prefix (nth b-args 2 nil)

            ;; Construct a valid `let` binding vector: [ds-binding db-name]
            ;; This tells clj-kondo that `ds-binding` is a local var
            let-bindings     (if (and ds-binding db-name)
                               [ds-binding db-name]
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
