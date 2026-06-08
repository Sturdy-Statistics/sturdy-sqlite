(ns sturdy.sqlite.test-support
  (:require
   [taoensso.telemere :as t]))

(set! *warn-on-reflection* true)

(defn- remove-all-handlers! []
  (doseq [h (keys (t/get-handlers))]
    (t/remove-handler! h)))

(defn run-quietly! [thunk]
  ;; Save current handlers, silence logging, run, restore.
  (let [saved-handlers (t/get-handlers)]
    (remove-all-handlers!)
    (t/add-handler! :noop (fn [_] nil))
    (try
      (thunk)
      (finally
        (remove-all-handlers!)
        (doseq [[k v] saved-handlers]
          (t/add-handler! k v))))))

(defmacro with-quiet-logging
  "Execute BODY with Telemere logging suppressed (no console/file output)."
  [& body]
  `(run-quietly! (fn [] ~@body)))

(defn eventually
  "Poll `pred` every poll-ms up to timeout-ms. Returns the first truthy
   value from `pred`, or nil on timeout."
  ([pred timeout-ms] (eventually pred timeout-ms 25))
  ([pred ^long timeout-ms ^long poll-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (let [v (try (pred) (catch Throwable _ nil))]
         (if v
           v
           (if (< (System/currentTimeMillis) deadline)
             (do (Thread/sleep poll-ms) (recur))
             nil)))))))
