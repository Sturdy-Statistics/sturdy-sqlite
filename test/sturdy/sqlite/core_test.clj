(ns sturdy.sqlite.core-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [sturdy.sqlite.core :as core]
   [sturdy.sqlite.test-support :as ts])
  (:import
   (java.io Closeable)))

(set! *warn-on-reflection* true)

(deftest close-datasource-test
  (ts/with-quiet-logging
   (testing "Graceful handling of close-datasource! errors"
     (let [dummy-ds (reify Closeable
                      (close [_] (throw (ex-info "Simulated close error" {}))))]
       (is (nil? (core/close-datasource! "testdb" dummy-ds))
           "Should catch error and not throw"))))

  (ts/with-quiet-logging
   (testing "Graceful handling of close-in-memory-datasource! errors"
     (let [dummy-ds (reify Closeable
                      (close [_] (throw (ex-info "Simulated DS close error" {}))))
           dummy-anchor (reify Closeable
                          (close [_] (throw (ex-info "Simulated anchor close error" {}))))]
       (is (nil? (core/close-in-memory-datasource! "testdb" dummy-ds dummy-anchor))
           "Should catch both errors and not throw")))))
