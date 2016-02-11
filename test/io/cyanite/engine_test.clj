(ns io.cyanite.engine-test
  (:require [io.cyanite.engine          :as e]
            [com.stuartsierra.component :as component]
            [clojure.test               :refer :all]
            [io.cyanite.test-helper     :refer :all]))


(deftest engine-test
  (with-config
    {:engine {:rules {"default" ["5s:1h"]}}}
    {}
    (let [engine    (:engine *system*)
          clock     (:clock *system*)
          writer    (:writer *system*)

          base-time 1454877020]
      (doseq [i (range 0 6)]
        (set-time! clock (* (+ base-time i) 1000))
        (e/ingest! engine {:path "a.b.c" :metric i :time (+ base-time i)}))

      (let [[res] (e/snapshot! engine)]
        (is (= "a.b.c" (:path res)))
        (is (= 2.0 (:mean res)))
        (is (= 10 (:sum res)))
        (is (= 4 (:max res)))
        (is (= 0 (:min res)))
        (is (= base-time (:time res)))))))
