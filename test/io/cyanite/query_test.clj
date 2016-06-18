(ns io.cyanite.query-test
     (:require [io.cyanite.query :refer :all]
               [io.cyanite.test-helper :refer :all]
               [com.stuartsierra.component :as component]
               [io.cyanite.store :as s]
               [io.cyanite.engine :as e]
               [io.cyanite.engine.writer :as writer]
               [clojure.test :refer :all]))

(deftest query-test
  (with-config
    {:engine {:rules {"default" ["5s:1h"]}}}
    {}
    (let [store     (:store *system*)
          clock     (:clock *system*)
          index     (:index *system*)
          writer    (:writer *system*)
          engine    (:engine *system*)
          base-time 1454877020]
      (doseq [i (range 0 11)]
        (set-time! clock (* (+ base-time i) 1000))
        (e/ingest! engine {:path "a.b.c" :metric i :time (+ base-time i)})
        (e/ingest! engine {:path "a.b.d" :metric (* 100 i) :time (+ base-time i)})
        (e/ingest! engine {:path "a.b.f" :metric (* 1000 i) :time (+ base-time i)})
        (when (= 0 (mod i 5))
          (e/snapshot! writer)))

      (is (= (run-query! store index engine base-time (+ base-time 60)
                         ["a.b.*"])
             [{:target "a.b.c" :datapoints [[2.0 base-time] [7.0 (+ base-time 5)]]}
              {:target "a.b.d" :datapoints [[200.0 base-time] [700.0 (+ base-time 5)]]}
              {:target "a.b.f" :datapoints [[2000.0 base-time] [7000.0 (+ base-time 5)]]}]))

      (are [query expansion result]
          (= (run-query! store index engine base-time (+ base-time 60)
                         [query])
             [{:target expansion :datapoints result}])

        "a.b.c" "a.b.c"
        [[2.0 base-time] [7.0 (+ base-time 5)]]

        "scale(a.b.c,2.0)" "scale(a.b.c,2.0)"
        [[4.0 base-time] [14.0 (+ base-time 5)]]

        "derivative(a.b.c)" "derivative(a.b.c)"
        [[5.0 (+ base-time 5)]]

        "sumSeries(a.b.c,a.b.d,a.b.f)" "sumSeries(a.b.c,a.b.d,a.b.f)"
        [[2202.0 base-time] [7707.0 (+ base-time 5)]]

        "sumSeries(a.b.*)" "sumSeries(a.b.c,a.b.d,a.b.f)"
        [[2202.0 base-time] [7707.0 (+ base-time 5)]]))))
