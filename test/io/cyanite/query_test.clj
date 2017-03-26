(ns io.cyanite.query-test
     (:require [io.cyanite.query           :refer :all]
               [io.cyanite.test-helper     :refer :all]
               [com.stuartsierra.component :as component]
               [io.cyanite.store           :as s]
               [io.cyanite.engine          :as e]
               [clojure.test               :refer :all]))

(defn mk-cfg
  [m]
  (merge {:engine {:rules {"default" ["5s:1h"]}}}
         m))

(def CONFIG
  [(mk-cfg {:store  {:cluster  "localhost"
                     :keyspace "cyanite_test"}
            :index  {:type     :cassandra
                     :cluster  "localhost"
                     :keyspace "cyanite_test"}})
   (mk-cfg {{:store  {:type     :memory}}
            {:index  {:type     :atom}}})])

(deftest query-test
  (with-config
    CONFIG
    {}
    (let [store     (:store *system*)
          clock     (:clock *system*)
          index     (:index *system*)
          engine    (:engine *system*)
          base-time 1454877020]
      (doseq [i (range 0 10)]
        (e/ingest! engine "a.b.c" (+ base-time i) i)
        (e/ingest! engine "a.b.d" (+ base-time i) (* 100 i))
        (e/ingest! engine "a.b.f" (+ base-time i) (* 1000 i)))

      (is (= (run-query! index engine base-time (+ base-time 60)
                         ["a.b.*"])
             [{:target "a.b.c" :datapoints [[2.0 base-time] [7.0 (+ base-time 5)]]}
              {:target "a.b.d" :datapoints [[200.0 base-time] [700.0 (+ base-time 5)]]}
              {:target "a.b.f" :datapoints [[2000.0 base-time] [7000.0 (+ base-time 5)]]}]))

      (are [query expansion result]
          (= (run-query! index engine base-time (+ base-time 60)
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
        [[2202.0 base-time] [7707.0 (+ base-time 5)]]
        ))))
