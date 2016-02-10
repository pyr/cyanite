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
    {:engine (component/using
              (e/map->Engine {:rules {"default" ["5s:1h"]}})
              [:drift
               :queues
               :writer])
     :store  (s/memory-store)
     :writer (component/using
              (writer/map->Writer {})
              [:index
               :store
               :queues])}
    (let [store     (:store *system*)
          clock     (:clock *system*)
          index     (:index *system*)
          engine    (:engine *system*)
          base-time 1454877020]
      (doseq [i (range 0 11)]
        (set-time! clock (* (+ base-time i) 1000))
        (e/accept! engine {:path "a.b.c" :metric i :time (+ base-time i)})
        (e/accept! engine {:path "a.b.d" :metric (* 100 i) :time (+ base-time i)})
        (e/accept! engine {:path "a.b.f" :metric (* 1000 i) :time (+ base-time i)}))

      (are [query result]
          (= (run-query! store index engine base-time (+ base-time 60)
                         [query])
             [{:target query :datapoints result}])

        "a.b.c"
        [[2.0 base-time] [7.0 (+ base-time 5)]]

        "scale(a.b.c,2.0)"
        [[4.0 base-time] [14.0 (+ base-time 5)]]

        "derivative(a.b.c)"
        [[2.0 base-time] [5.0 (+ base-time 5)]]

        "sumSeries(a.b.*)"
        [[2202.0 base-time] [7707.0 (+ base-time 5)]]))))
