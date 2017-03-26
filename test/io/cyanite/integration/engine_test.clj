(ns io.cyanite.integration.engine-test
  (:require [io.cyanite.store           :refer :all]
            [io.cyanite.query          :refer :all]
            [clojure.test               :refer :all]
            [io.cyanite.test-helper     :refer :all]
            [io.cyanite.engine          :as engine]
            [io.cyanite.engine.rule     :as rule]
            [io.cyanite.index           :as index]
            [qbits.alia                 :as alia]
            [io.cyanite.store :as store]))

(defn mmap-vals
  [f m]
  (zipmap (keys m)
          (map #(map f %) (vals m))))

(defn map-vals
  [f m]
  (zipmap (keys m)
          (map f (vals m))))

(defn populate
  [index store]
  (doseq [[path base] [["a.b.c" 10] ["a.b.d" 20]]] ;; "a.b.e"
    (do
      (index/register! index path)
      (doseq [i (range 1 100)]
        (insert! store
                 path
                 (rule/map->Resolution {:precision 5 :period 3600})
                 (engine/map->MetricSnapshot {:time (* 5 i)
                                              :mean (double (+ base i))
                                              :min  (double (+ base i))
                                              :max  (double (+ base i))
                                              :sum  (double (+ base i))}))))))

(defn cleanup
  [index store]
  (index/truncate! index)
  (store/truncate! store))

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

(deftest index-prefixes-test
  (with-config
    CONFIG
    {}
    (let [index   (:index *system*)
          store   (:store *system*)
          engine  (:engine *system*)
          session (:session store)]

      (populate index store)

      (let [result (->> (run-query! index engine 0 100 ["a.b.*"])
                        (group-by :target)
                        (mmap-vals :datapoints)
                        (map-vals #(mapcat identity %))
                        (map-vals #(map first %)))]
        (is (= (map double (range 11 30))
               (get result "a.b.c")))
        (is (= (map double (range 21 40))
               (get result "a.b.d"))))


      (cleanup index store))))

(deftest index-no-wildcard-test
  (with-config
    CONFIG
    {}
    (let [index    (:index *system*)
          store    (:store *system*)
          engine   (:engine *system*)
          session  (:session store)]
      (populate index store)

      (let [result (->> (run-query! index engine 0 100 ["sumSeries(a.b.c,a.b.d)"])
                        (group-by :target)
                        (mmap-vals :datapoints)
                        (map-vals #(mapcat identity %))
                        (map-vals #(map first %)))]
        (is (= (map double (mapv + (range 11 30) (range 21 40)))
               (get result "sumSeries(a.b.c,a.b.d)"))))

      (let [result (->> (run-query! index engine 0 100 ["a.b.c"])
                        (group-by :target)
                        (mmap-vals :datapoints)
                        (map-vals #(mapcat identity %))
                        (map-vals #(map first %)))]
        (is (= (map double (range 11 30))
               (get result "a.b.c"))))
      (let [result (->> (run-query! index engine 0 100 ["a.b.d"])
                        (group-by :target)
                        (mmap-vals :datapoints)
                        (map-vals #(mapcat identity %))
                        (map-vals #(map first %)))]
        (is (= (map double (range 21 40))
               (get result "a.b.d"))))
      (cleanup index store))))

(deftest trivial-ingest-test
  (with-config
    [(mk-cfg {:store  {:cluster  "localhost"
                       :keyspace "cyanite_test"}
              :index  {:type     :cassandra
                       :cluster  "localhost"
                       :keyspace "cyanite_test"}})
     (mk-cfg {{:store  {:type     :memory}}
              {:index  {:type     :atom}}})]
    {}
    (let [index    (:index *system*)
          store    (:store *system*)
          engine   (:engine *system*)
          session  (:session store)]

      (doseq [i (range 1 8)]
        (engine/ingest! engine "a.b.c" i i))

      (is (= [[3.0 0] [6.5 5]]
             (:datapoints (first (run-query! index engine 0 100 ["a.b.c"])))))
      (cleanup index store))))
