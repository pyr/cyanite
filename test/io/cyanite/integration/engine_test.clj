(ns io.cyanite.integration.engine-test
  (:require [io.cyanite.store           :refer :all]
            [io.cyanite.query          :refer :all]
            [clojure.test               :refer :all]
            [io.cyanite.test-helper     :refer :all]
            [io.cyanite.engine          :as engine]
            [io.cyanite.engine.rule     :as rule]
            [io.cyanite.index           :as index]
            [qbits.alia                 :as alia]))

(defn mmap-vals
  [f m]
  (zipmap (keys m)
          (map #(map f %) (vals m))))

(defn map-vals
  [f m]
  (zipmap (keys m)
          (map f (vals m))))

(defn insert-data
  [index store]
  (doseq [[metric base] [["a.b.c" 10] ["a.b.d" 20]]] ;; "a.b.e"
    (do
      (index/register! index metric)
      (doseq [i (range 1 100)]
        (insert! store
                 (engine/map->MetricSnapshot {:time (* 5 i)
                                              :mean (+ base i)
                                              :min  (+ base i)
                                              :max  (+ base i)
                                              :sum  (+ base i)
                                              :path metric
                                              :resolution (rule/map->Resolution {:precision 5 :period 3600})}))))))

(defn cleanup-tables
  [session]
  (doseq [table ["metric" "path" "segment"]]
    (alia/execute session (str "TRUNCATE TABLE " table))))

(deftest index-prefixes-test
  (with-config
    {:engine {:rules {"default" ["5s:1h"]}}
     :store {:cluster  "localhost"
             :keyspace "cyanite_test"}
     :index {:type     :cassandra
             :cluster  "localhost"
             :keyspace "cyanite_test"}}
    {}
    (let [index    (:index *system*)
          store    (:store *system*)
          engine   (:engine *system*)
          session  (:session store)]
      (insert-data index store)
      (let [result (->> (run-query! store index engine 0 100 ["a.b.*"])
                        (group-by :target)
                        (mmap-vals :datapoints)
                        (map-vals #(mapcat identity %))
                        (map-vals #(map first %)))]
        (is (= (map double (range 11 30))
               (get result "a.b.c")))
        (is (= (map double (range 21 40))
               (get result "a.b.d"))))
      (cleanup-tables session))))

(deftest index-no-wildcard-test
  (with-config
    {:engine {:rules {"default" ["5s:1h"]}}
     :store {:cluster  "localhost"
             :keyspace "cyanite_test"}
     :index {:type     :cassandra
             :cluster  "localhost"
             :keyspace "cyanite_test"}}
    {}
    (let [index    (:index *system*)
          store    (:store *system*)
          engine   (:engine *system*)
          session  (:session store)]
      (insert-data index store)

      (let [result (->> (run-query! store index engine 0 100 ["sumSeries(a.b.c,a.b.d)"])
                        (group-by :target)
                        (mmap-vals :datapoints)
                        (map-vals #(mapcat identity %))
                        (map-vals #(map first %)))]
        (is (= (map double (mapv + (range 11 30) (range 21 40)))
               (get result "sumSeries(a.b.c,a.b.d)"))))

      (let [result (->> (run-query! store index engine 0 100 ["a.b.c"])
                        (group-by :target)
                        (mmap-vals :datapoints)
                        (map-vals #(mapcat identity %))
                        (map-vals #(map first %)))]
        (is (= (map double (range 11 30))
               (get result "a.b.c"))))
      (let [result (->> (run-query! store index engine 0 100 ["a.b.d"])
                        (group-by :target)
                        (mmap-vals :datapoints)
                        (map-vals #(mapcat identity %))
                        (map-vals #(map first %)))]
        (is (= (map double (range 21 40))
               (get result "a.b.d"))))
      (cleanup-tables session))))
