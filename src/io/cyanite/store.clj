(ns io.cyanite.store
  "Implements a metric store on top of cassandra. This currently
   relies on a single schema. All cassandra interaction bits
   should quickly be abstracted with a protocol to more easily
   swap implementations"
  (:require [io.cyanite.precision :as p]
            [clojure.tools.logging :refer [debug]]))

(set! *warn-on-reflection* true)

(defrecord Metric [path time point data])

(defprotocol Metricstore
  (insert! [this tenant metric] [this tenant precision metric])
  (fetch [this tenant spec] [this tenant precision spec]))

;;
;; The next section contains a series of path matching functions

(defn max-points
  "Returns the maximum number of points to expect for
   a given resolution, time range and number of paths"
  [paths rollup from to]
  (-> (- to from)
      (/ rollup)
      (long)
      (inc)
      (* (count paths))
      (int)))

(defn fill-in
  "Fill in fetched data with nil metrics for a given time range"
  [nils data]
  (->> (group-by :time data)
       (merge nils)
       (map (comp first val))
       (sort-by :time)
       (map :metric)))

(defn data->series
  [data spec precision]
  (when-let [points (seq (map (partial p/aggregate spec) data))]
    (let [rollup     (:rollup precision)
          min-point  (-> points first :time)
          max-point  (-> (:to spec) (quot rollup) (* rollup))
          nil-points (->> (range min-point (inc max-point) rollup)
                          (map (fn [t] [t [{:time t}]]))
                          (reduce merge {}))
          by-path    (->> (group-by :path points)
                          (map (fn [[k v]] [k (fill-in nil-points v)]))
                          (reduce merge {}))]
      {:from min-point
       :to   max-point
       :step rollup
       :series by-path})))

(defn empty-series
  [spec]
  {:from (:from spec)
   :to (:to spec)
   :step 10 ;; inconsequent
   :series {}})

(defn wrapped-store
  [store precisions]
  (reify
    Metricstore
    (insert! [this tenant metric]
      (doseq [precision precisions
              :let [time (p/rollup precision (:time metric))]]
        (insert! store tenant precision
                 (map->Metric (assoc metric :time time)))))
    (fetch [this tenant spec]
      (let [precision (p/precision spec precisions)]
        (or (data->series (fetch store tenant precision spec) spec precision)
            (empty-series spec))))))
