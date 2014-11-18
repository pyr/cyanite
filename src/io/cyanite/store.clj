(ns io.cyanite.store
  "Implements a metric store on top of cassandra. This currently
   relies on a single schema. All cassandra interaction bits
   should quickly be abstracted with a protocol to more easily
   swap implementations"
  (:require [io.cyanite.resolution :as r]
            [clojure.tools.logging :refer [debug]]))

(set! *warn-on-reflection* true)

(defrecord Metric [path time point data])

(defprotocol Metricstore
  (insert! [this tenant metric] [this tenant resolution metric])
  (fetch [this tenant spec] [this tenant resolution spec]))

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
  [data spec resolution]
  (when-let [points (seq (map (partial r/aggregate spec) data))]
    (let [rollup     (:rollup resolution)
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
  [store resolutions]
  (reify
    Metricstore
    (insert! [this tenant metric]
      (doseq [resolution resolutions
              :let [time (r/rollup resolution (:time metric))]]
        (insert! store tenant resolution
                 (map->Metric (assoc metric :time time)))))
    (fetch [this tenant spec]
      (let [resolution (r/resolution spec resolutions)]
        (or (data->series (fetch store tenant resolution spec) spec resolution)
            (empty-series spec))))))
