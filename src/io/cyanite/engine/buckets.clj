(ns io.cyanite.engine.buckets
  (:require [io.cyanite.utils      :refer [nbhm keyset remove!]]
            [clojure.tools.logging :refer [debug]])
  (:import io.cyanite.engine.rule.Resolution))

(defrecord MetricTuple    [id path])
(defrecord MetricSnapshot [path time mean max min sum resolution])

(defprotocol Mutable
  (add-then-snap! [this metric now]))

(defn time-slot
  [^Resolution resolution ^Long now]
  (let [p (:precision resolution)]
    (* p (quot now p))))

(defn guarded
  "Wrap around a function f operating on two numbers. Guard
   against calling f on nil values by yielding in order of
   precedence: right, left and in the worst case: nil"
  [f]
  (fn [left right]
    (if right
      (if left (f left right) right)
      left)))

(defn augment-snap
  [snapshot {:keys [metric slot]}]
  (if-not metric
    snapshot
    (-> snapshot
        (assoc :slot slot)
        (update :count inc)
        (update :sum + metric)
        (update :max (guarded max) metric)
        (update :min (guarded min) metric))))

(defn process-snap
  [k {:keys [id slot count min max sum]}]
  (MetricSnapshot.
   (:path k)
   slot
   (double (/ sum count))
   max
   min
   sum
   (:id k)))

(def init-snap
  {:count 0
   :min   nil
   :max   nil
   :sum   0})

(defn take-snap
  [[_ vals] k]
  (let [sz   (count vals)
        path (:path k)
        res  (:id k)]
    (when (pos? (count vals))
      (process-snap k (reduce augment-snap init-snap vals)))))

(defn add-then-snap
  [[_ vals] metric k floor]
  (let [slot      (time-slot (:id k) (:time metric))
        vals      (sort-by :time (conj vals (assoc metric :slot slot)))
        [old new] (split-with #(< (:slot %) floor) vals)]
    [(map take-snap (group-by :slot old) (repeat k)) new]))

(defrecord MetricKey [k data]
  Mutable
  (add-then-snap! [this metric now]
    (let [slot          (time-slot (:id k) (:time metric))
          floor         (time-slot (:id k) now)
          [snapshots _] (swap! data add-then-snap metric k floor)]
      snapshots)))

(defn metric-key
  [^MetricTuple k]
  (MetricKey. k (atom nil)))

(defn tuple
  [^Resolution id ^String path]
  (MetricTuple. id path))
