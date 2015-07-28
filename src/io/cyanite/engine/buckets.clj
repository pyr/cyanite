(ns io.cyanite.engine.buckets
  (:require [io.cyanite.utils      :refer [nbhm keyset remove!]]
            [clojure.tools.logging :refer [debug]])
  (:import java.util.concurrent.atomic.AtomicLong
           java.util.concurrent.atomic.AtomicLongArray
           java.util.concurrent.atomic.AtomicReferenceArray
           io.cyanite.engine.rule.Resolution))

(deftype MetricTuple [^Resolution id ^String path]
  java.lang.Object
  (hashCode [this]
    (.hashCode [id path]))
  (equals [this other]
    (and
     (= (.id this) (.id other))
     (= (.path this) (.path other)))))

(deftype TimeSlotTuple [^String path ^Long slot]
  java.lang.Object
  (hashCode [this]
    (.hashCode (str path ":" slot)))
  (equals [this other]
    (and
     (= (.path this) (.path other))
     (= (.slot this) (.slot other)))))

(defrecord MetricSnapshot [path time mean max min sum resolution])

(defprotocol Mutable
  (add! [this metric]))

(defprotocol Snapshotable
  (snapshot! [this]))

(defrecord DriftSlot [drift]
  Mutable
  (add! [this ts]
    (let [now       (quot (System/currentTimeMillis) 1000)
          new-drift (- now ts)]
      (when (pos? new-drift)
        (locking this
          (if (> new-drift (.get drift))
            (.set drift new-drift))))))
  clojure.lang.IDeref
  (deref [this]
    (.get drift)))

(defn drift-slot
  []
  (DriftSlot. (AtomicLong. 0)))

(defrecord MetricTimeSlot [k nvalues values]
  Mutable
  (add! [this metric]
    (locking this
      (try
        (.set values (.getAndIncrement nvalues) (:metric metric))
        (catch Exception e
          ;; Silently drop overflows
          (debug e "exception while incrementing timeslot")))))
  Snapshotable
  (snapshot! [this]
    (locking this
      (loop [i       0
             sz      (.get nvalues)
             count   0
             sum     0
             minval  nil
             maxval  nil]
        (let [val   (.get values i)]
          (cond
            (zero? sz) nil
            (< i sz)   (recur (inc i)
                              sz
                              (inc count)
                              (+ val sum)
                              (if (nil? minval) val (min minval val))
                              (if (nil? maxval) val (max maxval val)))
            :else      (MetricSnapshot. (.path k)
                                        (.slot k)
                                        (double (/ sum count))
                                        maxval
                                        minval
                                        sum
                                        nil)))))))

(defn time-slot
  [resolution now]
  (let [p (:precision resolution)]
    (* p (quot now p))))

(defn insert-slot!
  [time-slots k]
  (let [slot (MetricTimeSlot.
              k
              (AtomicLong. 0)
              (AtomicReferenceArray. (int 10e3)))]
    (assoc! time-slots k slot)
    slot))

(defrecord MetricKey [k time-slots drift]
  Mutable
  (add! [this metric]
    (add! drift (:time metric))
    (let [slot (time-slot (.id k) (:time metric))
          k    (TimeSlotTuple. (:path metric) slot)
          mts  (or (get time-slots k)
                   (insert-slot! time-slots k))]
      (add! mts metric)
      (snapshot! this)))
  Snapshotable
  (snapshot! [this]
    (let [now       (quot (System/currentTimeMillis) 1000)
          drift-ts  (- now @drift)
          low-slot  (time-slot (.id k) drift-ts)
          slots     (keyset time-slots)
          old-slots (filter #(< (.slot %) low-slot) slots)]
      (try
        (vec
         (for [slot-key old-slots
               :let [slot (remove! time-slots slot-key)]
               :when slot]
           (snapshot! slot)))
        (catch Exception e
          (debug e "exception while snapshotting"))))))

(defn metric-key
  [^MetricTuple k]
  (MetricKey. k (nbhm) (drift-slot)))

(defn tuple
  [^Resolution id ^String path]
  (MetricTuple. id path))
