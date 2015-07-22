(ns io.cyanite.engine.rule
  "Rule manipulation functions"
  (:require [clojure.string :refer [split]]))

(defn ->seconds
  "Takes a string containing a duration like 13s, 4h etc. and
   converts it to seconds"
  [s]
  (let [[_ value unit] (re-matches #"^([0-9]+)([a-z])$" s)
        quantity (Long/valueOf value)]
    (case unit
      "s" quantity
      "m" (* 60 quantity)
      "h" (* 60 60 quantity)
      "d" (* 24 60 60 quantity)
      "w" (* 7 24 60 60 quantity)
      "y" (* 365 24 60 60 quantity)
      (throw (ex-info (str "unknown unit: " unit) {})))))

(defrecord Resolution [precision points period id])

(defn fit?
  [resolution oldest ts]
  (and (<= (- ts (:period resolution))
           oldest)
       (:id resolution)))

(def default-resolution
  "By default, keep a 1 minute resolution for a day"
  (Resolution. 60 1440 86400 "60:86400"))

(defn ->resolution
  "Converts an individual resolution to a description map"
  [res-def]
  (cond
    (string? res-def)
    (let [[precision-string period-string] (split res-def #":" 2)
          precision-secs (->seconds precision-string)
          period-secs    (->seconds period-string)
          points-secs    (/ period-secs precision-secs)]
      (Resolution. precision-secs
                   points-secs
                   period-secs
                   (format "%s:%s" precision-secs period-secs)))

    (map? res-def)
    (-> res-def
        (update :precision ->seconds)
        (update :period    ->seconds)
        (as-> x (assoc x
                       :points (/ (:period x) (:precision x))
                       :id     (format "%s:%s" (:precision x) (:period x))))
        (map->Resolution))

    :else
    (throw (ex-info "invalid resolution definition" {:resolution res-def}))))

(defprotocol MetricMatcher
  (metric-matches? [this metric]))

(defrecord MetricRule [pattern resolutions]
  MetricMatcher
  (metric-matches? [this metric]
    (and
     (re-find pattern (:path metric))
     this)))

(defn ->exec-plan
  [rules metric]
  (when-let [rule (some #(metric-matches? % metric) rules)]
    (:resolutions rule)))

(defn ->rule
  [[pattern resolutions]]
  (MetricRule. (re-pattern (if (= (name pattern) "default") ".*" pattern))
               (if (seq resolutions)
                 (mapv ->resolution resolutions)
                 [default-resolution])))
