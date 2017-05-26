(ns io.cyanite.engine.rule
  "Rule manipulation functions"
  (:require [clojure.string :refer [split]]))

(defn ->seconds
  "Takes a string containing a duration like 13s, 4h etc. and
   converts it to seconds"
  [s]
  (let [[_ value unit] (re-matches #"^([0-9]+)([a-z])$" s)
        quantity (Long/valueOf ^String value)]
    (case unit
      "s" quantity
      "m" (* 60 quantity)
      "h" (* 60 60 quantity)
      "d" (* 24 60 60 quantity)
      "w" (* 7 24 60 60 quantity)
      "y" (* 365 24 60 60 quantity)
      (throw (ex-info (str "unknown unit: " unit) {})))))

(defrecord Resolution [precision period])

(defrecord ResolutionChain [precisions])

(defn fit?
  "Does a point fit in a resolution, given
   a reference to now ?"
  [resolution oldest ts]
  (when (<= (- ts (:period resolution)) oldest)
    resolution))

(def default-resolution
  "By default, keep a 1 minute resolution for a day"
  (Resolution. 60 86400))

(defn ->resolution
  "Converts an individual resolution to a description map"
  [res-def]
  (cond
    (string? res-def)
    (let [[precision-string period-string] (split res-def #":" 2)
          precision-secs (->seconds precision-string)
          period-secs    (->seconds period-string)]
      (Resolution. precision-secs period-secs))

    (map? res-def)
    (-> res-def
        (update :precision ->seconds)
        (update :period    ->seconds)
        (map->Resolution))

    :else
    (throw (ex-info "invalid resolution definition" {:resolution res-def}))))

(defprotocol MetricMatcher
  (metric-matches? [this metric]))

(defrecord MetricRule [pattern resolutions]
  MetricMatcher
  (metric-matches? [this path]
    (and
     (re-find pattern path)
     this)))

(defn ->exec-plan
  [planner metric]
  (when-let [rule (some #(metric-matches? % metric) planner)]
    (:resolutions rule)))

(defn ->rule
  [[pattern resolutions]]
  (MetricRule. (re-pattern (if (= (name pattern) "default")
                             ".*"
                               (name pattern)))
               (if (seq resolutions)
                 (mapv ->resolution resolutions)
                 [default-resolution])))
