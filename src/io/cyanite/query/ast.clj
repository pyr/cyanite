(ns io.cyanite.query.ast
  (:require [clojure.string :as s]))

(defn nil-safe-op
  [f]
  (fn [& vals]
    (if (some nil? vals)
      nil
      (apply f vals))))

(defprotocol SeriesTransform
  "A protocol to realize operations on series."
  (transform! [this]))

(defn resolve!
  [series floor ceiling]
  (let [series (first (transform! series))
        width   (count (second series))]
    (when-not (<= (or floor 0) width (or ceiling Long/MAX_VALUE))
      (throw (ex-info "invalid width for series"
                      {:ceiling ceiling
                       :floor   floor
                       :width   width
                       :series  series})))
    series))

(defn merge-resolve!
  [series floor ceiling]
  (let [series      (mapcat transform! series)
        width       (reduce + 0 (map (comp count second) series))]
    (when-not (<= (or floor 0) width (or ceiling Long/MAX_VALUE))
      (throw (ex-info "invalid width for series"
                      {:ceiling ceiling
                       :floor   floor
                       :width   width
                       :series series})))
    [(s/join "," (map first series))
     (reduce concat [] (map second series))]))

(defn flatten-series
  [f series]
  (for [[name payload] series]
    (cond
      (and (nil? f) (> 1 (count payload)))
      (throw (ex-info (str "Cannot flatten series with nil inner function. "
                           "A likely reason is that you used a single-arity "
                           "function on a path that resolves to multiple "
                           "series.")
                      {}))

      (= 1 (count payload))
      (-> payload first)

      :else
      (apply mapv f payload))))


(defn index-series
  [series]
  (let [names (map first series)
        wc    (s/join "," names)]
    (->> names
         (map-indexed #(vector (str %1) %2))
         (reduce merge {}))))

(defn series-rename
  [series repr]
  (let [indexed (index-series series)]
    (s/replace repr
               #"\$([0-9*]+)"
               (fn [x] (get indexed (second x) "")))))

(defn traverse!
  [repr outer inner & series]
  (let [renamed (series-rename series repr)]
    [[renamed [(apply mapv (nil-safe-op outer) (flatten-series (nil-safe-op inner) series))]]]))

(defn add-date
  [from step data]
  (loop [res        []
         [d & ds]   (first data)
         point      from]
    (if ds
      (recur (if d (conj res [d point]) res) ds (+ point step))
      res)))

(defrecord SumOperation [series]
  SeriesTransform
  (transform! [this]
    (let [merged (merge-resolve! series 1 nil)]
      (traverse! "sumSeries($0)" + + merged))))

(defrecord DivOperation [top bottom]
  SeriesTransform
  (transform! [this]
    (traverse!
     "divideSeries($0,$1)"
     /
     nil
     (resolve! top 1 1)
     (resolve! bottom 1 1))))

(defrecord ScaleOperation [factor series]
  SeriesTransform
  (transform! [this]
    (traverse!
     (format "scale($0,%s)" factor)
     (partial * factor)
     nil
     (resolve! series 1 1))))

(defrecord DerivativeOperation [series]
  SeriesTransform
  (transform! [this]
    (traverse!
     "derivative($0)"
     (let [safe-nil-subtract (nil-safe-op -)]
       (fn [[a b]] (safe-nil-subtract b a)))
     nil
     (mapcat (fn [[k v]] [k (map #(partition 2 1 (cons nil %)) v)])
             (partition 2 2
                        (resolve! series 1 1))))))

(defrecord AbsoluteOperation [series]
  SeriesTransform
  (transform! [this]
    (traverse!
     "absolute($0)"
     #(Math/abs %)
     nil
     (resolve! series 1 1))))

(defrecord IdentityOperation [path series]
  SeriesTransform
  (transform! [this]
    (if (nil? path)
      (mapv
       (fn [[k v]]
         [k [v]]) series)
      [[path [(get series path)]]])))

(defmulti  tokens->ast
  (fn [series tokens]
    (first tokens)))

(defn wildcard-path?
  [path]
  (.contains path "*"))

(defmethod tokens->ast :path
  [series [_ path]]
  (if (wildcard-path? path)
    (IdentityOperation. nil series)
    (IdentityOperation. path series)))

(defmethod tokens->ast :sumseries
  [series [_ & tokens]]
  (SumOperation. (mapv #(tokens->ast series %) tokens)))

(defmethod tokens->ast :divideseries
  [series [_ top bottom]]
  (DivOperation. (tokens->ast series top) (tokens->ast series bottom)))

(defmethod tokens->ast :scale
  [series [_ path factor]]
  (ScaleOperation. (Double. factor) (tokens->ast series path)))

(defmethod tokens->ast :absolute
  [series [_ path]]
  (AbsoluteOperation. (tokens->ast series path)))

(defmethod tokens->ast :derivative
  [series [_ path]]
  (DerivativeOperation. (tokens->ast series path)))

(defmethod tokens->ast :default
  [series x]
  (throw (ex-info "unsupported function in cyanite" {:arg x})))

(defn run-query!
  ;; for testing purposes
  ([tokens series]
   (transform! (tokens->ast series tokens)))
  ([tokens series from step]
   (for [[n datapoints] (transform! (tokens->ast series tokens))]
     {:target n :datapoints (add-date from step datapoints)})))
