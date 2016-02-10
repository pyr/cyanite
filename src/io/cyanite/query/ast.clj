(ns io.cyanite.query.ast
  (:require [clojure.string :as s]))

(def ^:dynamic *series*)

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
  (let [payload (transform! series)
        width   (count (second payload))]
    (when-not (<= (or floor 0) width (or ceiling Long/MAX_VALUE))
      (throw (ex-info "invalid width for series"
                      {:ceiling ceiling
                       :floor   floor
                       :width   width
                       :series  series})))
    payload))

(defn merge-resolve!
  [series floor ceiling]
  (let [transformed (map transform! series)
        width       (reduce + 0 (map (comp count second) transformed))]
    (when-not (<= (or floor 0) width (or ceiling Long/MAX_VALUE))
      (throw (ex-info "invalid width for series"
                      {:ceiling ceiling
                       :floor   floor
                       :width   width
                       :series  transformed})))
    [(s/join "," (map first transformed))
     (reduce concat [] (map second transformed))]))

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
    [renamed [(apply mapv (nil-safe-op outer) (flatten-series (nil-safe-op inner) series))]]))

(extend-protocol SeriesTransform
  String
  (transform! [this]
    [this (or (get *series* this) [])])
  clojure.lang.PersistentVector
  (transform! [this]
    this))

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
     (let [st (volatile! ::none)]
       (fn [i]
         (if-not (nil? i)
           (let [prev @st]
             (vreset! st i)
             (if (= ::none prev)
               i
               (- i prev))))))

     nil
     (resolve! series 1 1))))

(defrecord AbsoluteOperation [series]
  SeriesTransform
  (transform! [this]
    (traverse!
     "absolute($0)"
     #(Math/abs %)
     nil
     (resolve! series 1 1))))

(defmulti  tokens->ast first)

(defmethod tokens->ast :path
  [[_ path]]
  path)

(defmethod tokens->ast :sumseries
  [[_ & series]]
  (SumOperation. (mapv tokens->ast series)))

(defmethod tokens->ast :divideseries
  [[_ top bottom]]
  (DivOperation. (tokens->ast top) (tokens->ast bottom)))

(defmethod tokens->ast :scale
  [[_ series factor]]
  (ScaleOperation. (Double. factor) (tokens->ast series)))

(defmethod tokens->ast :absolute
  [[_ series]]
  (AbsoluteOperation. (tokens->ast series)))

(defmethod tokens->ast :derivative
  [[_ series]]
  (DerivativeOperation. (tokens->ast series)))

(defmethod tokens->ast :default
  [x]
  (throw (ex-info "unsupported function in cyanite" {:arg x})))

(defn run-query!
  [tokens series]
  (binding [*series* series]
    (transform! (tokens->ast tokens))))

(comment
  ;;
  (require 'io.cyanite.query.series.parser)
  (run-query! (io.cyanite.query.series.parser/query->tokens
               "sumSeries(f*)")
              {"f*" [["f1" [1 1 1]]
                     ["f2" [2 2 2]]]})

  ;;
  (run-query! (io.cyanite.query.series.parser/query->tokens
               "divideSeries(sumSeries(g*),absolute(scale(sumSeries(f1,f2),-2)))")
              {"g*" [["g1" [1 1 1]]
                     ["g2" [1 1 1]]]
               "f1" [["f1" [1 1 1]]]
               "f2" [["f2" [1 1 1]]]})


  )
