(ns io.cyanite.query.ast
  (:require [clojure.string :as s]))

(defn nil-safe-op
  [f]
  (if (nil? f)
    nil
    (fn [& vals]
      (if (some nil? vals)
        nil
        (apply f vals)))))

(defprotocol SeriesTransform
  "A protocol to realize operations on series."
  (transform! [this]))

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
    [[(s/join "," (map first series))
      (reduce conj [] (map second series))]]))

(defn flatten-series
  [reducer mapper series]
  (mapcat
   (fn [[name payload]]
     (let [payload (if (nil? mapper)
                     payload
                     ;; we need a second mapper here for cases when
                     ;; we sum absolute values or similar
                     (map #(map mapper %) payload))]
       (if (nil? reducer)
         payload
         (reducer payload))))
   series))


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
  [rename-fn combiner reducer mapper series]
  (let [renamed   (series-rename series rename-fn)
        flattened (flatten-series (nil-safe-op reducer) (nil-safe-op mapper) series)
        combined  (if (nil? combiner)
                    flattened
                    (apply mapv (nil-safe-op combiner) flattened))]
    [[renamed combined]]))

(defn traverse-each!
  [rename-fn reducer mapper series]
  (for [s series]
    (let [renamed   (series-rename [s] rename-fn)
          mapped    (if (nil? mapper)
                      (second s)
                      (map (nil-safe-op mapper) (second s)))
          reduced   (if (nil? reducer)
                      mapped
                      (reducer mapped))]
      [renamed reduced])))



(defn add-date
  [from step data]
  (loop [res        []
         [d & ds]   data ;;(first data)
         point      from]
    (if ds
      (recur (if d (conj res [d point]) res) ds (+ point step))
      res)))

(defrecord SumOperation [series]
  SeriesTransform
  (transform! [this]
    (let [merged (merge-resolve! series 1 nil)]
      (traverse!
       "sumSeries($0)"
       +
       nil
       nil
       merged))))

(defrecord DerivativeOperation [series]
  SeriesTransform
  (transform! [this]
    (traverse-each!
     "derivative($0)"
     (fn [s]
       (cons nil (map #(apply (nil-safe-op -) (reverse %)) (partition 2 1 s))))
     nil
     (transform! series))))

(defn lift-single-series
  "Lifts the series to "
  [series]
  (mapcat (fn [[k v]] [k [v]])
          (partition 2 2
                     series)))

(defrecord AbsoluteOperation [series]
  SeriesTransform
  (transform! [this]
    (traverse-each!
     "absolute($0)"
     nil
     #(Math/abs %)
     (transform! series))))

(defrecord DivOperation [top bottom]
  SeriesTransform
  (transform! [this]
    (traverse!
     "divideSeries($0)"
     /
     nil
     nil
     (merge-resolve! [top bottom]
                     1 nil))))

(defrecord ScaleOperation [factor series]
  SeriesTransform
  (transform! [this]
    (traverse-each!
     (format "scale($0,%s)" factor)
     nil
     (partial * factor)
     (transform! series))))

(defrecord IdentityOperation [path series]
  SeriesTransform
  (transform! [this]
    (if (nil? path)
      (mapv
       (fn [[k v]]
         [k v]) series)
      [[path (get series path)]])))

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
