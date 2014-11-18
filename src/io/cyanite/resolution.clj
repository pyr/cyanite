(ns io.cyanite.precision)

(defprotocol PrecisionCaps
  (ttl [this])
  (rollup [this time]))

(defprotocol FetchSpecCaps
  (precision [this precisions])
  (aggregate [this data]))

(defrecord Precision [rollup period]
  PrecisionCaps
  (ttl [this] (* rollup period))
  (rollup [this time] (-> time (quot rollup) (* rollup))))

(defrecord FetchSpec [agg paths from to]
  FetchSpecCaps
  (precision [this precisions]
    (let [now    (quot (System/currentTimeMillis) 1000) ;; epoch
          within (fn [precision] (>= from (- now (ttl precision))))]
      (->> precisions
           (sort-by :rollup)
           (drop-while (complement within))
           (first))))
  (aggregate [this metric]
    (let [data       (:data metric)
          agged      (when-let [data (seq (:data metric))]
                       (cond
                        (= agg :mean) (/ (reduce + 0.0 data) (count data))
                        (= agg :sum)  (reduce + 0.0 data)
                        (= agg :max)  (reduce max Double/MIN_VALUE data)
                        (= agg :min)  (reduce min Double/MAX_VALUE data)
                        (= agg :raw)  data))]
      (-> metric
          (dissoc :data)
          (assoc :metric agged)))))

(comment
  (precision (FetchSpec. :mean [] 1416173290 1416173290)
             [(Precision. 10 60480) (Precision. 600 105120)]
             )

  )
