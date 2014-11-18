(ns io.cyanite.resolution)

(defprotocol ResolutionCaps
  (ttl [this])
  (rollup [this time]))

(defprotocol FetchSpecCaps
  (resolution [this resolutions])
  (aggregate [this data]))

(defrecord Resolution [rollup period]
  ResolutionCaps
  (ttl [this] (* rollup period))
  (rollup [this time] (-> time (quot rollup) (* rollup))))

(defrecord FetchSpec [agg paths from to]
  FetchSpecCaps
  (resolution [this resolutions]
    (let [now    (quot (System/currentTimeMillis) 1000) ;; epoch
          within (fn [res] (>= from (- now (ttl res))))]
      (->> resolutions
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
  (resolution (FetchSpec. :mean [] 1416173290 1416173290)
              [(Resolution. 10 60480) (Resolution. 600 105120)]
              )

  )
