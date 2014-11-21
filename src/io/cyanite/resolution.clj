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

(defn to-seconds
  "Takes a string containing a duration like 13s, 4h etc. and
   converts it to seconds"
  [s]
  (let [[_ value unit] (re-matches #"^([0-9]+)([a-z])$" s)
        quantity (Integer/valueOf value)]
    (case unit
      "s" quantity
      "m" (* 60 quantity)
      "h" (* 60 60 quantity)
      "d" (* 24 60 60 quantity)
      "w" (* 7 24 60 60 quantity)
      "y" (* 365 24 60 60 quantity)
      (throw (ex-info (str "unknown rollup unit: " unit) {})))))

(defn any->Resolution
  [spec]
  (cond
   (string? spec)
   (let [[rollup-string retention-string] (seq (.split spec ":" 2))
         rollup-secs                      (to-seconds rollup-string)
         retention-secs                   (to-seconds retention-string)]
     (->Resolution rollup-secs (/ retention-secs rollup-secs)))

   (map? spec)
   (map->Resolution spec)

   (and (sequential? spec) (= 2 (count spec)))
   (apply ->Resolution spec)

   :else
   (throw (ex-info "invalid resolution spec" {:spec spec}))))



(comment
  (resolution (FetchSpec. :mean [] 1416173290 1416173290)
              [(Resolution. 10 60480) (Resolution. 600 105120)]
              )

  )
