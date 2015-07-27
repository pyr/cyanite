(ns io.cyanite.store.pure)

(defn max-points
  [paths resolution from to]
  (-> (- to from)
      (/ resolution)
      (long)
      (inc)
      (* (count paths))
      (int)))

(defn fill-in
  [nils data]
  (->> (group-by :time data)
       (map (fn [[k v]] [k (-> v first :point :mean)]))
       (reduce merge {})
       (merge nils)
       (sort-by key)
       (mapv val)))

(defn resolution-gcd
  [data]
  (some-> data first :id :resolution :precision))

(defn normalize-to
  [gcd]
  (fn [data]
    data))

(defn normalize
  [data]
  (let [gcd (resolution-gcd data)]
    (vector gcd (map (normalize-to gcd) data))))

(defn empty-series
  [min-point max-point precision]
  (->> (range min-point (inc max-point) precision)
       (map #(vector % nil))
       (reduce merge {})))

(defn data->series
  [data to precision]
  (if-let [points (seq (sort-by :time data))]
    (let [min-point  (-> points first :time)
          max-point  (-> to (quot precision) (* precision))
          nil-points (empty-series min-point max-point precision)
          by-path    (->> (group-by (comp :path :id) points)
                          (map (fn [[k v]] [k (fill-in nil-points v)]))
                          (reduce merge {}))]
      {:from   min-point
       :to     max-point
       :step   precision
       :series by-path})
    {:from 0 :to 0 :step 0 :series {}}))
