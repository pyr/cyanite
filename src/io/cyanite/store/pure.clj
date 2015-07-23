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
       (map (fn [[k v]] [k (-> v first :point (get "mean"))]))
       (reduce merge {})
       (merge nils)
       (sort-by key)
       (mapv val)))

(defn normalize
  [data]
  (vector
   (some-> data
           first
           :id
           (get "resolution")
           (get "precision"))
   data))

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
          get-path   (fn [{:keys [id]}] (get id "path"))
          by-path    (->> (group-by get-path points)
                          (map (fn [[k v]] [k (fill-in nil-points v)]))
                          (reduce merge {}))]
      {:from   min-point
       :to     max-point
       :step   precision
       :series by-path})
    {:from 0 :to 0 :step 0 :series {}}))
