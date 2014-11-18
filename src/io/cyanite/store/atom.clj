(ns io.cyanite.store.atom
  (:require [io.cyanite.store :as store]))

(defn atom-store
  [_]
  (let [store (atom {})]
    (reify
      store/Metricstore
      (insert! [this tenant resolution metric]
        (swap! store update-in
               [tenant (:path metric) resolution (:time metric)]
               conj (:point metric)))
      (fetch [this tenant resolution spec]
        (->> (for [path (:paths spec)
                   :let [points (get-in @store [tenant path resolution])
                         sorted (sort-by key points)]]
               (->> sorted
                    (drop-while #(< (key %) (:from spec)))
                    (take-while #(< (key %) (:to spec)))
                    (map (comp (partial apply store/->Metric)
                               (juxt (constantly path)
                                     key
                                     (constantly nil)
                                     val)))))
             (reduce concat []))))))
