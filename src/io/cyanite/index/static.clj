(ns io.cyanite.index.static
  (:require [io.cyanite.index :as index]))

(defn static-index
  [{:keys [tenants]}]
  (reify
    index/Index
    (register! [this tenant path])
    (query [this tenant path recurse?]
      (get tenants (keyword tenant)))))
