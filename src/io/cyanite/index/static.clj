(ns io.cyanite.index.static
  (:require [io.cyanite.index :as index]
            [clojure.string   :refer [split]]))

(defn static-index
  [{:keys [tenants]}]
  (reify
    index/Index
    (register! [this tenant path])
    (query [this tenant path recurse?]
      (map #(split % #"\.")
           (get tenants (keyword tenant))))))

(comment
  (def i (index/wrapped-index
          (static-index {:tenants {(keyword "")
                                   ["foo.bar" "foo.baz"]}})))

  (index/lookup i "" "foo.*")
  )
