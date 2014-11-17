(ns io.cyanite.index.atom
  (:refer-clojure :exclude [replace])
  (:require [io.cyanite.index :as index]
            [clojure.tools.logging :refer [debug]]
            [clojure.string   :refer [split]]))

(defn atom-index
  [_]
  (let [store (atom {})]
    (reify
      index/Index
      (register! [this tenant path]
        (swap! store update-in [tenant] #(set (conj % (split path #"\."))))
        (debug "index now: " @store))
      (query [this tenant path recurse?]
        (get @store tenant)))))

(comment
  (def i (index/wrapped-index (atom-index nil)))

  (index/register! i "" "foo.baz")

  (index/query i "" "foo.*" true)
  (index/lookup i "" "foo.bar")

  (sequential? "foo")
  )
