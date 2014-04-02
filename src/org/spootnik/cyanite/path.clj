(ns org.spootnik.cyanite.path
  "Implements a path store which tracks metric names. The
   default implementation provides lucene based implementations"
  (:require [clucy.core                  :as search]
            [clojure.tools.logging       :refer [error info debug]]))

(defprotocol Pathstore
  "The pathstore provides a way to insert paths and later look them up"
  (register [this tenant path])
  (search [this tenant path]))

(defn lucene-pathstore
  [index]
  (reify Pathstore
    (register [this tenant path]
      (search/add index {:tenant tenant :path path}))
    (search [this tenant path]
      (let [query (format "tenant:'%s' path:'%s'" tenant path)]
        (->> (loop [init []
                    page 0]
               (let [paths (map :path (search/search index query 1000000
                                                     :page page
                                                     :results-per-page 100))]

                 (if (< (count paths) 100)
                   (concat init paths)
                   (recur (concat init paths) (inc page))))))))))

(defn lucene-memory-pathstore
  [_]
  (lucene-pathstore (search/memory-index)))

(defn lucene-file-pathstore
  [{:keys [path]}]
  (lucene-pathstore (search/disk-index path)))
