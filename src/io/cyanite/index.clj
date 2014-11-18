(ns io.cyanite.index
  "Implements a path index which tracks metric names."
  (:refer-clojure :exclude [replace])
  (:require [clojure.string  :refer [split join replace]]))

(defprotocol Index
  "The index provides a way to insert paths and later look them up"
  (register! [this tenant path])
  (query     [this tenant query recurse?])
  (prefixes  [this tenant path])
  (lookup    [this tenant path]))

(defn path-elem-re
  "Each graphite path elem may contain '*' wildcards, this
   functions yields a regexp pattern fro this format"
  [e]
  (re-pattern (format "^%s$" (replace e "*" ".*"))))

(defn path-q
  "For a complete path query, yield a list of regexp pattern
   for each element"
  [path]
  (map path-elem-re (split path #"\.")))

(defn matches?
  "Predicate testing a path against a query. partial? determines
   whether the query should be treated as a prefix query or an
   absolute query"
  [query path]
  (and (= (count query) (count path))
       (every? seq (map re-matches query path))))

(defn truncate
  "When doing prefix searches, we're only interested in returning
   prefixes, this function yields a map of two keys:

   * leaf: indicates whether this is a prefix or an actual point
   * path: the prefix"
  [depth path]
  (let [truncated (take depth path)]
    {:leaf (= truncated path) :path (join "." truncated)}))

(defn prefix?
  [query path]
  (every? seq (map re-matches query (take (count query) path))))

(defn wrapped-index
  [index]
  (reify
    Index
    (register! [this tenant path]
      (register! index tenant path))
    (prefixes [this tenant path]
      (let [q (path-q (str path "*"))]
        (->> (query index tenant path false)
             (set)
             (map #(split % #"\."))
             (filter (partial prefix? q))
             (map (partial truncate (count q)))
             (set)
             (sort-by :path))))
    (lookup [this tenant path]
      (->> (query index tenant path true)
           (set)
           (map #(split % #"\."))
           (filter (partial matches? (path-q path)))
           (map (partial join "."))))))
