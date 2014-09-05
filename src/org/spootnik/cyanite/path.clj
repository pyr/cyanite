(ns org.spootnik.cyanite.path
  "Implements a path store which tracks metric names."
  (:require [clojure.tools.logging :refer [error info debug]]
            [clojure.string        :refer [split join] :as str]
            [lamina.core           :refer [channel receive-all]]
            [clojure.core.async :as async :refer [<! >! go chan]]))

(defprotocol Pathstore
  "The pathstore provides a way to insert paths and later look them up"
  (channel-for [this])
  (register [this tenant path])
  (prefixes [this tenant path])
  (lookup   [this tenant path]))

(defn path-elem-re
  "Each graphite path elem may contain '*' wildcards, this
   functions yields a regexp pattern fro this format"
  [e]
  (re-pattern (format "^%s$" (str/replace e "*" ".*"))))

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

(defn memory-pathstore
  [_]
  (let [store (atom {})]
    (reify Pathstore
      (register [this tenant path]
        (swap! store update-in [tenant] #(set (conj % (split path #"\.")))))
      (channel-for [this]
        (let [c (chan)]
          (go
            (while true
              (let [p (<! c)]
                (register this "" p))))
          c))
      (prefixes [this tenant path]
        (let [pstar (str path "*")
              query (path-q pstar)]
          (->> (get @store tenant)
               (filter (partial prefix? query))
               (map (partial truncate (count query)))
               (set)
               (sort-by :path))))
      (lookup [this tenant path]
        (->> (get @store tenant)
             (filter (partial matches? (path-q path)))
             (map (partial join ".")))))))
