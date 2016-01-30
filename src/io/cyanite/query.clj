(ns io.cyanite.query
  "Query handler for the Graphite DSL.
   This handler will first figure out which paths to fetch and
   then hand over the fetched paths to the AST processor."
  (:require [io.cyanite.query.parser :as parser]
            [io.cyanite.query.ast    :as ast]
            [io.cyanite.query.path   :as path]
            [io.cyanite.index        :as index]
            [io.cyanite.store        :as store]
            [io.cyanite.engine       :as engine]
            [clojure.tools.logging   :refer [debug]]))

(defn path-leaves
  [index paths]
  (reduce merge {} (for [p paths] [p (index/leaves index p)])))

(defn merge-paths
  [by-path series]
  (let [fetch-series (fn [leaf] (get-in series [:series (:path leaf)]))]
    (->> (for [[p leaves] by-path]
           [p (remove nil? (map fetch-series leaves))])
         (reduce merge {}))))

(defn add-date
  [from step data]
  (loop [res        []
         [d & ds]   data
         point      from]
    (if d (recur (conj res [d point]) ds (+ point step)) res)))

(defn run-query!
  [store index engine from to queries]
  (debug "running query: " (pr-str queries))
  (for [query queries]
    (let [tokens  (parser/query->tokens query)
          paths   (path/tokens->paths tokens)
          by-path (path-leaves index paths)
          leaves  (->> (mapcat val by-path)
                       (map :path)
                       (map (partial engine/resolution engine to))
                       (remove nil?)
                       (set))
          series  (store/query! store from to (seq leaves))
          merged  (merge-paths by-path series)
          from    (:from series)
          step    (:step series)]
      (let [[n [data]] (ast/run-query! tokens merged)]
        {:target n :datapoints (add-date from step data)}))))
