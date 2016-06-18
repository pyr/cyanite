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
  (zipmap paths
          (map #(index/leaves index %) paths)))

(defn merge-paths
  [by-path series]
  (let [fetch-series (fn [leaf]
                       [(:path leaf) (get-in series [:series (:path leaf)])])]

    (->> by-path
         (map (fn [[p leaves]]
                (remove #(nil? (second %)) (map fetch-series leaves))))
         (mapcat identity)
         (reduce merge {}))))

(defn run-query!
  [store index engine from to queries]
  (debug "running query: " (pr-str queries))
  (flatten
   (for [query queries]
     (let [tokens     (parser/query->tokens query)
           paths      (path/tokens->paths tokens)
           by-path    (path-leaves index paths)
           leaves     (->> (mapcat val by-path)
                           (map :path)
                           (map (partial engine/resolution engine from to))
                           (remove nil?)
                           (distinct))
           series     (store/query! store from to leaves)
           merged     (merge-paths by-path series)
           from       (:from series)
           step       (:step series)]
       (ast/run-query! tokens merged from step)))))
