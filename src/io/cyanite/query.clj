(ns io.cyanite.query
  "Query handler for the Graphite DSL.
   This handler will first figure out which paths to fetch and
   then hand over the fetched paths to the AST processor."
  (:require [io.cyanite.query.parser :as parser]
            [io.cyanite.query.ast    :as ast]
            [io.cyanite.query.path   :as path]
            [io.cyanite.index        :as index]
            [io.cyanite.store        :as store]
            [io.cyanite.engine       :as engine]))

(defn path-leaves
  [index paths]
  (reduce merge {} (for [p paths] [p (index/leaves index p)])))

(defn merge-paths
  [by-path series]
  (->> (for [[p leaves] by-path]
         [p (remove nil? (map (partial get series) leaves))])
       (reduce merge {})))

(defn run-query!
  [index store engine from to query]
  (let [tokens  (parser/query->tokens query)
        paths   (path/tokens->paths tokens)
        by-path (path-leaves index paths)
        leaves  (->> (mapcat val by-path)
                     (partial engine/resolution from)
                     (remove nil?)
                     (set))
        series  (store/query! store from to (seq leaves))
        merged  (merge-paths by-path series)
        params  (select-keys series [:from :to :step])]
    (assoc params :series (ast/run-query! tokens merged))))
