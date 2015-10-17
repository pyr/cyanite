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
  (->> (for [[p leaves] by-path]
         [p (remove nil? (map (partial get series) leaves))])
       (reduce merge {})))

(defn run-query!
  [store index engine from to query]
  (debug "running query: " (pr-str query))
  (let [tokens  (parser/query->tokens query)
        paths   (path/tokens->paths tokens)
        by-path (path-leaves index paths)
        leaves  (->> (mapcat val by-path)
                     (map (partial engine/resolution engine from))
                     (remove nil?)
                     (set))
        series  (store/query! store from to (seq leaves))
        merged  (merge-paths by-path series)
        params  (select-keys series [:from :to :step])]
    (debug "merged series:" (pr-str merged))
    (debug "tokens:" (pr-str tokens))
    (assoc params :series (ast/run-query! tokens merged))))


(comment
  (parser/query->tokens "STRESS.host.ip-0.com.graphite.stresser.a.mean")
  )
