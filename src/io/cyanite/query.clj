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
            [clojure.tools.logging   :refer [debug]]
            [clojure.string          :refer [join]]
            ))

(def aggregates ["min" "max" "mean" "sum"])
(def pattern (re-pattern (str "(.*)(\\_)(" (join "|" aggregates) ")")))

(defn maybe-multiplex
  [paths]
  (mapcat
   (fn [path]
     (if (not (:expandable path))
       (map #(assoc path
                    :path (str (:path path) %)
                    :text (str (:text path) %))
            (cons "" (map #(str "_" %) aggregates)))
       [path]))
   paths))

(defn extract-aggregate
  [path]
  (if-let [[_ extracted :as all] (re-matches pattern path)]
    [extracted (keyword (last all))]
    [path :default]))

(defn path-leaves
  [index paths]
  (zipmap paths
          (map #(index/prefixes index (first %)) paths)))

(defn merge-paths
  [by-path series]
  (let [fetch-series (fn [leaf aggregate]
                       (let [path (store/reconstruct-aggregate (:path leaf) aggregate)]
                         [path (get-in series [:series path])]))]
    (->> by-path
         (map (fn [[[_ aggregate] leaves]]
                (->> leaves
                     (map #(fetch-series % aggregate))
                     (remove #(nil? (second %))))))
         (mapcat identity)
         (reduce merge {}))))

(defn run-query!
  [store index engine from to queries]
  (debug "running query: " (pr-str queries))
  (flatten
   (for [query queries]
     (let [tokens     (parser/query->tokens query)
           paths      (->> tokens
                           (path/tokens->paths)
                           (map extract-aggregate))
           ;; by this point we have "real" paths (without aggregates)
           by-path    (path-leaves index paths)
           leaves     (->> by-path
                           (mapcat
                            (fn [[[_ aggregate] paths]]
                              (map
                               #(engine/resolution engine from to (:path %) aggregate)
                               paths
                               )))
                           (remove nil?)
                           (distinct))
           series     (store/query! store from to leaves)
           merged     (merge-paths by-path series)
           from       (:from series)
           step       (:step series)]
       (ast/run-query! tokens merged from step)))))
