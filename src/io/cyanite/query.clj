(ns io.cyanite.query
  "Query handler for the Graphite DSL.
   This handler will first figure out which paths to fetch and
   then hand over the fetched paths to the AST processor."
  (:require [io.cyanite.query.parser :as parser]
            [io.cyanite.query.ast    :as ast]
            [io.cyanite.query.path   :as path]
            [io.cyanite.index        :as index]
            [io.cyanite.store        :as store]))

(defn by-name
  [index store from to path]
  (let [leaves (index/leaves index path)]
    (store/fetch! store from to leaves)))

(defn by-pattern
  [index store from to paths]
  (reduce merge {} (for [p paths] [p (by-name index store from to p)])))

(defn run-query!
  [index store from to q]
  (let [tokens (parser/query->tokens q)
        paths  (path/tokens->paths tokens)
        series (by-pattern index store from to paths)]
    (ast/run-query! tokens series)))

(comment

  (parser/query->tokens "divideSeries(sumSeries(f1,f2),scale(f3,2.0))")
  (ast/tokens->ast (parser/query->tokens "scale(f1,2.0)"))
  (ast/tokens->ast (parser/query->tokens "sumSeries(scale(f1,2.0),divideSeries(f2,f3))"))
  (run-query! "scale(f1,2.0)")
  (run-query! "sumSeries(f1,f2)")
  (run-query! "divideSeries(sumSeries(f1,f2),scale(f3,2.0))")

)
