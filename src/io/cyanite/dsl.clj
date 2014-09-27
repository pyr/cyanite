(ns io.cyanite.dsl
  (:require [instaparse.core :as parse]))

(def init
  "<expr>           = path | funcall
    sumseries      = <#'(?i)sumseries'>
    scale          = <#'(?i)scale'>
    scaletoseconds = <#'(?i)scaletoseconds'>
    offset         = <#'(?i)offset'>
    percentile     = <#'(?i)percentile'>
    summarize      = <#'(?i)summarize'>
    movingavg      = <#'(?i)movingaverage'>
    movingmedian   = <#'(?i)movingmedian'>
    maxabove       = <#'(?i)maximumabove'>
    maxbelow       = <#'(?i)maximumbelow'>
    npercentile    = <#'(?i)npercentile'>
    limit          = <#'(?i)limit'>
    <uqpath>       = #'(?i)[a-z0-9.*]+'
    <qpath>        = <'\"'> uqpath <'\"'>
    path           = uqpath | qpath
    <func1>        = sumseries | scale | scaletoseconds | offset | percentile
    <func2>        = summarize | movingavg | movingmedian | maxabove
    <func3>        = maxbelow | npercentile | limit
    <func>         = func1 | func2 | func3
    <arglist>      = (expr <','>)* (Epsilon | expr)
    <funcall>      = func <'('> arglist <')'>")

(def query->ast
  (parse/parser init))

(defn extract-paths
  [ast]
  (->> ast
       (filter (comp (partial = :path) first))
       (map last)
       (set)))
