(ns io.cyanite.index
  (:require [com.stuartsierra.component :as component]
            [clojure.string             :refer [join split]]
            [clojure.set                :refer [union intersection]]
            [globber.glob               :refer [glob]]))

(defprotocol MetricIndex
  (register!      [this path])
  (prefixes       [this pattern])

  (multiplex-aggregates [this prefixes])
  (extract-aggregate    [this prefix]))

;; Path explansion / artificial aggreate paths
;;

(defn make-pattern
  [aggregates]
  (re-pattern (str "(.*)(\\_)(" (join "|" aggregates) ")")))

(defn multiplex-aggregates-paths
  [aggregates paths]
  (mapcat
   (fn [path]
     (if (not (:expandable path))
       (map #(assoc path
                    :path (str (:path path) %)
                    :text (str (:text path) %))
            (cons "" (map #(str "_" %) aggregates)))
       [path]))
   paths))

(defn extract-aggregate-path
  [pattern path]
  (if-let [[_ extracted :as all] (re-matches pattern path)]
    [extracted (keyword (last all))]
    [path :default]))

;; Implementation
;; ==============

;;
;; We build an inverted index of segment to path
;; To service query we resolve and expand multiple
;; options (delimited in curly brackets) or multiple
;; characters (in a [] character class) then we dispatch
;; resolve our inverted index and filter on the result list
;;

(defn- segmentize
  [path]
  (let [elems (split path #"\.")]
    (map-indexed vector elems)))

(defn prefix-info
  [length [path matches]]
  (let [lengths (set (map second matches))]
    {:path         path
     :text         (last (split path #"\."))
     :id           path
     :allowChildren (if (some (partial < length) lengths) 1 0)
     :expandable   (if (some (partial < length) lengths) 1 0)
     :leaf         (if (boolean (lengths length)) 1 0)}))

(defn truncate-to
  [pattern-length [path length]]
  [(join "." (take pattern-length (split path #"\.")))
   length])



(defn- push-segment*
  [segments segment path length]
  (into (sorted-map)
        (update segments segment
                (fn [paths tuple]
                  (into (sorted-set)
                        (conj paths tuple)))
                [path length])))

(defn- by-pos
  [db pos]
  (-> @db (get pos) keys))

(defn- by-segment
  [db pos segment]
  (get (get @db pos) segment))

(defn- by-segments
  [db pos segments]
  (mapcat (partial by-segment db pos) segments))

(defn- matches
  [db pattern leaves?]
  (let [segments (segmentize pattern)
        length   (count segments)
        pred     (partial (if leaves? = <=) length)
        matches  (for [[pos pattern] segments]
                   (->> (by-pos db pos)
                        (glob pattern)
                        (by-segments db pos)
                        (filter (comp pred second))
                        (set)))
        paths    (reduce union #{} matches)]
    (->> (reduce intersection paths matches)
         (map (partial truncate-to length))
         (group-by first)
         (map (partial prefix-info length))
         (sort-by :path))))

;;
;; Indexes
;;

(defrecord AtomIndex [options db aggregates pattern]
  component/Lifecycle
  (start [this]
    (let [aggregates (or (:aggregates options) [])]
      (assoc this
             :db (atom {})
             :aggregates aggregates
             :pattern (make-pattern aggregates))))
  (stop [this]
    (assoc this
           :db nil
           :aggregates nil
           :pattern nil))
  MetricIndex
  (register! [this path]
    (let [segments (segmentize path)
          length   (count segments)]
      (doseq [[pos segment] segments]
        (swap! db update pos
               push-segment*
               segment path length))))
  (prefixes [index pattern]
    (matches db pattern false))

  (multiplex-aggregates [this prefixes]
    (multiplex-aggregates-paths aggregates prefixes))
  (extract-aggregate   [this prefix]
    (extract-aggregate-path pattern prefix)))

(defrecord EmptyIndex []
  component/Lifecycle
  (start [this] this)
  (stop [this] this)
  MetricIndex
  (register! [this path])
  (prefixes [index pattern]))

(defmulti build-index (comp (fnil keyword "agent") :type))

(defmethod build-index :empty
  [options]
  (EmptyIndex.))

(defmethod build-index :atom
  [options]
  (map->AtomIndex options))
