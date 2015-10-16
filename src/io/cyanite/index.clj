(ns io.cyanite.index
  (:require [com.stuartsierra.component :as component]
            [clojure.string             :refer [join split]]
            [clojure.set                :refer [union intersection]]
            [globber.glob               :refer [glob]]))

(defprotocol MetricIndex
  (push-segment! [this pos segment path length])
  (by-pos        [this pos])
  (by-segment    [this pos segment]))

(defrecord AgentIndex [db]
  component/Lifecycle
  (start [this]
    (assoc this :db (agent {})))
  (stop [this]
    (assoc this :db nil))
  MetricIndex
  (push-segment! [this pos segment path length]
    (send-off db update pos
              (fn [segments segment path length]
                (into (sorted-map)
                      (update segments segment
                              (fn [paths tuple]
                                (into (sorted-set)
                                      (conj paths tuple)))
                              [path length])))
           segment path length))
  (by-pos [this pos]
    (-> @db (get pos) keys))
  (by-segment [this pos segment]
    (get (get @db pos) segment)))

(defrecord EmptyIndex []
  component/Lifecycle
  (start [this] this)
  (stop [this] this)
  MetricIndex
  (push-segment! [this pos segment path length])
  (by-pos [this pos])
  (by-segment [this pos segments]))

(defmulti build-index (comp (fnil keyword "agent") :type))

(defmethod build-index :empty
  [options]
  (EmptyIndex.))

(defmethod build-index :agent
  [options]
  (AgentIndex. nil))

;; Implementation
;; ==============

;;
;; We build an inverted index of segment to path
;; To service query we resolve and expand multiple
;; options (delimited in curly brackets) or multiple
;; characters (in a [] character class) then we dispatch
;; resolve our inverted index and filter on the result list
;;

(defn segmentize
  [path]
  (let [elems (split path #"\.")]
    (map-indexed vector elems)))

(defn by-segments
  [index pos segments]
  (mapcat (partial by-segment index pos) segments))

(defn register!
  [index path]
  (let [segments (segmentize path)
        length   (count segments)]
    (doseq [[i s] segments]
      (push-segment! index i s path length))))

(defn prefix-info
  [length [path matches]]
  (let [lengths (set (map second matches))]
    {:path         path
     :text         (last (split path #"\."))
     :id           path
     :allowChilren (if (some (partial < length) lengths) 1 0)
     :expandable   (if (some (partial < length) lengths) 1 0)
     :leaf         (if (boolean (lengths length)) 1 0)}))

(defn truncate-to
  [pattern-length [path length]]
  [(join "." (take pattern-length (split path #"\.")))
   length])

(defn matches
  [index pattern leaves?]
  (let [segments (segmentize pattern)
        length   (count segments)
        pred     (partial (if leaves? = <=) length)
        matches  (for [[pos pattern] segments]
                   (->> (by-pos index pos)
                        (glob pattern)
                        (by-segments index pos)
                        (filter (comp pred second))
                        (set)))
        paths    (reduce union #{} matches)]
    (->> (reduce intersection paths matches)
         (map (partial truncate-to length))
         (group-by first)
         (map (partial prefix-info length))
         (sort-by :path))))

(defn prefixes
  [index pattern]
  (matches index pattern false))

(defn leaves
  [index pattern]
  (matches index pattern true))

;; Workbench
;; =========
(comment

  (let [i  (component/start (AgentIndex. nil))
        db (:db i)]

    (register! i "foo.bar.baz.bim")
    (register! i "foo.bar.baz.bim.bam.boum.barf")
    (register! i "foo.bar.baz.bim.bam.boum")
    (register! i "foo.bar.qux")
    (register! i "bar.bar.qux")
    (register! i "foo.baz.qux")

    (await db)
    (matches i "foo.bar.*" false))







  )
