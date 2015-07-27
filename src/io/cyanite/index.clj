(ns io.cyanite.index
  (:require [com.stuartsierra.component :as component]
            [clojure.string             :as s]
            [io.cyanite.index.es        :as es]
            [io.cyanite.utils           :refer [nbhm keyset assoc-if-absent!]]
            [clojure.string             :refer [split]]
            [clojure.set                :refer [union intersection]]))

(declare segmentize)
(declare register-segment!)
(declare query-plan)
(declare query-segment)
(declare prefix-list)
(declare explode-fnmatch-query)

(defrecord Prefix [pos prefix])

(defprotocol MetricIndex
  (get-db [this])
  (register! [this metric])
  (leaves [this query])
  (prefixes [this query]))

(defrecord MemoryIndex [db]
  component/Lifecycle
  (start [this]
    (assoc this :db (nbhm)))
  (stop [this]
    (dissoc this :db))
  MetricIndex
  (get-db [this]
    db)
  (register! [this metric]
    (let [path     (:path metric)
          segments (segmentize path)
          info     {:path path :length (count segments)}]
      (doseq [[index segment] segments]
        (register-segment!
         db segment
         (assoc info :index index :segment segment)))))
  (leaves [this query]
    (->> (query-plan query true)
         (map (partial query-segment db))
         (apply intersection)))
  (prefixes [this query]
    (->> (query-plan query false)
         (map (partial query-segment db))
         (apply intersection))))

(defrecord ElasticSearchIndex [options client]
  component/Lifecycle
  (start [this]
    (assoc this :client (es/client options)))
  (stop [this]
    (dissoc this :client))
  MetricIndex
  (register! [this metric]
    (es/register! client (:path metric) (-> metric :resolution :period)))
  (leaves [this query]
    (let [queries (explode-fnmatch-query query)]
      (set
       (mapcat #(es/query client % true) queries))))
  (prefixes [this query]
    (let [queries (explode-fnmatch-query query)]
      (set
       (mapcat #(es/query client % false) queries)))))

(defmulti build-index (comp (fnil keyword "memory") :type))

(defmethod build-index :memory
  [options]
  (MemoryIndex. nil))

(defmethod build-index :elasticsearch
  [options]
  (ElasticSearchIndex. options nil))

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
  (->> (split path #"\.")
       (map-indexed vector)))

(defn register-segment!
  [db segment info]
  (doseq [prefix (prefix-list (:segment info))]
    (let [k     (Prefix. (:index info) prefix)
          cell  (or (get db k) (nbhm))]
      (assoc! cell (:path info) info)
      (assoc-if-absent! db k cell))))

(defn repeat-elems
  [query]
  (loop [factor      1
         [e & elems] query
         res         []]
    (if (nil? e)
      [factor res]
      (recur (* factor (count e))
             elems
             (conj res (->> (mapv (partial repeat factor) e)
                            (flatten)
                            (vec)))))))
(defn cycle-elems
  [n elems]
  (->> (map cycle elems)
       (map (partial take n))
       (map vec)))

(defn combine-lists
  [lists]
  (let [[n elems] (repeat-elems lists)
        cycled    (cycle-elems n elems)]
    (loop [queries     (repeat n [])
           [e & elems] cycled]
      (if (nil? e)
        queries
        (recur (for [[q next] (partition 2 (interleave queries e))]
                 (conj q next))
               elems)))))

(defn explode-string
  [query re f]
  (let [m (re-matcher re query)]
    (loop [i   0
           res []]
      (if-let [mr (and (.find m) (.toMatchResult m))]
        (recur
         (.end mr)
         (-> res
             (conj (vector (.substring query i (.start mr))))
             (conj (f (.substring query (.start mr) (.end mr))))))
        (if (< i (.length query))
          (conj res (vector (.substring query i)))
          res)))))

(defn curly-vector
  [curly]
  (let [trimmed (.substring curly 1 (dec (.length curly)))]
    (split trimmed #",")))

(defn parse-curlies
  [query]
  (let [exploded (explode-string query #"\{[^}]+\}" curly-vector)]
    (combine-lists exploded)))

(defn charclass-vector
  [curly]
  (let [trimmed (.substring curly 1 (dec (.length curly)))]
    (mapv str (seq trimmed))))

(defn parse-charclasses
  [query]
  (let [exploded (explode-string query #"\[[^]]+\]" charclass-vector)]
    (combine-lists exploded)))

(defn s->prefix
  [i s]
  (let [[prefix & _] (split s #"\*")]
    (Prefix. i (or prefix ""))))

(defn s->matcher
  [s]
  (if (re-find #"\*" s)
    (let [pat (re-pattern (s/replace s "*" ".*"))]
      (fn [candidate]
        (re-find pat candidate)))
    (fn [candidate] (= s candidate))))

(defn matcher-applies?
  [input index matcher]
  (and (= (:index input) index)
       (matcher (:segment input))))

(defn yield-segment
  [index length valid-strings]
  {:valid    valid-strings
   :index    index
   :prefixes (mapv (partial s->prefix index) valid-strings)
   :matcher  (let [matchers (mapv s->matcher valid-strings)]
               (if length
                 (fn [input]
                   (and (= length (:length input))
                        (some (partial matcher-applies? input index) matchers)))
                 (fn [input]
                   (some (partial matcher-applies? input index) matchers))))})

(defn explode-fnmatch-query
  [q]
  (->> (parse-curlies q)
       (map (partial apply str))
       (mapcat parse-charclasses)
       (map (partial apply str))))

(defn parse-segment
  [length [index query]]
  (->> (parse-curlies query)
       (map (partial apply str))
       (mapcat parse-charclasses)
       (map (partial apply str))
       (yield-segment index length)))

(defn query-plan
  [path length?]
  (let [segments (segmentize path)]
    (mapv (partial parse-segment (when length? (count segments))) segments)))

(defn prefix-list
  [s]
  (loop [current  ""
         prefixes []
         [h & t]  (seq s)]
    (if (nil? h)
      (conj prefixes s)
      (recur (str current h) (conj prefixes current) t))))

(defn index-matches?
  [index info]
  (= index (:index info)))

(defn print-intermediate
  [prefix coll]
  (println prefix (pr-str (vec coll)))
  coll)

(defn query-segment
  [db {:keys [prefixes valid index matcher]}]
  (->> (mapcat (partial get db) prefixes)
       (filter matcher)
       (map :path)
       (set)))

;; Workbench
;; =========

(comment

  (let [i (component/start (MemoryIndex. nil))
        db (get-db i)]
    (register! i {:path "foo.bar.baz"})
    (register! i {:path "foo.qux.baz"})
    (register! i {:path "foo.qux.baz.gix"})

    (leaves i "fo[ob].*.{baz,biz}")

    )


  )
