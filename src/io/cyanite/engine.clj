(ns io.cyanite.engine
  "The core of cyanite"
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.engine.rule     :as rule]
            [io.cyanite.engine.queue    :as q]
            [spootnik.reporter          :as r]
            [io.cyanite.index           :as index]
            [io.cyanite.utils           :refer [nbhm assoc-if-absent! entries remove!]]
            [io.cyanite.engine.drift    :refer [drift! skewed-epoch! epoch!]]
            [clojure.tools.logging      :refer [info debug error]])
  (:import io.cyanite.engine.rule.Resolution))

(defprotocol Enqueuer
  (enqueue! [this value]))

(defprotocol Resolutioner
  (resolution [this oldest newest path]))

(defprotocol Ingester
  (ingest! [this metric]))

(defprotocol Snapshoter
  (snapshot! [this] [this now])
  (snapshot-by! [this matcher]))

(defn time-slot
  [^Resolution resolution ^Long now]
  (let [p (:precision resolution)]
    (* p (quot now p))))

(defrecord MetricSnapshot [time mean min max sum])

(defrecord MetricMonoid [count minv maxv sum]
  Ingester
  (ingest! [this val]
    (MetricMonoid. (inc count)
                   (if minv (min minv val) val)
                   (if maxv (max maxv val) val)
                   (+ sum val)))
  Snapshoter
  (snapshot! [this now]
    (MetricSnapshot. now (double (/ sum count)) minv maxv sum)))

(defn new-monoid
  []
  (MetricMonoid. 0 nil nil 0))

(defrecord MetricResolution [resolution slots]
  Ingester
  (ingest! [this metric]
    (let [val        (:metric metric)
          slot       (time-slot resolution (:time metric))
          monoid     (or (get slots slot) (new-monoid))]
      (.assoc slots slot (ingest! monoid val))))
  Snapshoter
  (snapshot! [this now]
    (let [floor    (time-slot resolution now)
          to-purge (filter #(< (key %) floor) (entries slots))]
      (doseq [slot (map key to-purge)]
        (remove! slots slot))
      (mapv #(snapshot! (val %) (key %)) to-purge))))

(defn make-resolutions
  [rules metric]
  (let [plan (rule/->exec-plan rules metric)]
    (mapv #(MetricResolution. % (nbhm)) plan)))

(defn fetch-resolutions
  [state rules metric absent-callback]
  (let [path        (:path metric)
        resolutions (or (get state path)
                        (make-resolutions rules metric))]
    (when (nil? (assoc-if-absent! state path resolutions))
      (absent-callback path))
    resolutions))

(defn snapshot-resolution
  [path now resolution]
  (mapv #(assoc % :path path :resolution (:resolution resolution)) (snapshot! resolution now)))

(defn snapshot-path
  [now [path resolutions]]
  (mapcat (partial snapshot-resolution path now) resolutions))

(defrecord Engine [rules state queues ingestq planner drift reporter index]
  component/Lifecycle
  (start [this]
    (let [state   (nbhm)
          planner (map rule/->rule rules)
          ingestq (:ingestq queues)]
      (info "starting engine")
      (let [this (assoc this :planner planner :state state :ingestq ingestq)]
        (q/consume! ingestq (partial ingest! this))
        (r/instrument! reporter [:cyanite])
        this)))
  (stop [this]
    (assoc this :planner nil :state nil :ingestq nil))
  Ingester
  (ingest! [this metric]
    (drift! drift (:time metric))
    (doseq [resolution (fetch-resolutions state planner metric
                                          #(r/time! reporter
                                                    [:cyanite :writer :indexing]
                                                    (index/register! index %)))]
      (ingest! resolution metric)))
  Enqueuer
  (enqueue! [this metric]
    (q/engine-event! ingestq metric))
  Snapshoter
  (snapshot! [this]
    (snapshot! this (skewed-epoch! drift)))
  (snapshot! [this now]
    (let [entry-set (entries state)]
      (doall (mapcat (partial snapshot-path now) entry-set))))
  (snapshot-by! [this matcher]
    (let [now       (skewed-epoch! drift)
          entry-set (->> state
                         (entries)
                         (filter #(matcher (.getKey %))))]

      (doall (mapcat (partial snapshot-path now) entry-set))))
  Resolutioner
  (resolution [this oldest newest path]
    (let [plan (->> (rule/->exec-plan planner {:path path})
                    (sort-by :precision))]
      (if-let [resolution (some #(rule/fit? % oldest newest)
                                plan)]
        {:path path :resolution resolution}
        {:path path :resolution (first plan)}))))

(prefer-method print-method clojure.lang.IRecord clojure.lang.IDeref)
