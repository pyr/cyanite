(ns io.cyanite.engine
  "The core of cyanite"
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.engine.rule     :as rule]
            [io.cyanite.engine.queue    :as q]
            [spootnik.reporter          :as r]
            [io.cyanite.utils           :refer [nbhm assoc-if-absent! now! entries remove!]]
            [io.cyanite.engine.drift    :refer [drift! skewed-epoch!]]
            [clojure.tools.logging      :refer [info debug error]])
  (:import io.cyanite.engine.rule.Resolution))

(defprotocol Enqueuer
  (enqueue! [this value]))

(defprotocol Resolutioner
  (resolution [this oldest path]))

(defprotocol Ingester
  (ingest! [this metric]))

(defprotocol Snapshoter
  (snapshot! [this] [this now]))

(defn time-slot
  [^Resolution resolution ^Long now]
  (let [p (:precision resolution)]
    (* p (quot now p))))

(defrecord MetricSnapshot [path time mean min max sum])

(defrecord MetricMonoid [count minv maxv sum]
  Ingester
  (ingest! [this val]
    (MetricMonoid. (inc count)
                   (if minv (min minv val) val)
                   (if maxv (max maxv val) val)
                   (+ sum val)))
  Snapshoter
  (snapshot! [this now]
    (MetricSnapshot. nil now (double (/ sum count)) minv maxv sum)))

(defrecord MetricResolution [resolution slots]
  Ingester
  (ingest! [this metric]
    (let [val        (:metric metric)
          slot       (time-slot resolution (:time metric))
          new-monoid #(atom (MetricMonoid. 0 nil nil 0))
          monoid     (or (get slots slot) (new-monoid))]
      (swap! monoid ingest! val)))
  Snapshoter
  (snapshot! [this now]
    (let [floor   (time-slot resolution now)
          entries (filter #(< (key %) floor) (entries slots))]
      (doseq [slot (map key entries)]
        (remove! slots slot))
      (mapv #(snapshot! (val %) (key %)) entries))))

(defn make-resolutions
  [rules metric]
  (let [plan (rule/->exec-plan rules metric)]
    (mapv #(MetricResolution. % (nbhm)) plan)))

(defn fetch-resolutions
  [state rules metric]
  (let [path        (:path metric)
        resolutions (or (get state path)
                        (make-resolutions rules metric))]
    (assoc-if-absent! state path resolutions)
    resolutions))

(defn snapshot-resolution
  [path now resolution]
  (mapv #(assoc % :path path) (snapshot! resolution now)))

(defn snapshot-path
  [now [path resolutions]]
  (vec (mapcat (partial snapshot-resolution path now))))

(defrecord Engine [rules state queues ingestq planner drift reporter]
  component/Lifecycle
  (start [this]
    (let [state   (nbhm)
          planner (map rule/->rule rules)
          ingestq (:ingestq queues)]
      (info "starting engine")
      (q/consume! ingestq (partial ingest! this))
      (r/instrument! reporter [:cyanite])
      (assoc this :planner planner :state state :ingestq ingestq)))
  (stop [this]
    (assoc this :planner nil :state nil :ingestq nil))
  Ingester
  (ingest! [this metric]
    (drift! drift (:time metric))
    (doseq [resolution (fetch-resolutions state rules metric)]
      (ingest! resolution metric)))
  Enqueuer
  (enqueue! [this metric]
    (q/add! ingestq metric))
  Snapshoter
  (snapshot! [this]
    (snapshot! this (skewed-epoch! drift)))
  (snapshot! [this now]
    (vec (mapcat (partial snapshot-path now) (entries state))))
  Resolutioner
  (resolution [this oldest path]
    (let [plan (rule/->exec-plan planner {:path path})
          ts   (now!)]
      (when-let [resolution (some #(rule/fit? % oldest ts)
                                  (sort-by :precision plan))]
        {:path path :resolution resolution}))))

(prefer-method print-method clojure.lang.IRecord clojure.lang.IDeref)
