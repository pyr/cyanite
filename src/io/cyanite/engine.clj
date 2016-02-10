(ns io.cyanite.engine
  "The core of cyanite"
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.engine.rule     :as rule]
            [io.cyanite.engine.queue    :as q]
            [io.cyanite.utils           :refer [nbhm assoc-if-absent! now!]]
            [io.cyanite.engine.drift    :refer [drift! skewed-epoch!]]
            [clojure.tools.logging      :refer [info debug error]])
  (:import io.cyanite.engine.rule.Resolution))

(defprotocol Acceptor
  (accept! [this value]))

(defprotocol Resolutionator
  (resolution [this oldest path]))

(defprotocol Ingester
  (ingest! [this metric]))

(defprotocol Snapshoter
  (snapshot! [this] [this floor]))

(defn time-slot
  [^Resolution resolution ^Long now]
  (let [p (:precision resolution)]
    (* p (quot now p))))

(defrecord MetricSnapshot [mean min max sum])

(defrecord MetricMonoid [count minv maxv sum]
  Ingester
  (ingest! [this val]
    (MetricMonoid. (inc count)
                   (if minv (min minv val) val)
                   (if maxv (max maxv val) val)
                   (+ sum val)))
  clojure.lang.IDeref
  (deref [this]
    (MetricSnapshot. (double (/ sum count)) minv maxv sum)))

(defrecord MetricResolution [resolution slots]
  Ingester
  (ingest! [this metric]
    (let [val        (:metric metric)
          slot       (time-slot resolution (:time metric))
          new-monoid #(atom (MetricMonoid. 0 nil nil 0))
          monoid     (or (get slots slot) (new-monoid))]
      (swap! monoid ingest! val))))

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

(defrecord Engine [rules state queues ingestq planner drift writer]
  component/Lifecycle
  (start [this]
    (let [state   (nbhm)
          planner (map rule/->rule rules)
          ingestq (:ingestq queues)]
      (info "starting engine")
      (q/consume! ingestq (partial ingest! this))
      (assoc this :planner planner :state state :ingestq ingestq)))
  (stop [this]
    (assoc this :planner nil :state nil :ingestq nil))
  Ingester
  (ingest! [this metric]
    (drift! drift (:time metric))
    (doseq [resolution (fetch-resolutions state rules metric)]
      (ingest! resolution metric)))
  Acceptor
  (accept! [this metric]
    (q/add! ingestq metric))
  Snapshoter
  (snapshot! [this]
    (snapshot! this (skewed-epoch! drift)))
  (snapshot! [this floor]
    (error "No snapshoting for now, I will eat up all your ram!"))
  Resolutionator
  (resolution [this oldest path]
    (let [plan (rule/->exec-plan planner {:path path})
          ts   (now!)]
      (when-let [resolution (some #(rule/fit? % oldest ts)
                                  (sort-by :precision plan))]
        {:path path :resolution resolution}))))

(prefer-method print-method clojure.lang.IRecord clojure.lang.IDeref)
