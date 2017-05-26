(ns io.cyanite.engine
  "The core of cyanite"
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.engine.rule     :as rule]
            [io.cyanite.engine.queue    :as q]
            [spootnik.reporter          :as r]
            [io.cyanite.store.pure      :as p]
            [io.cyanite.index           :as index]
            [io.cyanite.store           :as store]
            [io.cyanite.utils           :refer [nbhm assoc-if-absent! contains-key entries remove!]]
            [io.cyanite.engine.drift    :refer [drift! skewed-epoch! epoch!]]
            [clojure.tools.logging      :refer [info debug error]])
  (:import io.cyanite.engine.rule.Resolution))

(defprotocol Resolutioner
  (resolution [this oldest newest path aggregate]))

(defprotocol Metric
  (snapshot! [this])
  (append! [this ts val]))

(defprotocol Engine
  (ingest! [this path ts metric])
  (query [this from to leaves])
  ;; TODO: GC metrics that weren't updated for a longer time
  )

(defn time-slot
  [^Resolution resolution ^Long now]
  (let [p (:precision resolution)]
    (* p (quot now p))))

(defrecord MetricSnapshot [time mean min max sum])

(defn new-state
  [ts v]
  (let [bit-v (Double/doubleToRawLongBits v)]
    (long-array [ts 1 bit-v bit-v bit-v])))

(defn update-state
  [[old-ts count minv maxv sum] ts v]
  (let [minv (Double/longBitsToDouble minv)
        maxv (Double/longBitsToDouble maxv)
        sum  (Double/longBitsToDouble sum)]
    (long-array [ts
                 (inc count)
                 (Double/doubleToRawLongBits (min minv v))
                 (Double/doubleToRawLongBits (max maxv v))
                 (Double/doubleToRawLongBits (+ sum v))])))

(defn to-snapshot
  [[ts count minv maxv sum]]
  (let [min (Double/longBitsToDouble minv)
        max (Double/longBitsToDouble maxv)
        sum (Double/longBitsToDouble sum)]
    (MetricSnapshot. ts
                     (/ sum count)
                     min
                     max
                     count)))

(defrecord MetricState [resolution state]
  Metric
  (append! [this ts v]
    (loop []
      (let [current        @state
            [ret-v to-set] (if (nil? current)
                             ;; new window opens, set timestamp
                             [nil (new-state ts v)]
                             (let [timestamp (first current)
                                   diff (- ts timestamp)
                                   window-size (:precision resolution)
                                   current-window? (> window-size diff)]
                               (if current-window?
                                 ;; should we include in the current window?
                                 [nil (update-state current timestamp v)]
                                 ;; do a snapshot and put metric to the new window
                                 [current (new-state ts v)])))]

        (if (compare-and-set! state current to-set)
          (if (nil? ret-v) nil (to-snapshot ret-v))
          (recur)))))

  (snapshot! [this]
    (if-let [current @state]
      (to-snapshot current)
      (throw (ex-info "Can't take a snapshot of an empty metric" {})))))

(defn make-metric-state
  [resolution]
  (MetricState. resolution (atom nil)))

(defrecord DefaultEngine [rules state store queues ingestq planner drift reporter index]
  component/Lifecycle
  (start [this]
    (let [state   (nbhm)
          planner (map rule/->rule rules)
          ingestq (:ingestq queues)]
      (info "starting engine")
      (let [this (assoc this :planner planner :state state :ingestq ingestq)]
        ;; (q/consume! ingestq (partial ingest! this))
        (r/instrument! reporter [:cyanite])
        this)))
  (stop [this]
    (assoc this :planner nil :state nil :ingestq nil))

  Engine
  (ingest! [this path ts value]
    (when-not (contains-key state path)
      (index/register! index path)
      (let [resolutions (rule/->exec-plan planner path)]
        (assoc-if-absent! state path (atom
                                      (zipmap resolutions
                                              (map #(make-metric-state %)
                                                   resolutions))))))

    (doseq [metric-monoid (vals @(get state path))]
      (when-let [snapshot (append! metric-monoid ts value)]
        (store/insert! store path (:resolution metric-monoid) snapshot))))

  (query [this from to paths]
    (let [raw-series         (store/fetch! store from to paths)
          current-points     (map
                              (fn [{:keys [path resolution aggregate] :as id}]
                                ;; try finding an in-memory state
                                (when-let [resolutions (get state path)]
                                  (when-let [state (get @resolutions resolution)]
                                    (let [snapshot (snapshot! state)
                                          time     (:time snapshot)]
                                      (when (and (>= time from) (<= time to))
                                        {:id    id
                                         :point (get snapshot
                                                     (if (= :default aggregate)
                                                       ;; or switch to mean everywhere?
                                                       :mean
                                                       aggregate))
                                         :time  time})))))
                              paths)
          series             (concat raw-series (filter identity current-points))
          [precision series] (p/normalize series)]
      (p/data->series series to precision)))


  Resolutioner
  (resolution [this oldest newest path aggregate]
    (let [plan       (->> (rule/->exec-plan planner path)
                          (sort-by :precision))
          resolution (some #(rule/fit? % oldest newest)
                           plan)]
      {:path       path
       :resolution (or resolution (first plan))
       :aggregate  aggregate})))

(defn make-engine
  [options]
  (map->DefaultEngine options))

(prefer-method print-method clojure.lang.IRecord clojure.lang.IDeref)
