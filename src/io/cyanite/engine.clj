(ns io.cyanite.engine
  (:require [clojure.core.async    :as a]
            [io.cyanite.precision  :as p]
            [io.cyanite.store      :refer [insert!]]
            [io.cyanite.index      :refer [register!]]
            [clojure.tools.logging :refer [debug info warn error]]))

(defprotocol Service
  (start! [this]))

(defrecord FetchSpec [agg from to])

(def default-tenant "no tenants yet" "")

(defn formatter
  [precisions {:keys [path metric time] :as input}]
  (for [precision precisions]
    (try
      {:path   path
       :key    precision
       :time   (p/rollup precision time)
       :point  metric}
      (catch Exception e
        (info e "failed metric input" (pr-str input))))))

(defn denormalizer
  [precisions]
  (fn [metric]
    (try
      (formatter precisions metric)
      (catch Exception e
        (warn e "failed denormalization" (pr-str metric))))))

(defn engine
  [{:keys [precisions] :as config}]
  (let [transports (atom nil)
        denorm     (partial a/mapcat< (denormalizer precisions))]
    (reify
      clojure.lang.ITransientCollection
      (conj [this transport]
        (swap! transports conj transport)
        this)
      Service
      (start! [this]
        (doseq [t @transports]
          (future
            (start! t)))
        (when-let [chans (seq (map (comp denorm :channel) @transports))]
          (a/go-loop []
            (let [[{:keys [path point time] :as metric} _] (a/alts! chans)]
              (debug "new metric: " (pr-str metric))
              (when (and path point time)
                (insert! (:store config) default-tenant metric)
                (register! (:index config) default-tenant path)))
            (recur)))))))
