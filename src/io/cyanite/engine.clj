(ns io.cyanite.engine
  (:require [clojure.core.async    :as a]
            [io.cyanite.resolution :as r]
            [io.cyanite.store      :refer [insert!]]
            [io.cyanite.index      :refer [register!]]
            [clojure.tools.logging :refer [debug info warn error]]))

(defprotocol Service
  (start! [this]))

(defrecord FetchSpec [agg from to])

(def default-tenant "no tenants yet" "")

(defn formatter
  [resolutions {:keys [path point time] :as input}]
  (for [resolution resolutions]
    (try
      {:path   path
       :key    resolution
       :time   (r/rollup resolution time)
       :point  point}
      (catch Exception e
        (info e "failed metric input" (pr-str input))))))

(defn denormalizer
  [resolutions]
  (fn [metric]
    (try
      (formatter resolutions metric)
      (catch Exception e
        (warn e "failed denormalization" (pr-str metric))))))

(defn engine
  [{:keys [resolutions] :as config}]
  (let [transports (atom nil)
        denorm     (partial a/mapcat< (denormalizer resolutions))]
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
                (try
                  (insert! (:store config) default-tenant metric)
                  (catch Exception e
                    (debug e "could not insert metric")))
                (try
                  (register! (:index config) default-tenant path)
                  (catch Exception e
                    (debug e "could not index metric name")))))
            (recur)))))))
