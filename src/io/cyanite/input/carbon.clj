(ns io.cyanite.input.carbon
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.engine          :as engine]
            [net.tcp                    :as tcp]
            [net.ty.pipeline            :as pipeline]
            [clojure.string             :refer [split]]
            [clojure.tools.logging      :refer [info warn]]))

(defn parse-line
  [^String line]
  (let [[path metric time & garbage] (split line #"\s+")]
    (cond
      garbage
      (throw (ex-info "invalid carbon line: too many fields" {:line line})
             )
      (not (and (seq path) (seq metric) (seq time)))
      (throw (ex-info "invalid carbon line: missing fields" {:line line}))

      (re-find #"(?i)nan" metric)
      (throw (ex-info (str "invalid carbon line: NaN metric for path:" path)
                      {:line line
                       :path path})))
    (let [metric (try (Double. metric)
                      (catch NumberFormatException e
                        (throw (ex-info "invalid metric" {:metric metric}))))
          time   (try (long (Double. time))
                      (catch NumberFormatException e
                        (throw (ex-info "invalid time" {:time time}))))]
      {:path path :metric metric :time time})))

(defn pipeline
  [engine read-timeout]
  (pipeline/channel-initializer
    [(pipeline/line-based-frame-decoder 2048)
     pipeline/string-decoder
     (pipeline/read-timeout-handler read-timeout)
     (pipeline/with-input [ctx msg]
       (when (seq msg)
         (engine/enqueue! engine (parse-line msg))))]))

(defrecord CarbonTCPInput [host port timeout server engine]
  component/Lifecycle
  (start [this]
    (let [timeout  (or timeout 30)
          host     (or host "127.0.0.1")
          port     (or port 2002)
          server   (tcp/server {:handler (pipeline engine timeout)} host port)]
      (try
        (assoc this :server server)
        (catch Exception e
          (warn e "could not start server")))))
  (stop [this]
    (when server
      (server))
    (assoc this :server nil)))
