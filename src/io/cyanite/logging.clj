(ns io.cyanite.logging
  "Very simple wrapper for log4j"
  (:import (org.apache.log4j           Logger
                                       BasicConfigurator
                                       EnhancedPatternLayout
                                       Level
                                       ConsoleAppender
                                       FileAppender
                                       SimpleLayout)
           (org.apache.log4j.spi       RootLogger)
           (org.apache.log4j.rolling   TimeBasedRollingPolicy
                                       RollingFileAppender)
           (org.apache.commons.logging LogFactory))
  (:require [clojure.tools.logging :as log]))

(def
  ^{:doc "known log levels"}
  levels
  {"debug" Level/DEBUG
   "info"  Level/INFO
   "warn"  Level/WARN
   "error" Level/ERROR
   "all"   Level/ALL
   "fatal" Level/FATAL
   "trace" Level/TRACE
   "off"   Level/OFF})

(defn start-logging
  "Initialize log4j. Stolen from riemann"
  [{ :keys [external console files pattern level overrides]}]
  ; Reset loggers
  (let [layout      (EnhancedPatternLayout. pattern)
        root-logger (Logger/getRootLogger)]

    (when (not external)
      (.removeAllAppenders root-logger)

      (when console
        (.addAppender root-logger (ConsoleAppender. layout)))

      (doseq [file files]
        (let [rolling-policy (doto (TimeBasedRollingPolicy.)
                               (.setActiveFileName file)
                               (.setFileNamePattern
                                (str file ".%d{yyyy-MM-dd}.gz"))
                               (.activateOptions))
              log-appender   (doto (RollingFileAppender.)
                               (.setRollingPolicy rolling-policy)
                               (.setLayout layout)
                               (.activateOptions))]
          (.addAppender root-logger log-appender)))

      (.setLevel root-logger (get levels level Level/INFO))

      (doseq [[logger level] overrides
              :let [logger (Logger/getLogger (name logger))
                    level  (get levels level Level/DEBUG)]]
        (.setLevel logger level)))))
