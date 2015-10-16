(ns io.cyanite.config
  "Yaml config parser, with a poor man's dependency injector"
  (:require [com.stuartsierra.component :as component]
            [clj-yaml.core              :refer [parse-string]]
            [clojure.tools.logging      :refer [error info debug]]))

(def default-logging
  "Default logging configuration. Refer to https://github.com/pyr/unilog
   for details."
  {:pattern "%p [%d] %t - %c - %m%n"
   :external false
   :console true
   :files  []
   :level  "info"})

(defn load-path
  "Try to find a pathname, on the command line, in
   system properties or the environment and load it."
  [path]
  (-> (or path
          (System/getProperty "cyanite.configuration")
          (System/getenv "CYANITE_CONFIGURATION")
          "/etc/cyanite.yaml")
      slurp
      parse-string))
