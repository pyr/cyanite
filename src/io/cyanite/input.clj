(ns io.cyanite.input
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.engine          :as engine]
            [io.cyanite.input.carbon    :as carbon]
            [clojure.tools.logging      :refer [info]]))

(defmulti build-input (comp (fnil keyword "carbon") :type))

(defmethod build-input :carbon
  [options]
  (component/using
   (carbon/map->CarbonTCPInput (assoc options
                                      :port (or (:port options) 2003)
                                      :pipeline carbon/pipeline))
   [:engine]))
