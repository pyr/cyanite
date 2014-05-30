(ns org.spootnik.cyanite.es-client
  (:require [clojurewerkz.elastisch.rest.document :as esrd]
            [clojurewerkz.elastisch.rest :as rest]
            [org.httpkit.client :as http]
            [cheshire.core :as json])
  (:import clojurewerkz.elastisch.rest.Connection))


(defn multi-get
  [^Connection conn index mapping-type query func]
  (http/post
   (rest/index-mget-url conn index mapping-type)
   {:body (json/encode {:docs query})}
   #(let [bod (json/decode (:body %) true)]
      (func (filter :found (:docs bod))))))
