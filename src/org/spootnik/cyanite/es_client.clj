(ns org.spootnik.cyanite.es-client
  "Hacks of elastich to use async http-kit"
  (:require [clojurewerkz.elastisch.rest.document :as esrd]
            [clojurewerkz.elastisch.rest.bulk :as esrb]
            [clojurewerkz.elastisch.rest :as rest]
            [clojurewerkz.elastisch.arguments :as ar]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [clojure.tools.logging :refer [error info debug]])
  (:import clojurewerkz.elastisch.rest.Connection))

(defn multi-get
  [^Connection conn index mapping-type query func]
  (http/post
   (rest/index-mget-url conn index mapping-type)
   {:body (json/encode {:docs query})}
   #(if (= 200 (:status %))
      (let [bod (json/decode (:body %) true)]
        (func (filter :found (:docs bod))))
      (error "ES responded with non-200: " %))))

(comment "Note: i've ditched the optional args to ES, refer to orignal elastich code for how they should return")

(defn bulk-with-url
  [url operations func]
  (let [bulk-json (map json/encode operations)
        bulk-json (-> bulk-json
                      (interleave (repeat "\n"))
                      (str/join))]
    (http/post url
               {:body bulk-json}
               #(let [status (:status %)]
                  (when (not= 200 status)
                    (func %))))))

(defn remove-ids-from-docs
  [op-or-doc]
  (if (:tenant op-or-doc)
    (dissoc op-or-doc :_id)
    op-or-doc))

(defn multi-update
  [^Connection conn index mapping-type docs func]
  (bulk-with-url (rest/bulk-url conn index mapping-type)
                 (map
                  remove-ids-from-docs
                  (esrb/bulk-index (map #(assoc % :_id (:path %)
                                                :_index index
                                                :_type mapping-type) docs)))
                 func))
