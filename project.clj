(defproject so.grep/cyanite "0.1.0"
  :description "Alternative storage backend for graphite, backed by cassandra"
  :url "https://github.com/pyr/cyanite"
  :license {:name "MIT License"
            :url "https://github.com/pyr/cyanite/tree/master/LICENSE"}
  :maintainer {:email "pyr@spootnik.org"}
  :main so.grep.cyanite
  :profiles {:dev {:eval-in-leiningen true}}
  :dependencies [[org.clojure/clojure       "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/tools.cli     "0.2.4"]
                 [ring/ring-codec           "1.0.0"]
                 [aleph                     "0.3.0"]
                 [clj-yaml                  "0.4.0"]
                 [cc.qbits/alia             "1.8.2"]
                 [org.slf4j/slf4j-log4j12   "1.6.4"]
                 [log4j/apache-log4j-extras "1.0"]
                 [log4j/log4j               "1.2.16"
                  :exclusions [javax.mail/mail
                               javax.jms/jms
                               com.sun.jdkmk/jmxtools
                               com.sun.jmx/jmxri]]])
