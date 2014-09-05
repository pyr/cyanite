(defproject org.spootnik/cyanite "0.1.0"
  :description "Alternative storage backend for graphite, backed by cassandra"
  :url "https://github.com/pyr/cyanite"
  :license {:name "MIT License"
            :url "https://github.com/pyr/cyanite/tree/master/LICENSE"}
  :maintainer {:email "pyr@spootnik.org"}
  :main org.spootnik.cyanite
  :dependencies [[org.clojure/clojure                       "1.6.0"]
                 [org.clojure/tools.logging                 "0.2.6"]
                 [org.clojure/tools.cli                     "0.2.4"]
                 [org.spootnik/pickler                      "0.1.0"]
                 [clojurewerkz/elastisch                "2.0.0-rc1"]
                 [commons-logging/commons-logging           "1.1.3"]
                 [ring/ring-codec                           "1.0.0"]
                 [aleph                                     "0.3.0"]
                 [clj-yaml                                  "0.4.0"]
                 [cc.qbits/alia                             "2.0.0-rc1"]
                 [net.jpountz.lz4/lz4                       "1.2.0"]
                 [org.xerial.snappy/snappy-java             "1.0.5"]
                 [org.slf4j/slf4j-log4j12                   "1.6.4"]
                 [log4j/apache-log4j-extras                 "1.0"]
                 [io.netty/netty-all "4.0.19.Final"]
                 [org.clojure/core.async "0.1.278.0-76b25b-alpha"]
                 [http-kit "2.1.16"]
                 [log4j/log4j                               "1.2.16"
                  :exclusions [javax.mail/mail
                               javax.jms/jms
                               com.sun.jdkmk/jmxtools
                               com.sun.jmx/jmxri]]])
