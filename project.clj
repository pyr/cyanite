(defproject org.spootnik/cyanite "0.2.2"
  :description "Alternative storage backend for graphite, backed by cassandra"
  :url "https://github.com/pyr/cyanite"
  :license {:name "MIT License"
            :url "https://github.com/pyr/cyanite/tree/master/LICENSE"}
  :maintainer {:email "pyr@spootnik.org"}
  :main org.spootnik.cyanite
  :dependencies [[org.clojure/clojure                       "1.6.0"]
                 [org.clojure/tools.logging                 "0.3.0"]
                 [org.clojure/tools.cli                     "0.3.1"]
                 [org.spootnik/pickler                      "0.1.0"]
                 [clojurewerkz/elastisch                    "2.1.0-beta6"]
                 [commons-logging/commons-logging           "1.2"]
                 [ring/ring-codec                           "1.0.0"]
                 [aleph                                     "0.3.3"]
                 [ring/ring-json                            "0.3.1"]
                 [net.cgrand/moustache                      "1.2.0-alpha2"]
                 [clj-yaml                                  "0.4.0"]
                 [cc.qbits/alia                             "2.1.2"]
                 [net.jpountz.lz4/lz4                       "1.2.0"]
                 [org.xerial.snappy/snappy-java             "1.1.1.3"]
                 [org.slf4j/slf4j-log4j12                   "1.7.7"]
                 [log4j/apache-log4j-extras                 "1.2.17"]
                 [io.netty/netty-all                        "5.0.0.Alpha1"]
                 [org.clojure/core.async                    "0.1.278.0-76b25b-alpha"]
                 [http-kit                                  "2.1.19"]
                 [log4j/log4j                               "1.2.17"
                  :exclusions [javax.mail/mail
                               javax.jms/jms
                               com.sun.jdkmk/jmxtools
                               com.sun.jmx/jmxri]]])
