(defproject io.cyanite/cyanite "0.1.4"
  :description "Alternative storage backend for graphite, backed by cassandra"
  :url "https://github.com/pyr/cyanite"
  :license {:name "MIT License"
            :url "https://github.com/pyr/cyanite/tree/master/LICENSE"}
  :maintainer {:email "pyr@spootnik.org"}
  :main io.cyanite
  :dependencies [[org.clojure/clojure             "1.7.0-alpha2"]
                 [org.clojure/tools.logging       "0.3.1"]
                 [org.clojure/tools.cli           "0.3.1"]
                 [org.spootnik/pickler            "0.1.0"]
                 [clojurewerkz/elastisch          "2.0.0-rc1"]
                 [commons-logging/commons-logging "1.1.3"]
                 [ring/ring-codec                 "1.0.0"]
                 [clj-yaml                        "0.4.0"]
                 [instaparse                      "1.3.4"]
                 [cc.qbits/jet                    "0.5.0-alpha2"]
                 [cc.qbits/alia                   "2.2.0"]
                 [net.jpountz.lz4/lz4             "1.2.0"]
                 [org.xerial.snappy/snappy-java   "1.0.5"]
                 [org.slf4j/slf4j-log4j12         "1.6.4"]
                 [log4j/apache-log4j-extras       "1.0"]
                 [io.netty/netty-all              "4.0.19.Final"]
                 [org.clojure/core.async          "0.1.346.0-17112a-alpha"]
                 [log4j/log4j                     "1.2.16"
                  :exclusions [javax.mail/mail
                               javax.jms/jms
                               com.sun.jdkmk/jmxtools
                               com.sun.jmx/jmxri]]])
