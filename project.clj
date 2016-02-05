(defproject io.cyanite/cyanite "0.5.1"
  :description "Alternative storage backend for graphite, backed by cassandra"
  :url "https://github.com/pyr/cyanite"
  :license {:name "MIT License"
            :url "https://github.com/pyr/cyanite/tree/master/LICENSE"}
  :maintainer {:email "pyr@spootnik.org"}
  :aot :all
  :main io.cyanite
  :plugins [[lein-ancient "0.6.7"]]
  :dependencies [[org.clojure/clojure           "1.8.0"]
                 [org.clojure/tools.logging     "0.3.1"]
                 [org.clojure/tools.cli         "0.3.3"]
                 [com.stuartsierra/component    "0.3.1"]
                 [spootnik/pickler              "0.1.6"]
                 [spootnik/unilog               "0.7.13"]
                 [spootnik/uncaught             "0.5.3"]
                 [spootnik/globber              "0.4.1"]
                 [instaparse                    "1.4.1"]
                 [cheshire                      "5.5.0"]
                 [metrics-clojure               "2.6.1"]
                 [clj-yaml                      "0.4.0"]
                 [clj-time                      "0.11.0"]
                 [cc.qbits/alia                 "2.10.0"]
                 [com.lmax/disruptor            "3.3.4"]
                 [com.boundary/high-scale-lib   "1.0.6"]
                 [net.jpountz.lz4/lz4           "1.3"]
                 [org.xerial.snappy/snappy-java "1.1.2"]
                 [io.netty/netty-all            "4.0.32.Final"]])
