(defproject io.cyanite/cyanite "0.5.1"
  :description "Alternative storage backend for graphite, backed by cassandra"
  :url "https://github.com/pyr/cyanite"
  :license {:name "MIT License"
            :url "https://github.com/pyr/cyanite/tree/master/LICENSE"}
  :maintainer {:email "pyr@spootnik.org"}
  :aot :all
  :main io.cyanite
  :plugins [[lein-ancient "0.6.7"]]
  :dependencies [[org.clojure/clojure           "1.8.0-alpha2"]
                 [org.clojure/tools.logging     "0.3.1"]
                 [org.clojure/tools.cli         "0.3.2"]
                 [spootnik/pickler              "0.1.6"]
                 [spootnik/unilog               "0.7.8"]
                 [spootnik/uncaught             "0.5.2"]
                 [instaparse                    "1.4.1"]
                 [ring/ring-codec               "1.0.0"]
                 [clj-yaml                      "0.4.0"]
                 [cc.qbits/jet                  "0.6.6"]
                 [cc.qbits/alia                 "2.7.2"]
                 [com.boundary/high-scale-lib   "1.0.6"]
                 [net.jpountz.lz4/lz4           "1.3"]
                 [org.xerial.snappy/snappy-java "1.1.1.7"]
                 [com.stuartsierra/component    "0.2.3"]
                 [io.netty/netty-all            "4.0.30.Final"]
                 [org.clojure/core.async        "0.1.346.0-17112a-alpha"]])
