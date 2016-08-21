(defproject io.cyanite/cyanite "0.5.1"
  :description "Alternative storage backend for graphite, backed by cassandra"
  :url "https://github.com/pyr/cyanite"
  :license {:name "MIT License"
            :url "https://github.com/pyr/cyanite/tree/master/LICENSE"}
  :maintainer {:email "pyr@spootnik.org"}
  :profiles {:uberjar {:aot      :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}}
  :main io.cyanite
  :plugins [[lein-ancient "0.6.7"]]
  :dependencies [[org.clojure/clojure           "1.8.0"]
                 [org.clojure/tools.logging     "0.3.1"]
                 [org.clojure/tools.cli         "0.3.3"]
                 [com.stuartsierra/component    "0.3.1"]
                 [spootnik/unilog               "0.7.13"]
                 [spootnik/uncaught             "0.5.3"]
                 [spootnik/globber              "0.4.1"]
                 [spootnik/reporter             "0.1.5"]
                 [spootnik/signal               "0.2.0"]
                 [org.javassist/javassist       "3.20.0-GA"]
                 [instaparse                    "1.4.1"]
                 [cheshire                      "5.5.0"]
                 [metrics-clojure               "2.6.1"]
                 [clj-yaml                      "0.4.0"]
                 [clj-time                      "0.11.0"]
                 [cc.qbits/alia                 "3.1.3"]
                 [org.jctools/jctools-core      "1.2"]
                 [com.boundary/high-scale-lib   "1.0.6"]
                 [net.jpountz.lz4/lz4           "1.3"]
                 [org.xerial.snappy/snappy-java "1.1.2.1"]
                 [io.netty/netty-all            "4.0.34.Final"]])
