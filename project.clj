(defproject io.cyanite/cyanite "0.5.1"
  :description "Alternative storage backend for graphite, backed by cassandra"
  :url "https://github.com/pyr/cyanite"
  :license {:name "MIT License"
            :url "https://github.com/pyr/cyanite/tree/master/LICENSE"}
  :maintainer {:email "pyr@spootnik.org"}
  :profiles {:uberjar {:aot      :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}
             :dev {:global-vars {*warn-on-reflection* true}}}
  :main io.cyanite
  :plugins [[lein-ancient "0.6.7"]]
  :dependencies [[org.clojure/clojure                    "1.9.0-alpha14"]
                 [org.clojure/tools.logging              "0.3.1"]
                 [org.clojure/tools.cli                  "0.3.3"]
                 [com.stuartsierra/component             "0.3.2"]
                 [spootnik/unilog                        "0.7.19"]
                 [spootnik/uncaught                      "0.5.3"]
                 [spootnik/globber                       "0.4.1"]
                 [spootnik/reporter                      "0.1.17"]
                 [spootnik/signal                        "0.2.1"]
                 [spootnik/net                           "0.3.3-beta9"]
                 [org.javassist/javassist                "3.21.0-GA"]
                 [instaparse                             "1.4.5"]
                 [cheshire                               "5.7.0"]
                 [clj-yaml                               "0.4.0"]
                 [clj-time                               "0.13.0"]
                 [com.github.ben-manes.caffeine/caffeine "2.4.0"]
                 [cc.qbits/alia                          "4.0.0-beta7"]
                 [org.jctools/jctools-core               "2.0.1"]
                 [com.boundary/high-scale-lib            "1.0.6"]
                 [net.jpountz.lz4/lz4                    "1.3.0"]
                 [org.xerial.snappy/snappy-java          "1.1.2.6"]])
