(ns io.cyanite.engine.queue-test
  (:require [io.cyanite.engine.queue :as queue]
            [io.cyanite.test-helper  :refer :all]
            [clojure.test            :refer :all])
  (:import java.util.concurrent.CountDownLatch
           java.util.concurrent.TimeUnit))

(deftest queue-test
  (with-config
    {:engine {:rules {"default" ["5s:1h"]}}}
    {}
    (let [iterations 10000
          reporter   (:reporter *system*)
          q          (queue/make-queue {} :alias reporter)
          last-seen  (volatile! 0)
          latch      (CountDownLatch. iterations)]
      (queue/consume! q (fn [a]
                          ;; Only proceed if values came in correct order
                          (let [prev @last-seen]
                            (when (= a (inc prev)))
                            (vreset! last-seen a)
                            (.countDown latch))))
      (dotimes [i (inc iterations)]
        (queue/add! q i))
      (.await latch 1 TimeUnit/MINUTES)
      (is (= (.getCount latch) 0)))))
