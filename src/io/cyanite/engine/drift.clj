(ns io.cyanite.engine.drift
  "Drift handling component. Relies on a clock implementation
   which can yield an epoch with a second based resolution."
  (:require [com.stuartsierra.component :as component]))

(defprotocol Drift
  (drift! [this ts]     "Take new drift into account for this timestamp")
  (skewed-epoch! [this] "Yield an approximate epoch, accounting for drift"))

(defprotocol Clock
  (epoch! [this]        "Give us an epoch"))

;; System Clock is a basic wall clock
;; No configuration possible here.
(defrecord SystemClock []
  component/Lifecycle
  (start [this] this)
  (stop [this] this)
  Clock
  (epoch! [this]
    (quot (System/currentTimeMillis) 1000)))

;; Hold the state of our drift in an agent
;; This way we ensure that we have fast
;; execution of drift computation from
;; the caller site. this should eventually
;; rely on send-via to ensure we have our
;; own pool of threads to handle the max calls
(defrecord AgentDrift [slot clock]
  component/Lifecycle
  (start [this]
    (assoc this :slot (agent 0)))
  (stop [this]
    (assoc this :slot nil))
  Drift
  (drift! [this ts]
    (let [drift (- ts (epoch! clock))]
      (when (pos? drift)
        (send-off slot max drift))))
  (skewed-epoch! [this]
    (- (epoch! clock) @slot))
  clojure.lang.IDeref
  (deref [this]
    @slot))

(prefer-method print-method clojure.lang.IRecord clojure.lang.IDeref)
