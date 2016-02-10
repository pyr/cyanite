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

;; Hold the state of our drift in a volatile,
;; we don't care much about the atomicity guarantees
;; of our drift, we want a reasonable approximation
;; of the clock drift.
(defrecord VolatileDrift [slot clock]
  component/Lifecycle
  (start [this]
    (assoc this :slot (volatile! 0)))
  (stop [this]
    (assoc this :slot nil))
  Drift
  (drift! [this ts]
    (let [drift (- ts (epoch! clock))]
      (when (pos? drift)
        (vswap! slot max drift))))
  (skewed-epoch! [this]
    (- (epoch! clock) @slot))
  clojure.lang.IDeref
  (deref [this]
    @slot))

;; A no-op implementation of the drift,
;; might be used in test purposes or
;; to avoid the drift calculation ovrehead
;; in production systems.
(defrecord NoOpDrift [clock]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)
  Drift
  (drift! [this ts] nil)
  (skewed-epoch! [this]
    (epoch! clock))
  clojure.lang.IDeref
  (deref [this]
    0))

(defmulti build-drift (comp (fnil keyword "agent") :type))

(defmethod build-drift :no-op
  [options]
  (map->NoOpDrift options))

(defmethod build-drift :volatile
  [options]
  (map->VolatileDrift options))

(defmethod build-drift :agent
  [options]
  (map->AgentDrift options))

(prefer-method print-method clojure.lang.IRecord clojure.lang.IDeref)
