(ns io.cyanite.signals)

(defmacro with-handler
  [signal & body]
  `(sun.misc.Signal/handle
    (sun.misc.Signal. (-> ~signal name .toUpperCase))
    (proxy [sun.misc.SignalHandler] [] (handle [sig#] ~@body))))
