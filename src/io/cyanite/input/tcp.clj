(ns io.cyanite.input.tcp
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging      :refer [warn debug]])
  (:import io.netty.bootstrap.ServerBootstrap
           io.netty.channel.ChannelInitializer
           io.netty.channel.ChannelOption
           io.netty.channel.ChannelInboundHandlerAdapter
           io.netty.channel.ChannelHandlerContext
           io.netty.channel.ChannelHandler
           io.netty.handler.timeout.ReadTimeoutException
           io.netty.channel.nio.NioEventLoopGroup
           io.netty.channel.socket.SocketChannel
           io.netty.channel.socket.nio.NioServerSocketChannel
           io.netty.util.CharsetUtil))

(defmacro ^ChannelHandler with-input
  [input & body]
  `(proxy [ChannelInboundHandlerAdapter] []
     (channelRead [^ChannelHandlerContext ctx# input#]
       (let [~input input#]
         ~@body))
     (exceptionCaught [^ChannelHandlerContext ctx# ^Throwable e#]
       (if (instance? ReadTimeoutException e#)
         (.close ctx#)
         (proxy-super exceptionCaught ctx# e#)))
     (isSharable []
       true)))

(defn make-pipeline
  [handlers]
  (into-array ChannelHandler (for [h handlers] (if (fn? h) (h) h))))

(defn server-bootstrap
  [pipeline]
  (doto (ServerBootstrap.)
    (.group        (NioEventLoopGroup.))
    (.channel      NioServerSocketChannel)
    (.childHandler (proxy [ChannelInitializer] []
                     (initChannel [^SocketChannel s-chan]
                       (.addLast (.pipeline s-chan)
                                 (make-pipeline pipeline)))))
    (.option       ChannelOption/SO_BACKLOG (int 128))
    (.option       ChannelOption/CONNECT_TIMEOUT_MILLIS (int 1000))
    (.childOption  ChannelOption/SO_KEEPALIVE true)))

(defrecord TCPInput [host port timeout pipeline server engine]
  component/Lifecycle
  (start [this]
    (let [timeout  (or timeout 30)
          host     (or host "127.0.0.1")
          port     (or port 2002)
          server   ^ServerBootstrap (server-bootstrap
                                     (pipeline timeout engine))]
      (try
        (assoc this :server (-> server
                                (.bind ^String host (int port))
                                .channel))
        (catch Exception e
          (warn e "could not start server")))))
  (stop [this]
    (-> server
        .close
        .syncUninterruptibly)
    (assoc this :server nil)))
