(ns io.cyanite.transport.tcp
  (:require [clojure.core.async    :refer [put!]]
            [clojure.tools.logging :refer [debug]])
  (:import io.netty.bootstrap.ServerBootstrap
           io.netty.channel.ChannelInitializer
           io.netty.channel.ChannelOption
           io.netty.channel.ChannelInboundHandlerAdapter
           io.netty.channel.ChannelHandlerContext
           io.netty.channel.ChannelHandler
           io.netty.handler.codec.LineBasedFrameDecoder
           io.netty.handler.codec.LengthFieldBasedFrameDecoder
           io.netty.handler.codec.string.StringDecoder
           io.netty.handler.timeout.ReadTimeoutHandler
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
  [& handlers]
  (into-array ChannelHandler handlers))

(defn line-pipeline
  [ch ^Integer read-timeout]
  (make-pipeline (LineBasedFrameDecoder. 2048)
                 (StringDecoder. (CharsetUtil/UTF_8))
                 (ReadTimeoutHandler. read-timeout)
                 (with-input input
                   (when (seq input)
                     (put! ch input)))))

(defn pickle-pipeline
  [ch ^Integer read-timeout]
  (make-pipeline (LengthFieldBasedFrameDecoder. Integer/MAX_VALUE 0 4 0 4)
                 (ReadTimeoutHandler. read-timeout)
                 (with-input input
                   (put! ch input))))

(defn server-bootstrap
  [pipeline]
  (doto (ServerBootstrap.)
    (.group        (NioEventLoopGroup.))
    (.channel      NioServerSocketChannel)
    (.childHandler (proxy [ChannelInitializer] []
                     (initChannel [^SocketChannel s-chan]
                       (.addLast (.pipeline s-chan) pipeline))))
    (.option       ChannelOption/SO_BACKLOG (int 128))
    (.option       ChannelOption/CONNECT_TIMEOUT_MILLIS (int 1000))
    (.childOption  ChannelOption/SO_KEEPALIVE true)))

(defn start-tcp-server
  [{:keys [port host timeout type]} ch]

  (let [timeout  (or timeout 30)
        pipeline (if (= (keyword type) :pickle)
                  (pickle-pipeline ch timeout)
                  (line-pipeline ch timeout))
        server   ^ServerBootstrap (server-bootstrap pipeline)]
    (debug "tcp server ready, will bind to " host port)
    (try
      (-> server (.bind ^String host (int port)) .channel .closeFuture)
      (catch Exception e
        (debug e "server did not bootstrap")))))
