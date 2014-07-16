(ns org.spootnik.cyanite.tcp
  (:require
   [clojure.core.async :as async :refer [put! >!!]])
  (:import
   [java.net InetSocketAddress]
   [java.util.concurrent Executors]
   [io.netty.buffer ByteBuf]
   [io.netty.bootstrap ServerBootstrap]
   [io.netty.channel
    ChannelFuture ChannelInitializer
    ChannelOption EventLoopGroup
    ChannelInboundHandlerAdapter ChannelHandlerContext
    ChannelHandler]
   [io.netty.handler.codec
    ByteToMessageDecoder
    LineBasedFrameDecoder]
   [io.netty.handler.codec.string StringDecoder]
   [io.netty.handler.timeout
    ReadTimeoutHandler
    ReadTimeoutException]
   [io.netty.channel.nio NioEventLoopGroup]
   [io.netty.channel.socket SocketChannel]
   [io.netty.channel.socket.nio NioServerSocketChannel]
   [io.netty.util CharsetUtil]
   [java.util.concurrent Executors]))

(def ^:const new-line (byte 0x0A))

(defn ^ChannelHandler build-handler-factory
  "Returns a Netty handler."
  [response-channel]
  (fn []
    (proxy [ChannelInboundHandlerAdapter] []
      (channelRead [^ChannelHandlerContext ctx ^String metric]
        (when (not-empty metric)
          (>!! response-channel metric)))
      (exceptionCaught [^ChannelHandlerContext ctx ^Throwable e]
        (if (instance? ReadTimeoutException e)
          (.close ctx)
          (proxy-super ctx e))))))

(defn boot-strap-server
  [handler-factory ^Integer readtimeout]
  (let [sd (new StringDecoder (CharsetUtil/UTF_8))]
      (doto
          (ServerBootstrap.)
        (.group (NioEventLoopGroup.))
        (.channel NioServerSocketChannel)
        (.childHandler (proxy [ChannelInitializer] []
                         (initChannel [^SocketChannel chan]
                           (.addLast (.pipeline chan)
                                     (into-array ChannelHandler
                                                 [(new LineBasedFrameDecoder 2048)
                                                  sd
                                                  (new ReadTimeoutHandler readtimeout)
                                                  (handler-factory)])))))
        (.option ChannelOption/SO_BACKLOG (int 128))
        (.option ChannelOption/CONNECT_TIMEOUT_MILLIS (int 1000))
        (.childOption ChannelOption/SO_KEEPALIVE true))))

(defn start-tcp-server
  [{:keys [port host readtimeout response-channel] :as options}]
  (let [handler (build-handler-factory response-channel)
        server (boot-strap-server handler readtimeout)
        f (-> server (.bind port))]
    (-> f .channel .closeFuture)))
