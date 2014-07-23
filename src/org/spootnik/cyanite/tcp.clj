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

(defn ^ChannelHandler channel-putter
  "Returns a Netty handler."
  [response-channel]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelRead [^ChannelHandlerContext ctx ^String metric]
      (when (not-empty metric)
        (>!! response-channel metric)))
    (exceptionCaught [^ChannelHandlerContext ctx ^Throwable e]
      (if (instance? ReadTimeoutException e)
        (.close ctx)
        (proxy-super exceptionCaught ctx e)))
    (isSharable []
      true)))

(defn boot-strap-server
  [putter ^Integer readtimeout]
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
                                                  putter])))))
        (.option ChannelOption/SO_BACKLOG (int 128))
        (.option ChannelOption/CONNECT_TIMEOUT_MILLIS (int 1000))
        (.childOption ChannelOption/SO_KEEPALIVE true))))

(defn start-tcp-server
  [{:keys [port host readtimeout response-channel] :as options}]
  (let [putter (channel-putter response-channel)
        server (boot-strap-server putter readtimeout)
        f (-> server (.bind port))]
    (-> f .channel .closeFuture)))
