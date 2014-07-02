(ns org.spootnik.cyanite.tcp
  (:require
   [clojure.core.async :as async :refer [put!]])
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
   [io.netty.util CharsetUtil]))

(def ^:const new-line (byte 0x0A))

(defn ^ChannelHandler build-handler-factory
  "Returns a Netty handler."
  [response-channel]
  (fn []
    (proxy [ChannelInboundHandlerAdapter] []
      (channelRead [^ChannelHandlerContext ctx ^String metric]
        (put! response-channel metric))
      (exceptionCaught [^ChannelHandlerContext ctx ^Throwable e]
        (if (instance? ReadTimeoutException e)
          (.close ctx)
          (proxy-super ctx e))))))

(defn boot-strap-server
  [handler-factory]
  (doto
      (ServerBootstrap.)
    (.group (NioEventLoopGroup.) (NioEventLoopGroup.))
    (.channel NioServerSocketChannel)
    (.childHandler (proxy [ChannelInitializer] []
                     (initChannel [^SocketChannel chan]
                       (.addLast (.pipeline chan)
                                 (into-array ChannelHandler
                                             [(new LineBasedFrameDecoder 2048)
                                              (new StringDecoder (CharsetUtil/UTF_8))
                                              (new ReadTimeoutHandler 1)
                                              (handler-factory)])))))
    (.option ChannelOption/SO_BACKLOG (int 128))
    (.option ChannelOption/CONNECT_TIMEOUT_MILLIS (int 1000))
    (.childOption ChannelOption/SO_KEEPALIVE true)))

(defn start-tcp-server
  [{:keys [port host response-channel] :as options}]
  (let [handler (build-handler-factory response-channel)
        server (boot-strap-server handler)
        f (-> server (.bind 2003))]
    (-> f .channel .closeFuture)))
