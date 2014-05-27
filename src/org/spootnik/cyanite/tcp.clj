(ns org.spootnik.cyanite.tcp
  (:require
   [clojure.core.async :as async :refer [<! >! >!! go chan]])
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
   [io.netty.handler.codec ByteToMessageDecoder]
   [io.netty.channel.nio NioEventLoopGroup]
   [io.netty.channel.socket SocketChannel]
   [io.netty.channel.socket.nio NioServerSocketChannel]))

(def ^:const new-line (byte 0x0A))
(def ^:const empty (byte 0x00))

(defn ^ByteToMessageDecoder metric-decoder []
  (proxy [ByteToMessageDecoder] []
    (decode [^ChannelHandlerContext ctx ^ByteBuf buf ^java.util.List out]
      (let [write-dex (.writerIndex buf)]
        (when (> write-dex 2)
          (let [pre-byte (.getByte buf (dec write-dex))]
               (when (= new-line pre-byte)
                 (.close ctx)
                 (let [msg (String. (.array (.readBytes buf (dec (.readableBytes buf)))))]
                   (when (not-empty msg)
                     (.add out msg))))))))))

(defn ^ChannelHandler build-handler-factory
  "Returns a Netty handler."
  [response-channel]
  (fn []
    (proxy [ChannelInboundHandlerAdapter] []
      (channelRead [^ChannelHandlerContext ctx ^String metrics]
        (doseq [metric (clojure.string/split-lines metrics)]
          (>!! response-channel metric))))))

(defn boot-strap-server
  [handler-factory]
  (doto
      (ServerBootstrap.)
    (.group (NioEventLoopGroup.) (NioEventLoopGroup.))
    (.channel NioServerSocketChannel)
    (.childHandler (proxy [ChannelInitializer] []
                     (initChannel [^SocketChannel chan]
                       (.addLast (.pipeline chan) (into-array ChannelHandler [(metric-decoder)]))
                       (.addLast (.pipeline chan) (into-array ChannelHandler [(handler-factory)])))))
    (.option ChannelOption/SO_BACKLOG (int 128))
    (.childOption ChannelOption/SO_KEEPALIVE true)))

(defn start-tcp-server
  [{:keys [port host response-channel] :as options}]
  (let [handler (build-handler-factory response-channel)
        server (boot-strap-server handler)
        f (-> server (.bind 2003) .sync)]
    (-> f .channel .closeFuture .sync)))
