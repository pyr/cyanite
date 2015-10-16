(ns io.cyanite.http
  "Small wrapper around netty for HTTP servers"
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging      :refer [error]])
  (:import io.netty.channel.ChannelHandlerContext
           io.netty.channel.ChannelHandlerAdapter
           io.netty.channel.ChannelInboundHandlerAdapter
           io.netty.channel.ChannelOutboundHandlerAdapter
           io.netty.channel.ChannelHandler
           io.netty.channel.ChannelOption
           io.netty.channel.ChannelInitializer
           io.netty.channel.ChannelFutureListener
           io.netty.channel.nio.NioEventLoopGroup
           io.netty.channel.socket.nio.NioServerSocketChannel
           io.netty.channel.epoll.Epoll
           io.netty.channel.epoll.EpollServerSocketChannel
           io.netty.channel.epoll.EpollEventLoopGroup
           io.netty.handler.logging.LoggingHandler
           io.netty.handler.logging.LogLevel
           io.netty.handler.codec.http.FullHttpRequest
           io.netty.handler.codec.http.HttpServerCodec
           io.netty.handler.codec.http.HttpMethod
           io.netty.handler.codec.http.HttpHeaders
           io.netty.handler.codec.http.HttpResponseStatus
           io.netty.handler.codec.http.DefaultFullHttpResponse
           io.netty.handler.codec.http.HttpVersion
           io.netty.handler.codec.http.HttpObjectAggregator
           io.netty.bootstrap.ServerBootstrap
           io.netty.buffer.Unpooled
           java.nio.charset.Charset))

(defn epoll?
  "Find out if epoll is available on the underlying platform."
  []
  (Epoll/isAvailable))

(defn bb->string
  "Convert a ByteBuf to a UTF-8 String."
  [bb]
  (.toString bb (Charset/forName "UTF-8")))

(def method->data
  "Yield a keyword representing an HTTP method."
  {HttpMethod/CONNECT :connect
   HttpMethod/DELETE  :delete
   HttpMethod/GET     :get
   HttpMethod/HEAD    :head
   HttpMethod/OPTIONS :options
   HttpMethod/PATCH   :patch
   HttpMethod/POST    :post
   HttpMethod/PUT     :put
   HttpMethod/TRACE   :trace})

(defn headers
  "Get a map out of netty headers."
  [^HttpHeaders headers]
  (into
   {}
   (for [[^String k ^String v] (-> headers .entries seq)]
     [(-> k .toLowerCase keyword) v])))

(defn data->response
  "Create a netty full http response from a map."
  [{:keys [status body headers]} version]
  (let [resp (DefaultFullHttpResponse.
               version
               (HttpResponseStatus/valueOf (int status))
               (Unpooled/wrappedBuffer (.getBytes body)))
        hmap (.headers resp)]
    (doseq [[k v] headers]
      (.set hmap (name k) v))
    resp))

(defn request-handler
  "Capture context and msg and yield a closure
   which generates a response.

   The closure may be called at once or submitted to a pool."
  [f ^ChannelHandlerContext ctx ^FullHttpRequest msg]
  (fn []
    (let [req  {:uri            (.getUri msg)
                :request-method (method->data (.getMethod msg))
                :version        (-> msg .getProtocolVersion .text)
                :headers        (headers (.headers msg))
                :body           (bb->string (.content msg))}
          resp (data->response (f req) (.getProtocolVersion msg))]
      (-> (.writeAndFlush ctx resp)
          (.addListener ChannelFutureListener/CLOSE)))))

(defn netty-handler
  "Simple netty-handler, everything may happen in
   channel read, since we're expecting a full http request."
  [f]
  (proxy [ChannelInboundHandlerAdapter] []
    (exceptionCaught [^ChannelHandlerContext ctx e]
      (error e "http server exception caught"))
    (channelRead [^ChannelHandlerContext ctx ^FullHttpRequest msg]
      (let [callback (request-handler f ctx msg)]
        (callback)))))

(defn initializer
  "Our channel initializer."
  [handler]
  (proxy [ChannelInitializer] []
    (initChannel [channel]
      (let [pipeline (.pipeline channel)]
        (.addLast pipeline "codec"      (HttpServerCodec.))
        (.addLast pipeline "aggregator" (HttpObjectAggregator. 1048576))
        (.addLast pipeline "handler"    (netty-handler handler))))))

(defn run-server
  "Prepare a bootstrap channel and start it."
  ([options handler]
   (run-server (assoc options :ring-handler handler)))
  ([options]
   (let [thread-count (or (:loop-thread-count options) 1)
         boss-group   (if (and (epoll?) (not (:disable-epoll options)))
                        (EpollEventLoopGroup. thread-count)
                        (NioEventLoopGroup.   thread-count))
         so-backlog   (int (or (:so-backlog options) 1024))]
     (try
       (let [bootstrap (doto (ServerBootstrap.)
                         (.option ChannelOption/SO_BACKLOG so-backlog)
                         (.group boss-group)
                         (.channel (if (epoll?)
                                     EpollServerSocketChannel
                                     NioServerSocketChannel))
                         (.handler (LoggingHandler. LogLevel/WARN))
                         (.childHandler (initializer (:ring-handler options))))
             channel   (-> bootstrap
                           (.bind ^String (or (:host options) "127.0.0.1")
                                  (int (or (:port options) 8080)))
                           (.sync)
                           (.channel))]
         (future
           (-> channel .closeFuture .sync))
         (fn []
           (.close channel)
           (.shutdownGracefully boss-group)))))))
