/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.shufflehandler;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import javax.crypto.SecretKey;

import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.common.security.SecureShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.NetUtil;
import io.netty.util.concurrent.GlobalEventExecutor;

public class ShuffleHandler implements AttemptRegistrationListener {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleHandler.class);

  public static final String SHUFFLE_HANDLER_LOCAL_DIRS = "llap.shuffle.handler.local-dirs";

  public static final String SHUFFLE_MANAGE_OS_CACHE = "llap.shuffle.manage.os.cache";
  public static final boolean DEFAULT_SHUFFLE_MANAGE_OS_CACHE = true;

  public static final String SHUFFLE_OS_CACHE_ALWAYS_EVICT =
      "llap.shuffle.os.cache.always.evict";
  public static final boolean DEFAULT_SHUFFLE_OS_CACHE_ALWAYS_EVICT = false;

  public static final String SHUFFLE_READAHEAD_BYTES = "llap.shuffle.readahead.bytes";
  public static final int DEFAULT_SHUFFLE_READAHEAD_BYTES = 4 * 1024 * 1024;

  public static final String SHUFFLE_DIR_WATCHER_ENABLED = "llap.shuffle.dir-watcher.enabled";
  public static final boolean SHUFFLE_DIR_WATCHER_ENABLED_DEFAULT = false;
  
  // pattern to identify errors related to the client closing the socket early
  // idea borrowed from Netty SslHandler
  private static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
      "^.*(?:connection.*reset|connection.*closed|broken.*pipe).*$",
      Pattern.CASE_INSENSITIVE);

  private int port;
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup workerGroup;
  private final ChannelGroup accepted = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final int sslFileBufferSize;

  // pipeline items
  private Shuffle SHUFFLE;
  private SSLFactory sslFactory;

  private final Configuration conf;
  private final String[] localDirs;
  private final DirWatcher dirWatcher;

  /**
   * Should the shuffle use posix_fadvise calls to manage the OS cache during
   * sendfile
   */
  private final boolean manageOsCache;
  private final boolean shouldAlwaysEvictOsCache;
  private final int readaheadLength;
  private final int maxShuffleConnections;
  private final int shuffleBufferSize;
  private final boolean shuffleTransferToAllowed;
  private final ReadaheadPool readaheadPool = ReadaheadPool.getInstance();

  /* List of registered applications */
  private final ConcurrentMap<String, Integer> registeredApps = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Integer> registeredDirectories = new ConcurrentHashMap<>();
  /* Maps application identifiers (jobIds) to the associated user for the app */
  private final ConcurrentMap<String,String> userRsrc;
  private JobTokenSecretManager secretManager;

  public static final String SHUFFLE_PORT_CONFIG_KEY = "llap.shuffle.port";
  public static final int DEFAULT_SHUFFLE_PORT = 15551;

  public static final String SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED =
      "llap.shuffle.connection-keep-alive.enable";
  public static final boolean DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED = true;

  public static final String SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT =
      "llap.shuffle.connection-keep-alive.timeout";
  public static final int DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT = 5; //seconds

  public static final String SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE =
      "llap.shuffle.mapoutput-info.meta.cache.size";
  public static final int DEFAULT_SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE =
      10000;

  public static final String CONNECTION_CLOSE = "close";

  public static final String SUFFLE_SSL_FILE_BUFFER_SIZE_KEY =
    "llap.shuffle.ssl.file.buffer.size";

  public static final int DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE = 60 * 1024;

  public static final String MAX_SHUFFLE_CONNECTIONS = "llap.shuffle.max.connections";
  public static final int DEFAULT_MAX_SHUFFLE_CONNECTIONS = 0; // 0 implies no limit
  
  public static final String MAX_SHUFFLE_THREADS = "llap.shuffle.max.threads";
  // 0 implies Netty default of 2 * number of available processors
  public static final int DEFAULT_MAX_SHUFFLE_THREADS = Runtime.getRuntime().availableProcessors() * 3;
  
  public static final String SHUFFLE_BUFFER_SIZE = 
      "llap.shuffle.transfer.buffer.size";
  public static final int DEFAULT_SHUFFLE_BUFFER_SIZE = 128 * 1024;
  
  public static final String  SHUFFLE_TRANSFERTO_ALLOWED = 
      "llap.shuffle.transferTo.allowed";
  public static final boolean DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED = true;

  static final String DATA_FILE_NAME = "file.out";
  static final String INDEX_FILE_NAME = "file.out.index";
  private static final AtomicBoolean started = new AtomicBoolean(false);
  private static final AtomicBoolean initing = new AtomicBoolean(false);
  private static ShuffleHandler INSTANCE;
  private static final String TIMEOUT_HANDLER = "timeout";


  final boolean connectionKeepAliveEnabled;
  final int connectionKeepAliveTimeOut;
  final int mapOutputMetaInfoCacheSize;
  private final LocalDirAllocator lDirAlloc =
      new LocalDirAllocator(SHUFFLE_HANDLER_LOCAL_DIRS);
  private final Shuffle shuffle;

  @Override
  public void registerAttemptDirs(AttemptPathIdentifier identifier,
                                  AttemptPathInfo pathInfo) {
    shuffle.registerAttemptDirs(identifier, pathInfo);
  }


  @Metrics(about="Shuffle output metrics", context="mapred")
  static class ShuffleMetrics implements ChannelFutureListener {
    @Metric("Shuffle output in bytes")
    MutableCounterLong shuffleOutputBytes;
    @Metric("# of failed shuffle outputs")
    MutableCounterInt shuffleOutputsFailed;
    @Metric("# of succeeded shuffle outputs")
    MutableCounterInt shuffleOutputsOK;
    @Metric("# of current shuffle connections")
    MutableGaugeInt shuffleConnections;

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (future.isSuccess()) {
        shuffleOutputsOK.incr();
      } else {
        shuffleOutputsFailed.incr();
      }
      shuffleConnections.decr();
    }
  }

  @VisibleForTesting
  ShuffleHandler(Configuration conf) {
    this.conf = conf;
    manageOsCache = conf.getBoolean(SHUFFLE_MANAGE_OS_CACHE,
        DEFAULT_SHUFFLE_MANAGE_OS_CACHE);
    shouldAlwaysEvictOsCache = conf.getBoolean(SHUFFLE_OS_CACHE_ALWAYS_EVICT,
        DEFAULT_SHUFFLE_OS_CACHE_ALWAYS_EVICT);

    readaheadLength = conf.getInt(SHUFFLE_READAHEAD_BYTES,
        DEFAULT_SHUFFLE_READAHEAD_BYTES);

    maxShuffleConnections = conf.getInt(MAX_SHUFFLE_CONNECTIONS,
        DEFAULT_MAX_SHUFFLE_CONNECTIONS);
    int maxShuffleThreads = conf.getInt(MAX_SHUFFLE_THREADS,
        DEFAULT_MAX_SHUFFLE_THREADS);
    if (maxShuffleThreads == 0) {
      maxShuffleThreads = 2 * Runtime.getRuntime().availableProcessors();
    }

    port = conf.getInt(SHUFFLE_PORT_CONFIG_KEY, DEFAULT_SHUFFLE_PORT);
    // TODO: this is never used
    localDirs = conf.getTrimmedStrings(SHUFFLE_HANDLER_LOCAL_DIRS);

    shuffleBufferSize = conf.getInt(SHUFFLE_BUFFER_SIZE,
        DEFAULT_SHUFFLE_BUFFER_SIZE);

    shuffleTransferToAllowed = conf.getBoolean(SHUFFLE_TRANSFERTO_ALLOWED,
        DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED);

    final String BOSS_THREAD_NAME_PREFIX = "ShuffleHandler Netty Boss #";
    AtomicInteger bossThreadCounter = new AtomicInteger(0);
    bossGroup = new NioEventLoopGroup(maxShuffleThreads, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, BOSS_THREAD_NAME_PREFIX + bossThreadCounter.incrementAndGet());
      }
    });

    final String WORKER_THREAD_NAME_PREFIX = "ShuffleHandler Netty Worker #";
    AtomicInteger workerThreadCounter = new AtomicInteger(0);
    workerGroup = new NioEventLoopGroup(maxShuffleThreads, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, WORKER_THREAD_NAME_PREFIX + workerThreadCounter.incrementAndGet());
      }
    });

    sslFileBufferSize = conf.getInt(SUFFLE_SSL_FILE_BUFFER_SIZE_KEY,
        DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE);
    connectionKeepAliveEnabled =
        conf.getBoolean(SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED,
            DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED);
    connectionKeepAliveTimeOut =
        Math.max(1, conf.getInt(SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT,
            DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT));
    mapOutputMetaInfoCacheSize =
        Math.max(1, conf.getInt(SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE,
            DEFAULT_SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE));

    userRsrc = new ConcurrentHashMap<>();
    secretManager = new JobTokenSecretManager();
    shuffle = new Shuffle(conf);
    if (conf.getBoolean(SHUFFLE_DIR_WATCHER_ENABLED, SHUFFLE_DIR_WATCHER_ENABLED_DEFAULT)) {
      LOG.info("Attempting to start dirWatcher");
      DirWatcher localDirWatcher = null;
      try {
        localDirWatcher = new DirWatcher(this);
      } catch (IOException e) {
        LOG.warn("Unable to start DirWatcher. Active scans disabled");
      }
      dirWatcher = localDirWatcher;
    } else {
      LOG.info("DirWatcher disabled by config");
      dirWatcher = null;
    }
    LOG.info("manageOsCache:{}, shouldAlwaysEvictOsCache:{}, readaheadLength:{}"
        + ", maxShuffleConnections:{}, localDirs:{}"
        + ", shuffleBufferSize:{}, shuffleTransferToAllowed:{}"
        + ", connectionKeepAliveEnabled:{}, connectionKeepAliveTimeOut:{}"
        + ", mapOutputMetaInfoCacheSize:{}, sslFileBufferSize:{}",
        manageOsCache, shouldAlwaysEvictOsCache,readaheadLength, maxShuffleConnections, localDirs,
        shuffleBufferSize, shuffleTransferToAllowed, connectionKeepAliveEnabled,
        connectionKeepAliveTimeOut, mapOutputMetaInfoCacheSize, sslFileBufferSize);
  }


  public void start() throws Exception {
    ServerBootstrap bootstrap = new ServerBootstrap()
        .channel(NioServerSocketChannel.class)
        .group(bossGroup, workerGroup)
        .localAddress(port)
        .option(ChannelOption.SO_BACKLOG, NetUtil.SOMAXCONN)
        .childOption(ChannelOption.SO_KEEPALIVE, true);
    initPipeline(bootstrap, conf);

    Channel ch = bootstrap.bind().sync().channel();
    accepted.add(ch);
    port = ((InetSocketAddress)ch.localAddress()).getPort();
    conf.set(SHUFFLE_PORT_CONFIG_KEY, Integer.toString(port));
    SHUFFLE.setPort(port);
    if (dirWatcher != null) {
      dirWatcher.start();
    }
    LOG.info("LlapShuffleHandler listening on port {} (SOMAXCONN: {})", port, NetUtil.SOMAXCONN);
  }

  private void initPipeline(ServerBootstrap bootstrap, Configuration conf) throws Exception {
    SHUFFLE = getShuffle(conf);
    // TODO Setup SSL Shuffle
    //  if (conf.getBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY,
    //                      MRConfig.SHUFFLE_SSL_ENABLED_DEFAULT)) {
    //    LOG.info("Encrypted shuffle is enabled.");
    //    sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
    //    sslFactory.init();
    //  }

    ChannelInitializer<NioSocketChannel> channelInitializer =
        new ChannelInitializer<NioSocketChannel>() {
          @Override
      public void initChannel(NioSocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        if (sslFactory != null) {
          pipeline.addLast("ssl", new SslHandler(sslFactory.createSSLEngine()));
        }
        if (LOG.isDebugEnabled()) {
          pipeline.addLast("loggingHandler", new LoggingHandler(LogLevel.DEBUG));
        }
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpObjectAggregator(1 << 16));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("chunking", new ChunkedWriteHandler());
        pipeline.addLast("shuffle", SHUFFLE);
        pipeline.addLast("idle", new IdleStateHandler(0, connectionKeepAliveTimeOut, 0));
        pipeline.addLast(TIMEOUT_HANDLER, new TimeoutHandler());
      }
    };
    bootstrap.childHandler(channelInitializer);
  }

  private void destroyPipeline() {
    if (sslFactory != null) {
      sslFactory.destroy();
    }
  }

  public static void initializeAndStart(Configuration conf) throws Exception {
    if (!initing.getAndSet(true)) {
      INSTANCE = new ShuffleHandler(conf);
      INSTANCE.start();
      started.set(true);
    }
  }

  public static void shutdown() throws Exception {
    if (INSTANCE != null) {
      INSTANCE.stop();
    }
  }

  public static ShuffleHandler get() {
    Preconditions.checkState(started.get(),
        "ShuffleHandler must be started before invoking get");
    return INSTANCE;
  }

  /**
   * Serialize the shuffle port into a ByteBuffer for use later on.
   * @param port the port to be sent to the ApplicationMaster
   * @return the serialized form of the port.
   */
  public static ByteBuffer serializeMetaData(int port) throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer portDob = new DataOutputBuffer();
    portDob.writeInt(port);
    ByteBuffer buf = ByteBuffer.wrap(portDob.getData(), 0, portDob.getLength());
    portDob.close();
    return buf;
  }

  /**
   * A helper function to deserialize the metadata returned by ShuffleHandler.
   * @param meta the metadata returned by the ShuffleHandler
   * @return the port the Shuffle Handler is listening on to serve shuffle data.
   */
  public static int deserializeMetaData(ByteBuffer meta) throws IOException {
    //TODO this should be returning a class not just an int
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(meta);
    int port = in.readInt();
    return port;
  }

  /**
   * A helper function to serialize the JobTokenIdentifier to be sent to the
   * ShuffleHandler as ServiceData.
   * @param jobToken the job token to be used for authentication of
   * shuffle data requests.
   * @return the serialized version of the jobToken.
   */
  public static ByteBuffer serializeServiceData(Token<JobTokenIdentifier> jobToken) throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer jobToken_dob = new DataOutputBuffer();
    jobToken.write(jobToken_dob);
    return ByteBuffer.wrap(jobToken_dob.getData(), 0, jobToken_dob.getLength());
  }

  static Token<JobTokenIdentifier> deserializeServiceData(ByteBuffer secret) throws IOException {
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(secret);
    Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>();
    jt.readFields(in);
    return jt;
  }

  public int getPort() {
    return port;
  }

  public boolean isDirWatcherEnabled() {
    return dirWatcher != null;
  }

  /**
   * Register an application and it's associated credentials and user information.
   *
   * This method and unregisterDag must be synchronized externally to prevent races in shuffle token registration/unregistration
   * This method may be called several times but we can only set the registeredDirectories once which will be
   * in the first call in which they are not null.
   *
   * @param applicationIdString
   * @param dagIdentifier
   * @param appToken
   * @param user
   * @param appDirs
   */
  public void registerDag(String applicationIdString, int dagIdentifier,
                          Token<JobTokenIdentifier> appToken,
                          String user, String[] appDirs) {
    Integer registeredDagIdentifier = registeredApps.putIfAbsent(applicationIdString, dagIdentifier);
    // App never seen, or previous dag has been unregistered.
    if (registeredDagIdentifier == null && appToken != null ) {
      recordJobShuffleInfo(applicationIdString, user, appToken);
    }
    // Register the new dag identifier, if that's not the one currently registered.
    // Register comes in before the unregister for the previous dag
    if (registeredDagIdentifier != null && !registeredDagIdentifier.equals(dagIdentifier)) {
      registeredApps.put(applicationIdString, dagIdentifier);
      // Don't need to recordShuffleInfo since the out of sync unregister will not remove the
      // credentials
    }

    if (appDirs == null) {
      return;
    }
    registeredDagIdentifier  = registeredDirectories.put(applicationIdString, dagIdentifier);
    if (registeredDagIdentifier != null && !registeredDagIdentifier.equals(dagIdentifier)) {
      registeredDirectories.put(applicationIdString, dagIdentifier);
    }
    // First time registration, or new register comes in before the previous unregister.
    if (registeredDagIdentifier == null || !registeredDagIdentifier.equals(dagIdentifier)) {
      if (dirWatcher != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Registering watches for AppDirs: appId={}, dagId={}", applicationIdString, dagIdentifier);
        }
        for (String appDir : appDirs) {
          try {
            dirWatcher.registerDagDir(appDir, applicationIdString, dagIdentifier, user,
                5 * 60 * 1000);
          } catch (IOException e) {
            LOG.warn("Unable to register dir: " + appDir + " with watcher");
          }
        }
      }
    }
  }

  /**
   * Unregister a specific dag
   *
   * This method and registerDag must be synchronized externally to prevent races in shuffle token registration/unregistration
   *
   * @param dir
   * @param applicationIdString
   * @param dagIdentifier
   */
  public void unregisterDag(String dir, String applicationIdString, int dagIdentifier) {
    Integer currentDagIdentifier = registeredApps.get(applicationIdString);
    // Unregister may come in after the new dag has started running. The methods are expected to
    // be synchronized, hence the following check is sufficient.
    if (currentDagIdentifier != null && currentDagIdentifier.equals(dagIdentifier)) {
      registeredApps.remove(applicationIdString);
      registeredDirectories.remove(applicationIdString);
      removeJobShuffleInfo(applicationIdString);
    }
    // Unregister for the dirWatcher for the specific dagIdentifier in either case.
    if (dirWatcher != null) {
      dirWatcher.unregisterDagDir(dir, applicationIdString, dagIdentifier);
    }
  }

  protected void stop() throws Exception {
    accepted.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    if (bossGroup != null) {
      bossGroup.shutdownGracefully();
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
    }
    destroyPipeline();

    if (dirWatcher != null) {
      dirWatcher.stop();
    }
  }

  @VisibleForTesting
  public Map getRegisteredApps() {
    return new HashMap<>(registeredApps);
  }

  @VisibleForTesting
  public Map getRegisteredDirectories() {
    return new HashMap<>(registeredDirectories);
  }

  protected Shuffle getShuffle(Configuration conf) {
    return shuffle;
  }


  private void addJobToken(String appIdString, String user,
      Token<JobTokenIdentifier> jobToken) {
    // This is in place to be compatible with the MR ShuffleHandler. Requests from ShuffleInputs
    // arrive with a job_ prefix.
    String jobIdString = appIdString.replace("application", "job");
    userRsrc.putIfAbsent(jobIdString, user);
    secretManager.addTokenForJob(jobIdString, jobToken);
    LOG.info("Added token for " + jobIdString);
  }

  private void recordJobShuffleInfo(String appIdString, String user,
      Token<JobTokenIdentifier> jobToken) {
    addJobToken(appIdString, user, jobToken);
  }

  private void removeJobShuffleInfo(String appIdString) {
    secretManager.removeTokenForJob(appIdString);
    userRsrc.remove(appIdString);
  }

  static class TimeoutHandler extends ChannelDuplexHandler {

    private boolean enabledTimeout;

    void setEnabledTimeout(boolean enabledTimeout) {
      this.enabledTimeout = enabledTimeout;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof IdleStateEvent) {
        IdleStateEvent e = (IdleStateEvent) evt;
        if (e.state() == IdleState.WRITER_IDLE && enabledTimeout) {
          ctx.channel().close();
        }
      }
    }
  }

  @Sharable
  class Shuffle extends ChannelInboundHandlerAdapter {

    private final Configuration conf;
    // TODO Change the indexCache to be a guava loading cache, rather than a custom implementation.
    private final IndexCache indexCache;
    private int port;

    private final LoadingCache<AttemptPathIdentifier, AttemptPathInfo> pathCache =
        CacheBuilder.newBuilder().expireAfterAccess(300, TimeUnit.SECONDS).softValues()
            .concurrencyLevel(16)
            .removalListener(new RemovalListener<AttemptPathIdentifier, AttemptPathInfo>() {
              @Override
              public void onRemoval(
                  RemovalNotification<AttemptPathIdentifier, AttemptPathInfo> notification) {
                LOG.debug("PathCacheEviction: " + notification.getKey() + ", Reason=" +
                    notification.getCause());
              }
            })
            .maximumWeight(10 * 1024 * 1024).weigher(
            new Weigher<AttemptPathIdentifier, AttemptPathInfo>() {
              @Override
              public int weigh(AttemptPathIdentifier key, AttemptPathInfo value) {
                return key.jobId.length() + key.user.length() + key.attemptId.length() +
                    value.indexPath.toString().length() +
                    value.dataPath.toString().length();
              }
            }).build(new CacheLoader<AttemptPathIdentifier, AttemptPathInfo>() {
          @Override
          public AttemptPathInfo load(AttemptPathIdentifier key) throws
              Exception {
            String base = getBaseLocation(key.jobId, key.dagId, key.user);
            String attemptBase = base + key.attemptId;
            Path indexFileName =
                lDirAlloc.getLocalPathToRead(attemptBase + "/" + INDEX_FILE_NAME, conf);
            Path mapOutputFileName =
                lDirAlloc.getLocalPathToRead(attemptBase + "/" + DATA_FILE_NAME, conf);

            LOG.debug("Loaded : " + key + " via loader");
            if (dirWatcher != null) {
              dirWatcher.attemptInfoFound(key);
            }
            return new AttemptPathInfo(indexFileName, mapOutputFileName);

          }
        });

    public Shuffle(Configuration conf) {
      this.conf = conf;
      indexCache = new IndexCache(conf);
      this.port = conf.getInt(SHUFFLE_PORT_CONFIG_KEY, DEFAULT_SHUFFLE_PORT);
    }
    
    public void setPort(int port) {
      this.port = port;
    }

    void registerAttemptDirs(AttemptPathIdentifier identifier,
                                    AttemptPathInfo pathInfo) {
      LOG.debug("Registering " + identifier + " via watcher");
      pathCache.put(identifier, pathInfo);
    }

    private List<String> splitMaps(List<String> mapq) {
      if (null == mapq) {
        return null;
      }
      final List<String> ret = new ArrayList<String>();
      for (String s : mapq) {
        Collections.addAll(ret, s.split(","));
      }
      return ret;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
        throws Exception {
      if ((maxShuffleConnections > 0) && (accepted.size() >= maxShuffleConnections)) {
        LOG.info(String.format("Current number of shuffle connections (%d) is " + 
            "greater than or equal to the max allowed shuffle connections (%d)", 
            accepted.size(), maxShuffleConnections));
        ctx.channel().close();
        return;
      }
      accepted.add(ctx.channel());
      super.channelActive(ctx);
     
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message)
        throws Exception {
      FullHttpRequest request = (FullHttpRequest) message;
      handleRequest(ctx, request);
      request.release();
    }

    private void handleRequest(ChannelHandlerContext ctx, FullHttpRequest request)
        throws IOException {
      if (request.getMethod() != GET) {
          sendError(ctx, METHOD_NOT_ALLOWED);
          return;
      }
      // Check whether the shuffle version is compatible
      if (!ShuffleHeader.DEFAULT_HTTP_HEADER_NAME.equals(
          request.headers().get(ShuffleHeader.HTTP_HEADER_NAME))
          || !ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION.equals(
              request.headers().get(ShuffleHeader.HTTP_HEADER_VERSION))) {
        sendError(ctx, "Incompatible shuffle request version", BAD_REQUEST);
      }
      final Map<String, List<String>> q = new QueryStringDecoder(request.uri()).parameters();
      final List<String> keepAliveList = q.get("keepAlive");
      boolean keepAliveParam = false;
      if (keepAliveList != null && keepAliveList.size() == 1) {
        keepAliveParam = Boolean.parseBoolean(keepAliveList.get(0));
        LOG.debug("KeepAliveParam : {} : {}", keepAliveList, keepAliveParam);
      }
      final List<String> mapIds = splitMaps(q.get("map"));
      final List<String> reduceQ = q.get("reduce");
      final List<String> jobQ = q.get("job");
      final List<String> dagIdQ = q.get("dag");
      if (LOG.isDebugEnabled()) {
        LOG.debug("RECV: " + request.uri() +
            "\n  mapId: " + mapIds +
            "\n  reduceId: " + reduceQ +
            "\n  jobId: " + jobQ +
            "\n  dagId: " + dagIdQ +
            "\n  keepAlive: " + keepAliveParam);
      }

      if (mapIds == null || reduceQ == null || jobQ == null | dagIdQ == null) {
        sendError(ctx, "Required param job, map and reduce", BAD_REQUEST);
        return;
      }
      if (reduceQ.size() != 1 || jobQ.size() != 1 || dagIdQ.size() != 1) {
        sendError(ctx, "Too many job/reduce parameters", BAD_REQUEST);
        return;
      }
      int reduceId;
      String jobId;
      int dagId;
      try {
        reduceId = Integer.parseInt(reduceQ.get(0));
        jobId = jobQ.get(0);
        dagId = Integer.parseInt(dagIdQ.get(0));
      } catch (NumberFormatException e) {
        sendError(ctx, "Bad reduce parameter", BAD_REQUEST);
        return;
      } catch (IllegalArgumentException e) {
        sendError(ctx, "Bad job parameter", BAD_REQUEST);
        return;
      }
      final String reqUri = request.uri();
      if (null == reqUri) {
        // TODO? add upstream?
        sendError(ctx, FORBIDDEN);
        return;
      }
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      try {
        verifyRequest(jobId, ctx, request, response,
            new URL("http", "", this.port, reqUri));
      } catch (IOException e) {
        LOG.warn("Shuffle failure ", e);
        sendError(ctx, e.getMessage(), UNAUTHORIZED);
        return;
      }

      Map<String, MapOutputInfo> mapOutputInfoMap =
          new HashMap<String, MapOutputInfo>();
      Channel ch = ctx.channel();
      // In case of KeepAlive, ensure that timeout handler does not close connection until entire
      // response is written (i.e, response headers + mapOutput).
      ChannelPipeline pipeline = ch.pipeline();
      TimeoutHandler timeoutHandler = (TimeoutHandler)pipeline.get(TIMEOUT_HANDLER);
      timeoutHandler.setEnabledTimeout(false);

      String user = userRsrc.get(jobId);
      try {
        populateHeaders(mapIds, jobId, dagId, user, reduceId,
            response, keepAliveParam, mapOutputInfoMap);
      } catch (DiskErrorException e) { // fatal error: fetcher should be aware of that
        LOG.error("Shuffle error in populating headers (fatal: DiskErrorException):", e);
        String errorMessage = getErrorMessage(e);
        // custom message, might be noticed by fetchers
        // it should reuse the current response object, as headers have been already set for it
        sendFakeShuffleHeaderWithError(ctx, "DISK_ERROR_EXCEPTION: " + errorMessage, response);
        return;
      } catch (IOException e) {
        ch.write(response);
        LOG.error("Shuffle error in populating headers :", e);
        String errorMessage = getErrorMessage(e);
        sendError(ctx, errorMessage, INTERNAL_SERVER_ERROR);
        return;
      }
      ch.write(response);
      // TODO refactor the following into the pipeline
      ChannelFuture lastMap = null;
      for (String mapId : mapIds) {
        try {
          MapOutputInfo info = mapOutputInfoMap.get(mapId);
          // This will be hit if there's a large number of mapIds in a single request
          // (Determined by the cache size further up), in which case we go to disk again.
          if (info == null) {
            info = getMapOutputInfo(jobId, dagId, mapId, reduceId, user);
          }
          lastMap =
              sendMapOutput(ctx, ch, user, mapId,
                reduceId, info);
          if (null == lastMap) {
            sendError(ctx, NOT_FOUND);
            return;
          }
        } catch (IOException e) {
          LOG.error("Shuffle error :", e);
          String errorMessage = getErrorMessage(e);
          sendError(ctx, errorMessage, INTERNAL_SERVER_ERROR);
          return;
        }
      }
      // by this special message flushed, we can make sure the whole response is finished
      ch.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
      // If Keep alive is enabled, do not close the connection.
      if (!keepAliveParam && !connectionKeepAliveEnabled) {
        lastMap.addListener(ChannelFutureListener.CLOSE);
      } else {
        lastMap.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
              // On error close the channel.
              future.channel().close();
              return;
            }
            // Entire response is written out. Safe to enable timeout handling.
            timeoutHandler.setEnabledTimeout(true);
          }
        });
      }
    }

    private String getErrorMessage(Throwable t) {
      StringBuilder sb = new StringBuilder(t.getMessage());
      while (t.getCause() != null) {
        sb.append(t.getCause().getMessage());
        t = t.getCause();
      }
      return sb.toString();
    }


    protected MapOutputInfo getMapOutputInfo(String jobId, int dagId, String mapId,
                                             int reduce, String user) throws IOException {
      AttemptPathInfo pathInfo;
      try {
        AttemptPathIdentifier identifier = new AttemptPathIdentifier(jobId, dagId, user, mapId);
        pathInfo = pathCache.get(identifier);
        LOG.debug("Retrieved pathInfo for {} check for corresponding "
            + "loaded messages to determine whether it was loaded or cached", identifier);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        } else {
          throw new RuntimeException(e.getCause());
        }
      }

      TezIndexRecord info =
          indexCache.getIndexInformation(mapId, reduce, pathInfo.indexPath, user);

      if (LOG.isDebugEnabled()) {
        LOG.debug("jobId=" + jobId + ", mapId=" + mapId + ",dataFile=" + pathInfo.dataPath +
            ", indexFile=" + pathInfo.indexPath);
      }

      // TODO Get rid of MapOutputInfo if possible
      MapOutputInfo outputInfo = new MapOutputInfo(pathInfo.dataPath, info);
      return outputInfo;
    }

    protected void populateHeaders(List<String> mapIds, String jobId, int dagId,
        String user, int reduce, HttpResponse response,
        boolean keepAliveParam, Map<String, MapOutputInfo> mapOutputInfoMap)
        throws IOException {
      // Reads the index file for each requested mapId, and figures out the overall
      // length of the response - which is populated into the response header.

      long contentLength = 0;
      for (String mapId : mapIds) {
        MapOutputInfo outputInfo = getMapOutputInfo(jobId, dagId, mapId, reduce, user);
        // mapOutputInfoMap is used to share the lookups with the caller
        if (mapOutputInfoMap.size() < mapOutputMetaInfoCacheSize) {
          mapOutputInfoMap.put(mapId, outputInfo);
        }
        ShuffleHeader header =
            new ShuffleHeader(mapId, outputInfo.indexRecord.getPartLength(),
                outputInfo.indexRecord.getRawLength(), reduce);
        DataOutputBuffer dob = new DataOutputBuffer();
        header.write(dob);

        contentLength += outputInfo.indexRecord.getPartLength();
        contentLength += dob.getLength();
      }

      // Now set the response headers.
      setResponseHeaders(response, keepAliveParam, contentLength);
    }

    protected void setResponseHeaders(HttpResponse response,
        boolean keepAliveParam, long contentLength) {
      if (!connectionKeepAliveEnabled && !keepAliveParam) {
        LOG.info("Setting connection close header...");
        response.headers().add(HttpHeaders.Names.CONNECTION, CONNECTION_CLOSE);
      } else {
        response.headers().add(HttpHeaders.Names.CONTENT_LENGTH,
          String.valueOf(contentLength));
        response.headers().add(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        response.headers().add(HttpHeaders.Values.KEEP_ALIVE, "timeout="
            + connectionKeepAliveTimeOut);
        LOG.debug("Content Length in shuffle : " + contentLength);
      }
    }

    class MapOutputInfo {
      final Path mapOutputFileName; // 100-200 byte string. Maybe replace with a local-dir-id, and construct on the fly.
      final TezIndexRecord indexRecord; // 3 longs + reference overheads.

      MapOutputInfo(Path mapOutputFileName, TezIndexRecord indexRecord) {
        this.mapOutputFileName = mapOutputFileName;
        this.indexRecord = indexRecord;
      }
    }

    protected void verifyRequest(String appid, ChannelHandlerContext ctx,
        HttpRequest request, HttpResponse response, URL requestUri)
        throws IOException {
      SecretKey tokenSecret = secretManager.retrieveTokenSecret(appid);
      if (null == tokenSecret) {
        LOG.info("Request for unknown token " + appid);
        throw new IOException("could not find jobid");
      }
      // string to encrypt
      String enc_str = SecureShuffleUtils.buildMsgFrom(requestUri);
      // hash from the fetcher
      String urlHashStr =
        request.headers().get(SecureShuffleUtils.HTTP_HEADER_URL_HASH);
      if (urlHashStr == null) {
        LOG.info("Missing header hash for " + appid);
        throw new IOException("fetcher cannot be authenticated");
      }
      if (LOG.isDebugEnabled()) {
        int len = urlHashStr.length();
        LOG.debug("verifying request. enc_str=" + enc_str + "; hash=..." +
            urlHashStr.substring(len-len/2, len-1));
      }
      // verify - throws exception
      SecureShuffleUtils.verifyReply(urlHashStr, enc_str, tokenSecret);
      // verification passed - encode the reply
      String reply =
        SecureShuffleUtils.generateHash(urlHashStr.getBytes(Charsets.UTF_8), 
            tokenSecret);
      response.headers().add(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH, reply);
      // Put shuffle version into http header
      response.headers().add(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      response.headers().add(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      if (LOG.isDebugEnabled()) {
        int len = reply.length();
        LOG.debug("Fetcher request verified. enc_str=" + enc_str + ";reply=" +
            reply.substring(len-len/2, len-1));
      }
    }

    protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx, Channel ch,
        String user, String mapId, int reduce, MapOutputInfo mapOutputInfo)
        throws IOException {
      final TezIndexRecord info = mapOutputInfo.indexRecord;
      final ShuffleHeader header =
        new ShuffleHeader(mapId, info.getPartLength(), info.getRawLength(), reduce);
      final DataOutputBuffer dob = new DataOutputBuffer();
      header.write(dob);
      ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
      final File spillfile =
          new File(mapOutputInfo.mapOutputFileName.toString());
      RandomAccessFile spill;
      try {
        spill = SecureIOUtils.openForRandomRead(spillfile, "r", user, null);
      } catch (FileNotFoundException e) {
        LOG.info(spillfile + " not found");
        return null;
      }
      ChannelFuture writeFuture;
      if (ch.pipeline().get(SslHandler.class) == null) {
        boolean canEvictAfterTransfer = true;
        if (!shouldAlwaysEvictOsCache) {
          canEvictAfterTransfer = (reduce > 0); // e.g broadcast data
        }
        final FadvisedFileRegion partition = new FadvisedFileRegion(spill,
            info.getStartOffset(), info.getPartLength(), manageOsCache, readaheadLength,
            readaheadPool, spillfile.getAbsolutePath(), 
            shuffleBufferSize, shuffleTransferToAllowed, canEvictAfterTransfer);
        writeFuture = ch.write(partition);
      } else {
        // HTTPS cannot be done with zero copy.
        final FadvisedChunkedFile chunk = new FadvisedChunkedFile(spill,
            info.getStartOffset(), info.getPartLength(), sslFileBufferSize,
            manageOsCache, readaheadLength, readaheadPool,
            spillfile.getAbsolutePath());
        writeFuture = ch.write(chunk);
      }
      return writeFuture;
    }

    protected void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
      sendError(ctx, "", status);
    }

    protected void sendError(ChannelHandlerContext ctx, String message, HttpResponseStatus status) {
      FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status);
      sendError(ctx, message, response);
    }

    protected void sendError(ChannelHandlerContext ctx, String message, FullHttpResponse response) {
      sendError(ctx, Unpooled.copiedBuffer(message, CharsetUtil.UTF_8), response);
    }

    private void sendFakeShuffleHeaderWithError(ChannelHandlerContext ctx, String message,
        HttpResponse response) throws IOException {
      FullHttpResponse fullResponse =
          new DefaultFullHttpResponse(response.getProtocolVersion(), response.getStatus());
      fullResponse.headers().set(response.headers());

      ShuffleHeader header = new ShuffleHeader(message, -1, -1, -1);
      DataOutputBuffer out = new DataOutputBuffer();
      header.write(out);

      sendError(ctx, wrappedBuffer(out.getData(), 0, out.getLength()), fullResponse);
    }

    protected void sendError(ChannelHandlerContext ctx, ByteBuf content,
        FullHttpResponse response) {
      response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
      // Put shuffle version into http header
      response.headers().add(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      response.headers().add(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      response.content().writeBytes(content);

      // Close the connection as soon as the error message is sent.
      ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
      /*
       * The general rule of thumb is that the party that accesses a reference-counted object last
       * is also responsible for the destruction of that reference-counted object.
       */
      content.release();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      if (cause instanceof TooLongFrameException) {
        sendError(ctx, BAD_REQUEST);
        return;
      } else if (cause instanceof IOException) {
        if (cause instanceof ClosedChannelException) {
          LOG.debug("Ignoring closed channel error", cause);
          return;
        }
        String message = String.valueOf(cause.getMessage());
        if (IGNORABLE_ERROR_MESSAGE.matcher(message).matches()) {
          LOG.debug("Ignoring client socket close", cause);
          return;
        }
      }

      LOG.error("Shuffle error: ", cause);
      if (ctx.channel().isActive()) {
        LOG.error("Shuffle error", cause);
        sendError(ctx, INTERNAL_SERVER_ERROR);
      }
    }
  }


  private static final String USERCACHE_CONSTANT = "usercache";
  private static final String APPCACHE_CONSTANT = "appcache";

  private static String getBaseLocation(String jobIdString, int dagId, String user) {
    // $x/$user/appcache/$appId/${dagId}/output/$mapId
    // TODO: Once Shuffle is out of NM, this can use MR APIs to convert
    // between App and Job
    String parts[] = jobIdString.split("_");
    Preconditions.checkArgument(parts.length == 3, "Invalid jobId. Expecting 3 parts");
    final ApplicationId appID =
        ApplicationId.newInstance(Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
    final String baseStr =
        USERCACHE_CONSTANT + "/" + user + "/"
            + APPCACHE_CONSTANT + "/"
            + ConverterUtils.toString(appID)
            +  "/" + dagId
            +  "/output" + "/";
    return baseStr;
  }

  static class AttemptPathInfo {
    // TODO Change this over to just store local dir indices, instead of the entire path. Far more efficient.
    private final Path indexPath;
    private final Path dataPath;

    public AttemptPathInfo(Path indexPath, Path dataPath) {
      this.indexPath = indexPath;
      this.dataPath = dataPath;
    }
  }

  static class AttemptPathIdentifier {
    private final String jobId;
    private final int dagId;
    private final String user;
    private final String attemptId;

    public AttemptPathIdentifier(String jobId, int dagId, String user, String attemptId) {
      this.jobId = jobId;
      this.dagId = dagId;
      this.user = user;
      this.attemptId = attemptId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      AttemptPathIdentifier that = (AttemptPathIdentifier) o;

      if (dagId != that.dagId) {
        return false;
      }
      if (!jobId.equals(that.jobId)) {
        return false;
      }
      return attemptId.equals(that.attemptId);

    }

    @Override
    public int hashCode() {
      int result = jobId.hashCode();
      result = 31 * result + dagId;
      result = 31 * result + attemptId.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "AttemptPathIdentifier{" +
          "jobId='" + jobId + '\'' +
          ", dagId=" + dagId +
          ", user='" + user + '\'' +
          ", attemptId='" + attemptId + '\'' +
          '}';
    }
  }
}
