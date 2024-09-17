/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.common;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.login.LoginException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.annotation.NoReconnect;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A thread safe time expired cache for HiveMetaStoreClient
 */
class HiveClientCache {
  public final static int DEFAULT_HIVE_CACHE_EXPIRY_TIME_SECONDS = 2 * 60;
  public final static int DEFAULT_HIVE_CACHE_INITIAL_CAPACITY = 50;
  public final static int DEFAULT_HIVE_CACHE_MAX_CAPACITY = 50;
  public final static boolean DEFAULT_HIVE_CLIENT_CACHE_STATS_ENABLED = false;

  private final Cache<HiveClientCacheKey, ICacheableMetaStoreClient> hiveCache;
  private static final Logger LOG = LoggerFactory.getLogger(HiveClientCache.class);
  private final int timeout;
  // This lock is used to make sure removalListener won't close a client that is being contemplated for returning by get()
  private final Object CACHE_TEARDOWN_LOCK = new Object();

  private static final AtomicInteger nextId = new AtomicInteger(0);

  private final ScheduledFuture<?> cleanupHandle; // used to cleanup cache

  private boolean enableStats;

  // Since HiveMetaStoreClient is not threadsafe, hive clients are not  shared across threads.
  // Thread local variable containing each thread's unique ID, is used as one of the keys for the cache
  // causing each thread to get a different client even if the conf is same.
  private static final ThreadLocal<Integer> threadId =
    new ThreadLocal<Integer>() {
      @Override
      protected Integer initialValue() {
        return nextId.getAndIncrement();
      }
    };

  private int getThreadId() {
    return threadId.get();
  }

  public static IMetaStoreClient getNonCachedHiveMetastoreClient(HiveConf hiveConf) throws MetaException {
    return RetryingMetaStoreClient.getProxy(hiveConf, true);
  }

  public HiveClientCache(HiveConf hiveConf) {
    this(hiveConf.getInt(HCatConstants.HCAT_HIVE_CLIENT_EXPIRY_TIME, DEFAULT_HIVE_CACHE_EXPIRY_TIME_SECONDS),
        hiveConf.getInt(HCatConstants.HCAT_HIVE_CLIENT_CACHE_INITIAL_CAPACITY, DEFAULT_HIVE_CACHE_INITIAL_CAPACITY),
        hiveConf.getInt(HCatConstants.HCAT_HIVE_CLIENT_CACHE_MAX_CAPACITY, DEFAULT_HIVE_CACHE_MAX_CAPACITY),
        hiveConf.getBoolean(HCatConstants.HCAT_HIVE_CLIENT_CACHE_STATS_ENABLED, DEFAULT_HIVE_CLIENT_CACHE_STATS_ENABLED));

  }

  /**
   * @deprecated This constructor will be made private or removed as more configuration properties are required.
   */
  @Deprecated
  public HiveClientCache(final int timeout) {
    this(timeout, DEFAULT_HIVE_CACHE_INITIAL_CAPACITY, DEFAULT_HIVE_CACHE_MAX_CAPACITY, DEFAULT_HIVE_CLIENT_CACHE_STATS_ENABLED);
  }

  /**
   * @param timeout the length of time in seconds after a client is created that it should be automatically removed
   */
  private HiveClientCache(final int timeout, final int initialCapacity, final int maxCapacity, final boolean enableStats) {
    this.timeout = timeout;
    this.enableStats = enableStats;

    LOG.info("Initializing cache: eviction-timeout=" + timeout + " initial-capacity=" + initialCapacity + " maximum-capacity=" + maxCapacity);

    CacheBuilder builder = CacheBuilder.newBuilder()
      .initialCapacity(initialCapacity)
      .maximumSize(maxCapacity)
      .expireAfterAccess(timeout, TimeUnit.SECONDS)
      .removalListener(createRemovalListener());

    /*
     * Guava versions <12.0 have stats collection enabled by default and do not expose a recordStats method.
     * Check for newer versions of the library and ensure that stats collection is enabled by default.
     */
    try {
      java.lang.reflect.Method m = builder.getClass().getMethod("recordStats", null);
      m.invoke(builder, null);
    } catch (NoSuchMethodException e) {
      LOG.debug("Using a version of guava <12.0. Stats collection is enabled by default.");
    } catch (Exception e) {
      LOG.warn("Unable to invoke recordStats method.", e);
    }

    this.hiveCache = builder.build();

    /*
     * We need to use a cleanup interval, which is how often the cleanup thread will kick in
     * and go do a check to see if any of the connections can be expired. We don't want to
     * do this too often, because it'd be like having a mini-GC going off every so often,
     * so we limit it to a minimum of DEFAULT_HIVE_CACHE_EXPIRY_TIME_SECONDS. If the client
     * has explicitly set a larger timeout on the cache, though, we respect that, and use that
     */
    long cleanupInterval = timeout > DEFAULT_HIVE_CACHE_EXPIRY_TIME_SECONDS ? timeout : DEFAULT_HIVE_CACHE_EXPIRY_TIME_SECONDS;

    this.cleanupHandle = createCleanupThread(cleanupInterval);

    createShutdownHook();
  }

  private RemovalListener<HiveClientCacheKey, ICacheableMetaStoreClient> createRemovalListener() {
    RemovalListener<HiveClientCacheKey, ICacheableMetaStoreClient> listener =
      new RemovalListener<HiveClientCacheKey, ICacheableMetaStoreClient>() {
        @Override
        public void onRemoval(RemovalNotification<HiveClientCacheKey, ICacheableMetaStoreClient> notification) {
          ICacheableMetaStoreClient hiveMetaStoreClient = notification.getValue();
          if (hiveMetaStoreClient != null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Evicting client: " + Integer.toHexString(System.identityHashCode(hiveMetaStoreClient)));
            }

            // TODO: This global lock may not be necessary as all concurrent methods in ICacheableMetaStoreClient
            // are synchronized.
            synchronized (CACHE_TEARDOWN_LOCK) {
              hiveMetaStoreClient.setExpiredFromCache();
              hiveMetaStoreClient.tearDownIfUnused();
            }
          }
        }
      };

    return listener;
  }

  private ScheduledFuture<?> createCleanupThread(long interval) {
    // Add a maintenance thread that will attempt to trigger a cache clean continuously
    Runnable cleanupThread = new Runnable() {
      @Override
      public void run() {
        cleanup();
      }
    };

    /**
     * Create the cleanup handle. In addition to cleaning up every cleanupInterval, we add
     * a slight offset, so that the very first time it runs, it runs with a slight delay, so
     * as to catch any other connections that were closed when the first timeout happened.
     * As a result, the time we can expect an unused connection to be reaped is
     * 5 seconds after the first timeout, and then after that, it'll check for whether or not
     * it can be cleaned every max(DEFAULT_HIVE_CACHE_EXPIRY_TIME_SECONDS,timeout) seconds
     */
    ThreadFactory daemonThreadFactory = (new ThreadFactoryBuilder()).setDaemon(true)
      .setNameFormat("HiveClientCache-cleaner-%d")
      .build();

    return Executors.newScheduledThreadPool(1, daemonThreadFactory)
      .scheduleWithFixedDelay(cleanupThread, timeout + 5, interval, TimeUnit.SECONDS);
  }

  private void createShutdownHook() {
    // Add a shutdown hook for cleanup, if there are elements remaining in the cache which were not cleaned up.
    // This is the best effort approach. Ignore any error while doing so. Notice that most of the clients
    // would get cleaned up via either the removalListener or the close() call, only the active clients
    // that are in the cache or expired but being used in other threads wont get cleaned. The following code will only
    // clean the active cache ones. The ones expired from cache but being hold by other threads are in the mercy
    // of finalize() being called.
    Thread cleanupHiveClientShutdownThread = new Thread() {
      @Override
      public void run() {
        LOG.debug("Cleaning up hive client cache in ShutDown hook");
        cleanupHandle.cancel(false); // Cancel the maintenance thread.
        closeAllClientsQuietly();
      }
    };

    ShutdownHookManager.addShutdownHook(cleanupHiveClientShutdownThread);
  }

  /**
   * Note: This doesn't check if they are being used or not, meant only to be called during shutdown etc.
   */
  void closeAllClientsQuietly() {
    try {
      ConcurrentMap<HiveClientCacheKey, ICacheableMetaStoreClient> elements = hiveCache.asMap();
      for (ICacheableMetaStoreClient cacheableHiveMetaStoreClient : elements.values()) {
        cacheableHiveMetaStoreClient.tearDown();
      }
    } catch (Exception e) {
      LOG.warn("Clean up of hive clients in the cache failed. Ignored", e);
    }

    if (this.enableStats) {
      LOG.info("Cache statistics after shutdown: size=" + hiveCache.size() + " " + hiveCache.stats());
    }
  }

  public void cleanup() {
    // TODO: periodically reload a new HiveConf to check if stats reporting is enabled.
    hiveCache.cleanUp();

    if (enableStats) {
      LOG.info("Cache statistics after cleanup: size=" + hiveCache.size() + " " + hiveCache.stats());
    }
  }

  /**
   * Returns a cached client if exists or else creates one, caches and returns it. It also checks that the client is
   * healthy and can be reused
   * @param hiveConf
   * @return the hive client
   * @throws MetaException
   * @throws IOException
   * @throws LoginException
   */
  public IMetaStoreClient get(final HiveConf hiveConf) throws MetaException, IOException, LoginException {
    final HiveClientCacheKey cacheKey = HiveClientCacheKey.fromHiveConf(hiveConf, getThreadId());
    ICacheableMetaStoreClient cacheableHiveMetaStoreClient = null;

    // the hmsc is not shared across threads. So the only way it could get closed while we are doing healthcheck
    // is if removalListener closes it. The synchronization takes care that removalListener won't do it
    synchronized (CACHE_TEARDOWN_LOCK) {
      cacheableHiveMetaStoreClient = getOrCreate(cacheKey);
      cacheableHiveMetaStoreClient.acquire();
    }
    if (!cacheableHiveMetaStoreClient.isOpen()) {
      synchronized (CACHE_TEARDOWN_LOCK) {
        hiveCache.invalidate(cacheKey);
        cacheableHiveMetaStoreClient.close();
        cacheableHiveMetaStoreClient = getOrCreate(cacheKey);
        cacheableHiveMetaStoreClient.acquire();
      }
    }
    return cacheableHiveMetaStoreClient;
  }

  /**
   * Return from cache if exists else create/cache and return
   * @param cacheKey
   * @return
   * @throws IOException
   * @throws MetaException
   * @throws LoginException
   */
  private ICacheableMetaStoreClient getOrCreate(final HiveClientCacheKey cacheKey)
      throws IOException, MetaException, LoginException {
    try {
      return hiveCache.get(cacheKey, new Callable<ICacheableMetaStoreClient>() {
        @Override
        public ICacheableMetaStoreClient call() throws MetaException {
          // This is called from HCat, so always allow embedded metastore (as was the default).
          return
              (ICacheableMetaStoreClient) RetryingMetaStoreClient.getProxy(cacheKey.getHiveConf(),
                  new Class<?>[]{HiveConf.class, Integer.class, Boolean.class},
                  new Object[]{cacheKey.getHiveConf(), timeout, true},
                  CacheableHiveMetaStoreClient.class.getName());
        }
      });
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      if (t instanceof IOException) {
        throw (IOException) t;
      } else if (t instanceof MetaException) {
        throw (MetaException) t;
      } else if (t instanceof LoginException) {
        throw (LoginException) t;
      } else {
        throw new IOException("Error creating hiveMetaStoreClient", t);
      }
    }
  }

  /**
   * A class to wrap HiveConf and expose equality based only on UserGroupInformation and the metaStoreURIs.
   * This becomes the key for the cache and this way the same HiveMetaStoreClient would be returned if
   * UserGroupInformation and metaStoreURIs are same. This function can evolve to express
   * the cases when HiveConf is different but the same hiveMetaStoreClient can be used
   */
  static class HiveClientCacheKey {
    final private String metaStoreURIs;
    final private UserGroupInformation ugi;
    final private HiveConf hiveConf;
    final private int threadId;

    private HiveClientCacheKey(HiveConf hiveConf, final int threadId) throws IOException, LoginException {
      this.metaStoreURIs = hiveConf.getVar(HiveConf.ConfVars.METASTORE_URIS);
      ugi = Utils.getUGI();
      this.hiveConf = hiveConf;
      this.threadId = threadId;
    }

    public static HiveClientCacheKey fromHiveConf(HiveConf hiveConf, final int threadId) throws IOException, LoginException {
      return new HiveClientCacheKey(hiveConf, threadId);
    }

    public HiveConf getHiveConf() {
      return hiveConf;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      HiveClientCacheKey that = (HiveClientCacheKey) o;
      return new EqualsBuilder().
        append(this.metaStoreURIs,
          that.metaStoreURIs).
        append(this.ugi, that.ugi).
        append(this.threadId, that.threadId).isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().
        append(metaStoreURIs).
        append(ugi).
        append(threadId).toHashCode();
    }

    @Override
    public String toString() {
      return "HiveClientCacheKey: uri=" + this.metaStoreURIs + " ugi=" + this.ugi + " thread=" + this.threadId; 
    }
  }

  @InterfaceAudience.Private
  public interface ICacheableMetaStoreClient extends IMetaStoreClient {
    @NoReconnect
    void acquire();

    @NoReconnect
    void setExpiredFromCache();

    @NoReconnect
    AtomicInteger getUsers();

    @NoReconnect
    boolean isClosed();

    /**
     * @deprecated This method is not used internally and should not be visible through HCatClient.create.
     */
    @Deprecated
    @NoReconnect
    boolean isOpen();

    @NoReconnect
    void tearDownIfUnused();

    @NoReconnect
    void tearDown();
  }

  /**
   * Add # of current users on HiveMetaStoreClient, so that the client can be cleaned when no one is using it.
   */
  static class CacheableHiveMetaStoreClient extends HiveMetaStoreClient implements ICacheableMetaStoreClient {

    private final AtomicInteger users = new AtomicInteger(0);
    private volatile boolean expiredFromCache = false;
    private boolean isClosed = false;

    CacheableHiveMetaStoreClient(final HiveConf conf, final Integer timeout, Boolean allowEmbedded)
        throws MetaException {
      super(conf, null, allowEmbedded);
    }

    /**
     * Increments the user count and optionally renews the expiration time.
     * <code>renew</code> should correspond with the expiration policy of the cache.
     * When the policy is <code>expireAfterAccess</code>, the expiration time should be extended.
     * When the policy is <code>expireAfterWrite</code>, the expiration time should not be extended.
     * A mismatch with the policy will lead to closing the connection unnecessarily after the initial
     * expiration time is generated.
     * @param renew whether the expiration time should be extended.
     */
    public synchronized void acquire() {
      users.incrementAndGet();
      if (users.get() > 1) {
        LOG.warn("Unexpected increment of user count beyond one: " + users.get() + " " + this);
      }
    }

    /**
     * Decrements the user count.
     */
    private void release() {
      if (users.get() > 0) {
        users.decrementAndGet();
      } else {
        LOG.warn("Unexpected attempt to decrement user count of zero: " + users.get() + " " + this);
      }
    }

    /**
     * Communicate to the client that it is no longer in the cache.
     * The expiration time should be voided to allow the connection to be closed at the first opportunity.
     */
    public synchronized void setExpiredFromCache() {
      if (users.get() != 0) {
        LOG.warn("Evicted client has non-zero user count: " + users.get());
      }

      expiredFromCache = true;
    }

    public boolean isClosed() {
      return isClosed;
    }

    /*
     * Used only for Debugging or testing purposes
     */
    public AtomicInteger getUsers() {
      return users;
    }

    /**
     * Make a call to hive meta store and see if the client is still usable. Some calls where the user provides
     * invalid data renders the client unusable for future use (example: create a table with very long table name)
     * @return
     */
    @Deprecated
    public boolean isOpen() {
      try {
        // Look for an unlikely database name and see if either MetaException or TException is thrown
        super.getDatabases("NonExistentDatabaseUsedForHealthCheck");
      } catch (TException e) {
        return false;
      }
      return true;
    }

    /**
     * Decrement the user count and piggyback this to set expiry flag as well, then  teardown(), if conditions are met.
     * This *MUST* be called by anyone who uses this client.
     */
    @Override
    public synchronized void close() {
      release();
      tearDownIfUnused();
    }

    /**
     * Attempt to tear down the client connection.
     * The connection will be closed if the following conditions hold:
     *  1. There are no active user holding the client.
     *  2. The client has been evicted from the cache.
     */
    public synchronized void tearDownIfUnused() {
      if (users.get() != 0) {
        LOG.warn("Non-zero user count preventing client tear down: users=" + users.get() + " expired=" + expiredFromCache);
      }

      if (users.get() == 0 && expiredFromCache) {
        this.tearDown();
      }
    }

    /**
     * Close the underlying objects irrespective of whether they are in use or not.
     */
    public void tearDown() {
      try {
        if (!isClosed) {
          super.close();
        }
        isClosed = true;
      } catch (Exception e) {
        LOG.warn("Error closing hive metastore client. Ignored.", e);
      }
    }

    @Override
    public String toString() {
      return "HCatClient: thread: " + Thread.currentThread().getId() + " users=" + users.get()
        + " expired=" + expiredFromCache + " closed=" + isClosed;
    }

    /**
     * GC is attempting to destroy the object.
     * No one references this client anymore, so it can be torn down without worrying about user counts.
     * @throws Throwable
     */
    @Override
    protected void finalize() throws Throwable {
      if (users.get() != 0) {
        LOG.warn("Closing client with non-zero user count: users=" + users.get() + " expired=" + expiredFromCache);
      }

      try {
        this.tearDown();
      } finally {
        super.finalize();
      }
    }
  }
}
