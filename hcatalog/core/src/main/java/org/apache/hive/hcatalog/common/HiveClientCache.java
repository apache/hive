/**
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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
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

  final private Cache<HiveClientCacheKey, ICacheableMetaStoreClient> hiveCache;
  private static final Logger LOG = LoggerFactory.getLogger(HiveClientCache.class);
  private final int timeout;
  // This lock is used to make sure removalListener won't close a client that is being contemplated for returning by get()
  private final Object CACHE_TEARDOWN_LOCK = new Object();

  private static final AtomicInteger nextId = new AtomicInteger(0);

  private final ScheduledFuture<?> cleanupHandle; // used to cleanup cache

  // Since HiveMetaStoreClient is not threadsafe, hive clients are not  shared across threads.
  // Thread local variable containing each thread's unique ID, is used as one of the keys for the cache
  // causing each thread to get a different client even if the hiveConf is same.
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
    return RetryingMetaStoreClient.getProxy(hiveConf);
  }

  public HiveClientCache(HiveConf hiveConf) {
    this(hiveConf.getInt(HCatConstants.HCAT_HIVE_CLIENT_EXPIRY_TIME,
        DEFAULT_HIVE_CACHE_EXPIRY_TIME_SECONDS));
  }
  /**
   * @param timeout the length of time in seconds after a client is created that it should be automatically removed
   */
  public HiveClientCache(final int timeout) {
    this.timeout = timeout;
    RemovalListener<HiveClientCacheKey, ICacheableMetaStoreClient> removalListener =
      new RemovalListener<HiveClientCacheKey, ICacheableMetaStoreClient>() {
        @Override
        public void onRemoval(RemovalNotification<HiveClientCacheKey, ICacheableMetaStoreClient> notification) {
          ICacheableMetaStoreClient hiveMetaStoreClient = notification.getValue();
          if (hiveMetaStoreClient != null) {
            synchronized (CACHE_TEARDOWN_LOCK) {
              hiveMetaStoreClient.setExpiredFromCache();
              hiveMetaStoreClient.tearDownIfUnused();
            }
          }
        }
      };
    hiveCache = CacheBuilder.newBuilder()
      .expireAfterWrite(timeout, TimeUnit.SECONDS)
      .removalListener(removalListener)
      .build();

    // Add a maintenance thread that will attempt to trigger a cache clean continuously
    Runnable cleanupThread = new Runnable() {
      @Override
      public void run() {
        hiveCache.cleanUp();
      }
    };

    /**
     * We need to use a cleanup interval, which is how often the cleanup thread will kick in
     * and go do a check to see if any of the connections can be expired. We don't want to
     * do this too often, because it'd be like having a mini-GC going off every so often,
     * so we limit it to a minimum of DEFAULT_HIVE_CACHE_EXPIRY_TIME_SECONDS. If the client
     * has explicitly set a larger timeout on the cache, though, we respect that, and use that
     */
    long cleanupInterval = DEFAULT_HIVE_CACHE_EXPIRY_TIME_SECONDS;

    if (timeout > cleanupInterval){
      cleanupInterval = timeout;
    }

    /**
     * Create the cleanup handle. In addition to cleaning up every cleanupInterval, we add
     * a slight offset, so that the very first time it runs, it runs with a slight delay, so
     * as to catch any other connections that were closed when the first timeout happened.
     * As a result, the time we can expect an unused connection to be reaped is
     * 5 seconds after the first timeout, and then after that, it'll check for whether or not
     * it can be cleaned every max(DEFAULT_HIVE_CACHE_EXPIRY_TIME_SECONDS,timeout) seconds
     */
    ThreadFactory daemonThreadFactory = (new ThreadFactoryBuilder()).setDaemon(true)
        .setNameFormat("HiveClientCache-cleaner-%d").build();

    cleanupHandle = Executors.newScheduledThreadPool(1, daemonThreadFactory).scheduleWithFixedDelay(
        cleanupThread,
        timeout + 5, cleanupInterval, TimeUnit.SECONDS);


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
    Runtime.getRuntime().addShutdownHook(cleanupHiveClientShutdownThread);
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
  }

  public void cleanup() {
    hiveCache.cleanUp();
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
  public ICacheableMetaStoreClient get(final HiveConf hiveConf) throws MetaException, IOException, LoginException {
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
          return
              (ICacheableMetaStoreClient) RetryingMetaStoreClient.getProxy(cacheKey.getHiveConf(),
                  new Class<?>[]{HiveConf.class, Integer.class},
                  new Object[]{cacheKey.getHiveConf(), timeout},
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
  public static class HiveClientCacheKey {
    final private String metaStoreURIs;
    final private UserGroupInformation ugi;
    final private HiveConf hiveConf;
    final private int threadId;

    private HiveClientCacheKey(HiveConf hiveConf, final int threadId) throws IOException, LoginException {
      this.metaStoreURIs = hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);
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
  }

  public interface ICacheableMetaStoreClient extends IMetaStoreClient {

    void acquire();

    void release();

    void setExpiredFromCache();

    AtomicInteger getUsers();

    boolean isClosed();

    boolean isOpen();

    void tearDownIfUnused();

    void tearDown();
  }

  /**
   * Add # of current users on HiveMetaStoreClient, so that the client can be cleaned when no one is using it.
   */
  static class CacheableHiveMetaStoreClient extends HiveMetaStoreClient implements ICacheableMetaStoreClient {

    private final AtomicInteger users = new AtomicInteger(0);
    private volatile boolean expiredFromCache = false;
    private boolean isClosed = false;
    private final long expiryTime;
    private static final int EXPIRY_TIME_EXTENSION_IN_MILLIS = 60 * 1000;

    CacheableHiveMetaStoreClient(final HiveConf conf, final Integer timeout) throws MetaException {
      super(conf);
      // Extend the expiry time with some extra time on top of guava expiry time to make sure
      // that items closed() are for sure expired and would never be returned by guava.
      this.expiryTime = System.currentTimeMillis() + timeout * 1000 + EXPIRY_TIME_EXTENSION_IN_MILLIS;
    }

    public void acquire() {
      users.incrementAndGet();
    }

    public void release() {
      users.decrementAndGet();
    }

    public void setExpiredFromCache() {
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
    public void close() {
      release();
      if (System.currentTimeMillis() >= expiryTime)
        setExpiredFromCache();
      tearDownIfUnused();
    }

    /**
     * Tear down only if
     *  1. There are no active user
     *  2. It has expired from the cache
     */
    public void tearDownIfUnused() {
      if (users.get() == 0 && expiredFromCache) {
        this.tearDown();
      }
    }

    /**
     * Close if not closed already
     */
    public synchronized void tearDown() {
      try {
        if (!isClosed) {
          super.close();
        }
        isClosed = true;
      } catch (Exception e) {
        LOG.warn("Error closing hive metastore client. Ignored.", e);
      }
    }

    /**
     * Last effort to clean up, may not even get called.
     * @throws Throwable
     */
    @Override
    protected void finalize() throws Throwable {
      try {
        this.tearDown();
      } finally {
        super.finalize();
      }
    }
  }
}
