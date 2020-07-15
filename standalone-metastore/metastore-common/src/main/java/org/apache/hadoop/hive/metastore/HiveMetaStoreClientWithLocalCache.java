package org.apache.hadoop.hive.metastore;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

public class HiveMetaStoreClientWithLocalCache extends HiveMetaStoreClient {

  private static LoadingCache<CacheKey, Object> mscLocalCache;
  private static volatile boolean cacheInitialized = false;
  private boolean isCacheEnabled = true;
  private static HashMap<Class<?>, ObjectEstimator> sizeEstimator = null;

  public HiveMetaStoreClientWithLocalCache(Configuration conf) throws MetaException {
    this(conf, null, true);
  }

  public HiveMetaStoreClientWithLocalCache(Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
    this(conf, hookLoader, true);
  }

  public HiveMetaStoreClientWithLocalCache(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
    super(conf, hookLoader, allowEmbedded);

//    isCacheEnabled = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.MSC_CACHE_ENABLED);
    if (isCacheEnabled) {
      // if cache is not initialized, init size estimator and the cache.
      if (!cacheInitialized) {
        cacheInitialized = true;
        LOG.debug("Initializing local cache in HiveMetaStoreClient...");
        // initialize a size estimator with this class and all other classes in KeyType.
        sizeEstimator = IncrementalObjectSizeEstimator.createEstimators(HiveMetaStoreClientWithLocalCache.class);
        Arrays.stream(KeyType.values()).forEach(e -> {
            IncrementalObjectSizeEstimator.createEstimators(e.keyClass, sizeEstimator);
            IncrementalObjectSizeEstimator.createEstimators(e.valueClass, sizeEstimator);}
          );
        initCache();
        LOG.debug("Local cache initialized in HiveMetaStoreClient: " + mscLocalCache);
      }
    }
  }

  public enum KeyType {
    PARTITIONS_BY_EXPR_REQUEST(PartitionsByExprRequest.class, PartitionsByExprResult.class);

    private final Class<?> keyClass;
    private final Class<?> valueClass;

    KeyType(Class<?> keyClass, Class<?> valueClass) {
      this.keyClass = keyClass;
      this.valueClass = valueClass;
    }
  }

  public static class CacheKey{
    KeyType IDENTIFIER;
    Object obj;

    public CacheKey(KeyType IDENTIFIER, Object obj) {
      this.IDENTIFIER = IDENTIFIER;
      this.obj = obj;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CacheKey cacheKey = (CacheKey) o;
      return IDENTIFIER == cacheKey.IDENTIFIER &&
              Objects.equals(obj, cacheKey.obj);
    }

    @Override
    public int hashCode() {
      return Objects.hash(IDENTIFIER, obj);
    }
  }


  private synchronized void initCache() {
    int maxSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.MSC_CACHE_MAX_SIZE);
    int initSize = 100;
    mscLocalCache = Caffeine.newBuilder()
            .initialCapacity(initSize)
            .maximumWeight(maxSize)
            .weigher((CacheKey key, Object val) -> {
              if (val instanceof Exception) return 0;
              ObjectEstimator keySizeEstimator = sizeEstimator.get(key.IDENTIFIER.keyClass);
              ObjectEstimator valSizeEstimator = sizeEstimator.get(key.IDENTIFIER.valueClass);
              int keySize = keySizeEstimator.estimate(key, sizeEstimator);
              int valSize = valSizeEstimator.estimate(val, sizeEstimator);
              LOG.debug("Cache entry weight - key: {}, value: {}, total: {}", keySize, valSize, keySize+valSize);
              return keySize + valSize;
            })
            .removalListener((key, val, cause) ->
                    LOG.debug(String.format("Caffeine - (%s, %s) was removed (%s)", key, val, cause)))
            .recordStats()
            .build(key -> {
              Object val;
              try {
                val = getResultObject(key);
              } catch (Exception e) {
                LOG.debug("Exception in MSC local cache: " + e.toString());
                if (e instanceof MetaException) {
                  val = new MetaException(e.getMessage());
                } else {
                  val = new Exception(e.getMessage());
                }
              }
              return val;
            });
    cacheInitialized = true;
  }


  protected Object getResultObject(CacheKey cacheKey) throws TException {
    Object result = null;

    switch (cacheKey.IDENTIFIER) {
      case PARTITIONS_BY_EXPR_REQUEST:
        result = super.getPartitionsByExprResult((PartitionsByExprRequest)cacheKey.obj);
        break;
      default:
        break;
    }

    return result;
  }

  @Override
  protected PartitionsByExprResult getPartitionsByExprResult(PartitionsByExprRequest req) throws TException {
    PartitionsByExprResult r = null;

    if (isCacheEnabled) {
      CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_BY_EXPR_REQUEST, req);
      Object val;
      try {
        val = mscLocalCache.get(cacheKey); // get either the result or an Exception

        if (val instanceof PartitionsByExprResult) {
          r = (PartitionsByExprResult) val;
          LOG.debug(mscLocalCache.toString().substring(mscLocalCache.toString().indexOf("LoadingCache")) + ": " +
                  mscLocalCache.stats().toString());
        } else if (val instanceof Exception) {
          mscLocalCache.invalidate(cacheKey);
          throw (Exception)val;
        }
      } catch (Exception e) {
        if (e instanceof MetaException) {
          throw new MetaException(e.getMessage());
        } else {
          throw new TException(e.getMessage());
        }
      }
    } else {
         r = client.get_partitions_by_expr(req);
    }

    return r;
  }

}
