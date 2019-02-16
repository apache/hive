/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProvider;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProviderFactory;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MOrder;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MType;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.datanucleus.AbstractNucleusContext;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ClassLoaderResolverImpl;
import org.datanucleus.NucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.store.scostore.Store;
import org.datanucleus.util.WeakValueMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.JDOCanRetryException;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.datastore.DataStoreCache;
import javax.sql.DataSource;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * This class is a wrapper class around PersistenceManagerFactory and its properties
 * These objects are static and need to be carefully modified together such that there are no
 * race-conditions when updating them. Additionally, this class provides thread-safe methods
 * to get PersistenceManager instances from the current PersistenceManagerFactory. The most
 * common usage of this class is to create a PersistenceManager from existing PersistenceManagerFactory
 * PersistenceManagerFactory properties are modified less often and hence the update pmf properties
 * can make use of read/write locks such that it is only blocking when current properties change.
 */
public class PersistenceManagerProvider {
  private static PersistenceManagerFactory pmf;
  private static Properties prop;
  private static final ReentrantReadWriteLock pmfLock = new ReentrantReadWriteLock();
  private static final Lock pmfReadLock = pmfLock.readLock();
  private static final Lock pmfWriteLock = pmfLock.writeLock();
  private static final Logger LOG = LoggerFactory.getLogger(PersistenceManagerProvider.class);
  private static final Map<String, Class<?>> PINCLASSMAP;
  private static boolean forTwoMetastoreTesting;
  private static int retryLimit;
  private static long retryInterval;

  static {
    Map<String, Class<?>> map = new HashMap<>();
    map.put("table", MTable.class);
    map.put("storagedescriptor", MStorageDescriptor.class);
    map.put("serdeinfo", MSerDeInfo.class);
    map.put("partition", MPartition.class);
    map.put("database", MDatabase.class);
    map.put("type", MType.class);
    map.put("fieldschema", MFieldSchema.class);
    map.put("order", MOrder.class);
    PINCLASSMAP = Collections.unmodifiableMap(map);
  }

  private PersistenceManagerProvider() {
    // prevent instantiation
  }

  private static final Set<Class<? extends Throwable>> retriableExceptionClasses =
      new HashSet<>(Arrays.asList(JDOCanRetryException.class));
  /**
   * Helper function for initialize to determine if we should retry an exception.
   * We return true if the exception is of a known type of retriable exceptions, or if one
   * of its recursive .getCause returns a known type of retriable exception.
   */
  private static boolean isRetriableException(Throwable e) {
    if (e == null){
      return false;
    }
    if (retriableExceptionClasses.contains(e.getClass())){
      return true;
    }
    for (Class<? extends Throwable> c : retriableExceptionClasses){
      if (c.isInstance(e)){
        return true;
      }
    }

    if (e.getCause() == null){
      return false;
    }
    return isRetriableException(e.getCause());
  }
  /**
   * This method updates the PersistenceManagerFactory and its properties if the given
   * configuration is different from its current set of properties. Most common case is that
   * the persistenceManagerFactory properties do not change, and hence this method is optimized to
   * be non-blocking in such cases. However, if the properties are different, this method blocks
   * other threads until the properties are updated, current pmf is closed and
   * a new pmf is re-initialized. Note that when a PersistenceManagerFactory is re-initialized all
   * the PersistenceManagers which are instantiated using old factory become invalid and will throw
   * JDOUserException. Hence it is recommended that this method is called in the setup/init phase
   * of the Metastore service when there are no other active threads serving clients.
   *
   * @param conf Configuration which provides the datanucleus/datasource properties for comparison
   */
  public static void updatePmfProperties(Configuration conf) {
    // take a read lock to check if the datasource properties changed.
    // Most common case is that datasource properties do not change
    Properties propsFromConf = PersistenceManagerProvider.getDataSourceProps(conf);
    pmfReadLock.lock();
    // keep track of if the read-lock is acquired by this thread
    // so that we can unlock it before leaving this method
    // this is needed because pmf methods below could throw JDOException (unchecked exception)
    // which can lead to readLock not being acquired at the end of the inner try-finally
    // block below
    boolean readLockAcquired = true;
    try {
      // if pmf properties change, need to update, release read lock and take write lock
      if (prop == null || pmf == null || !propsFromConf.equals(prop)) {
        pmfReadLock.unlock();
        readLockAcquired = false;
        pmfWriteLock.lock();
        try {
          // check if we need to update pmf again here in case some other thread already did it
          // for us after releasing readlock and before acquiring write lock above
          if (prop == null || pmf == null || !propsFromConf.equals(prop)) {
            // OK, now we really need to re-initialize pmf and pmf properties
            if (LOG.isInfoEnabled()) {
              LOG.info("Updating the pmf due to property change");
              if (prop == null) {
                LOG.info("Current pmf properties are uninitialized");
              } else {
                for (String key : prop.stringPropertyNames()) {
                  if (!key.equals(propsFromConf.get(key))) {
                     if (LOG.isDebugEnabled() && MetastoreConf.isPrintable(key)) {
                       // The jdbc connection url can contain sensitive information like username and password
                       // which should be masked out before logging.
                       String oldVal = prop.getProperty(key);
                       String newVal = propsFromConf.getProperty(key);
                       if (key.equals(ConfVars.CONNECT_URL_KEY.getVarname())) {
                         oldVal = MetaStoreServerUtils.anonymizeConnectionURL(oldVal);
                         newVal = MetaStoreServerUtils.anonymizeConnectionURL(newVal);
                       }
                       LOG.debug("Found {} to be different. Old val : {} : New Val : {}", key,
                           oldVal, newVal);
                     } else {
                      LOG.debug("Found masked property {} to be different", key);
                    }
                  }
                }
              }
            }
            if (pmf != null) {
              clearOutPmfClassLoaderCache();
              if (!forTwoMetastoreTesting) {
                // close the underlying connection pool to avoid leaks
                LOG.debug("Closing PersistenceManagerFactory");
                pmf.close();
                LOG.debug("PersistenceManagerFactory closed");
              }
              pmf = null;
            }
            // update the pmf properties object then initialize pmf using them
            prop = propsFromConf;
            retryLimit = MetastoreConf.getIntVar(conf, ConfVars.HMS_HANDLER_ATTEMPTS);
            retryInterval = MetastoreConf
                .getTimeVar(conf, ConfVars.HMS_HANDLER_INTERVAL, TimeUnit.MILLISECONDS);
            // init PMF with retry logic
            retry(() -> {initPMF(conf); return null;});
          }
          // downgrade by acquiring read lock before releasing write lock
          pmfReadLock.lock();
          readLockAcquired = true;
        } finally {
          pmfWriteLock.unlock();
        }
      }
    } finally {
      if (readLockAcquired) {
        pmfReadLock.unlock();
      }
    }
  }

  private static void initPMF(Configuration conf) {
    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);

    if (dsp == null) {
      pmf = JDOHelper.getPersistenceManagerFactory(prop);
    } else {
      try {
        DataSource ds = dsp.create(conf);
        Map<Object, Object> dsProperties = new HashMap<>();
        //Any preexisting datanucleus property should be passed along
        dsProperties.putAll(prop);
        dsProperties.put(PropertyNames.PROPERTY_CONNECTION_FACTORY, ds);
        dsProperties.put(PropertyNames.PROPERTY_CONNECTION_FACTORY2, ds);
        dsProperties.put(ConfVars.MANAGER_FACTORY_CLASS.getVarname(),
            "org.datanucleus.api.jdo.JDOPersistenceManagerFactory");
        pmf = JDOHelper.getPersistenceManagerFactory(dsProperties);
      } catch (SQLException e) {
        LOG.warn("Could not create PersistenceManagerFactory using "
            + "connection pool properties, will fall back", e);
        pmf = JDOHelper.getPersistenceManagerFactory(prop);
      }
    }
    DataStoreCache dsc = pmf.getDataStoreCache();
    if (dsc != null) {
      String objTypes = MetastoreConf.getVar(conf, ConfVars.CACHE_PINOBJTYPES);
      LOG.info(
          "Setting MetaStore object pin classes with hive.metastore.cache.pinobjtypes=\"{}\"",
          objTypes);
      if (org.apache.commons.lang.StringUtils.isNotEmpty(objTypes)) {
        String[] typeTokens = objTypes.toLowerCase().split(",");
        for (String type : typeTokens) {
          type = type.trim();
          if (PINCLASSMAP.containsKey(type)) {
            dsc.pinAll(true, PINCLASSMAP.get(type));
          } else {
            LOG.warn("{} is not one of the pinnable object types: {}", type,
                org.apache.commons.lang.StringUtils.join(PINCLASSMAP.keySet(), " "));
          }
        }
      }
    } else {
      LOG.warn("PersistenceManagerFactory returned null DataStoreCache object. "
          + "Unable to initialize object pin types defined by hive.metastore.cache.pinobjtypes");
    }
  }

  /**
   * Removed cached classloaders from DataNucleus
   * DataNucleus caches classloaders in NucleusContext.
   * In UDFs, this can result in classloaders not getting GCed resulting in PermGen leaks.
   * This is particularly an issue when using embedded metastore with HiveServer2,
   * since the current classloader gets modified with each new add jar,
   * becoming the classloader for downstream classes, which DataNucleus ends up using.
   * The NucleusContext cache gets freed up only on calling a close on it.
   * We're not closing NucleusContext since it does a bunch of other things which we don't want.
   * We're not clearing the cache HashMap by calling HashMap#clear to avoid concurrency issues.
   */
  public static void clearOutPmfClassLoaderCache() {
    pmfWriteLock.lock();
    try {
      if ((pmf == null) || (!(pmf instanceof JDOPersistenceManagerFactory))) {
        return;
      }
      // NOTE : This is hacky, and this section of code is fragile depending on DN code varnames
      // so it's likely to stop working at some time in the future, especially if we upgrade DN
      // versions, so we actively need to find a better way to make sure the leak doesn't happen
      // instead of just clearing out the cache after every call.
      JDOPersistenceManagerFactory jdoPmf = (JDOPersistenceManagerFactory) pmf;
      NucleusContext nc = jdoPmf.getNucleusContext();
      try {
        Field pmCache = pmf.getClass().getDeclaredField("pmCache");
        pmCache.setAccessible(true);
        Set<JDOPersistenceManager> pmSet = (Set<JDOPersistenceManager>) pmCache.get(pmf);
        for (JDOPersistenceManager pm : pmSet) {
          org.datanucleus.ExecutionContext ec = pm.getExecutionContext();
          if (ec instanceof org.datanucleus.ExecutionContextThreadedImpl) {
            ClassLoaderResolver clr =
                ((org.datanucleus.ExecutionContextThreadedImpl) ec).getClassLoaderResolver();
            clearClr(clr);
          }
        }
        org.datanucleus.plugin.PluginManager pluginManager =
            jdoPmf.getNucleusContext().getPluginManager();
        Field registryField = pluginManager.getClass().getDeclaredField("registry");
        registryField.setAccessible(true);
        org.datanucleus.plugin.PluginRegistry registry =
            (org.datanucleus.plugin.PluginRegistry) registryField.get(pluginManager);
        if (registry instanceof org.datanucleus.plugin.NonManagedPluginRegistry) {
          org.datanucleus.plugin.NonManagedPluginRegistry nRegistry =
              (org.datanucleus.plugin.NonManagedPluginRegistry) registry;
          Field clrField = nRegistry.getClass().getDeclaredField("clr");
          clrField.setAccessible(true);
          ClassLoaderResolver clr = (ClassLoaderResolver) clrField.get(nRegistry);
          clearClr(clr);
        }
        if (nc instanceof org.datanucleus.PersistenceNucleusContextImpl) {
          org.datanucleus.PersistenceNucleusContextImpl pnc =
              (org.datanucleus.PersistenceNucleusContextImpl) nc;
          org.datanucleus.store.types.TypeManagerImpl tm =
              (org.datanucleus.store.types.TypeManagerImpl) pnc.getTypeManager();
          Field clrField = tm.getClass().getDeclaredField("clr");
          clrField.setAccessible(true);
          ClassLoaderResolver clr = (ClassLoaderResolver) clrField.get(tm);
          clearClr(clr);
          Field storeMgrField = pnc.getClass().getDeclaredField("storeMgr");
          storeMgrField.setAccessible(true);
          org.datanucleus.store.rdbms.RDBMSStoreManager storeMgr =
              (org.datanucleus.store.rdbms.RDBMSStoreManager) storeMgrField.get(pnc);
          Field backingStoreField =
              storeMgr.getClass().getDeclaredField("backingStoreByMemberName");
          backingStoreField.setAccessible(true);
          Map<String, Store> backingStoreByMemberName =
              (Map<String, Store>) backingStoreField.get(storeMgr);
          for (Store store : backingStoreByMemberName.values()) {
            org.datanucleus.store.rdbms.scostore.BaseContainerStore baseStore =
                (org.datanucleus.store.rdbms.scostore.BaseContainerStore) store;
            clrField = org.datanucleus.store.rdbms.scostore.BaseContainerStore.class
                .getDeclaredField("clr");
            clrField.setAccessible(true);
            clr = (ClassLoaderResolver) clrField.get(baseStore);
            clearClr(clr);
          }
        }
        Field classLoaderResolverMap =
            AbstractNucleusContext.class.getDeclaredField("classLoaderResolverMap");
        classLoaderResolverMap.setAccessible(true);
        Map<String, ClassLoaderResolver> loaderMap =
            (Map<String, ClassLoaderResolver>) classLoaderResolverMap.get(nc);
        for (ClassLoaderResolver clr : loaderMap.values()) {
          clearClr(clr);
        }
        classLoaderResolverMap.set(nc, new HashMap<String, ClassLoaderResolver>());
        LOG.debug("Removed cached classloaders from DataNucleus NucleusContext");
      } catch (Exception e) {
        LOG.warn("Failed to remove cached classloaders from DataNucleus NucleusContext", e);
      }
    } finally {
      pmfWriteLock.unlock();
    }
  }

  private static void clearClr(ClassLoaderResolver clr) throws Exception {
    if (clr != null) {
      if (clr instanceof ClassLoaderResolverImpl) {
        ClassLoaderResolverImpl clri = (ClassLoaderResolverImpl) clr;
        long resourcesCleared = clearFieldMap(clri, "resources");
        long loadedClassesCleared = clearFieldMap(clri, "loadedClasses");
        long unloadedClassesCleared = clearFieldMap(clri, "unloadedClasses");
        LOG.debug("Cleared ClassLoaderResolverImpl: {}, {}, {}", resourcesCleared,
            loadedClassesCleared, unloadedClassesCleared);
      }
    }
  }

  private static long clearFieldMap(ClassLoaderResolverImpl clri, String mapFieldName)
      throws Exception {
    Field mapField = ClassLoaderResolverImpl.class.getDeclaredField(mapFieldName);
    mapField.setAccessible(true);

    Map<String, Class> map = (Map<String, Class>) mapField.get(clri);
    long sz = map.size();
    mapField.set(clri, Collections.synchronizedMap(new WeakValueMap()));
    return sz;
  }

  /**
   * creates a PersistenceManager instance for the current PersistenceManagerFactory. Note that this
   * acquires a read-lock on PersistenceManagerFactory so that this method will block if any other
   * thread is actively, (re-)initializing PersistenceManagerFactory when this method is called
   * Note that this method throws a RuntimeException, if PersistenceManagerFactory is not yet initialized.
   *
   * @return PersistenceManager from the current PersistenceManagerFactory instance
   */
  public static PersistenceManager getPersistenceManager() {
    pmfReadLock.lock();
    try {
      if (pmf == null) {
        throw new RuntimeException(
            "Cannot create PersistenceManager. PersistenceManagerFactory is not yet initialized");
      }
      return retry(pmf::getPersistenceManager);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      pmfReadLock.unlock();
    }
  }

  /**
   * Properties specified in hive-default.xml override the properties specified
   * in jpox.properties.
   */
  @SuppressWarnings("nls")
  private static Properties getDataSourceProps(Configuration conf) {
    Properties prop = new Properties();
    correctAutoStartMechanism(conf);

    // First, go through and set all our values for datanucleus and javax.jdo parameters.  This
    // has to be a separate first step because we don't set the default values in the config object.
    for (ConfVars var : MetastoreConf.dataNucleusAndJdoConfs) {
      String confVal = MetastoreConf.getAsString(conf, var);
      String varName = var.getVarname();
      Object prevVal = prop.setProperty(varName, confVal);
      if (MetastoreConf.isPrintable(varName)) {
        LOG.debug("Overriding {} value {} from jpox.properties with {}", varName, prevVal, confVal);
      }
    }

    // Now, we need to look for any values that the user set that MetastoreConf doesn't know about.
    // TODO Commenting this out for now, as it breaks because the conf values aren't getting properly
    // interpolated in case of variables.  See HIVE-17788.
    /*
    for (Map.Entry<String, String> e : conf) {
      if (e.getKey().startsWith("datanucleus.") || e.getKey().startsWith("javax.jdo.")) {
        // We have to handle this differently depending on whether it is a value known to
        // MetastoreConf or not.  If it is, we need to get the default value if a value isn't
        // provided.  If not, we just set whatever the user has set.
        Object prevVal = prop.setProperty(e.getKey(), e.getValue());
        if (LOG.isDebugEnabled() && MetastoreConf.isPrintable(e.getKey())) {
          LOG.debug("Overriding " + e.getKey() + " value " + prevVal
              + " from  jpox.properties with " + e.getValue());
        }
      }
    }
    */

    // Password may no longer be in the conf, use getPassword()
    try {
      String passwd = MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.PWD);
      if (org.apache.commons.lang.StringUtils.isNotEmpty(passwd)) {
        // We can get away with the use of varname here because varname == hiveName for PWD
        prop.setProperty(ConfVars.PWD.getVarname(), passwd);
      }
    } catch (IOException err) {
      throw new RuntimeException("Error getting metastore password: " + err.getMessage(), err);
    }

    if (LOG.isDebugEnabled()) {
      for (Entry<Object, Object> e : prop.entrySet()) {
        if (MetastoreConf.isPrintable(e.getKey().toString())) {
          LOG.debug("{} = {}", e.getKey(), e.getValue());
        }
      }
    }

    return prop;
  }

  /**
   * Update conf to set datanucleus.autoStartMechanismMode=ignored.
   * This is necessary to able to use older version of hive against
   * an upgraded but compatible metastore schema in db from new version
   * of hive
   *
   * @param conf
   */
  private static void correctAutoStartMechanism(Configuration conf) {
    final String autoStartKey = "datanucleus.autoStartMechanismMode";
    final String autoStartIgnore = "ignored";
    String currentAutoStartVal = conf.get(autoStartKey);
    if (!autoStartIgnore.equalsIgnoreCase(currentAutoStartVal)) {
      LOG.warn("{} is set to unsupported value {} . Setting it to value: {}", autoStartKey,
          conf.get(autoStartKey), autoStartIgnore);
    }
    conf.set(autoStartKey, autoStartIgnore);
  }

  /**
   * To make possible to run multiple metastore in unit test
   *
   * @param twoMetastoreTesting if we are using multiple metastore in unit test
   */
  @VisibleForTesting
  public static void setTwoMetastoreTesting(boolean twoMetastoreTesting) {
    forTwoMetastoreTesting = twoMetastoreTesting;
  }

  public static String getProperty(String key) {
    return prop == null ? null : prop.getProperty(key);
  }

  private static <T> T retry(Supplier<T> s) {
    Exception ex = null;
    int myRetryLimit = retryLimit;
    while (myRetryLimit > 0) {
      try {
        return s.get();
      } catch (Exception e) {
        myRetryLimit--;
        boolean retriable = isRetriableException(e);
        if (myRetryLimit > 0 && retriable) {
          LOG.info("Retriable exception while invoking method, retrying. {} attempts left",
              myRetryLimit, e);
          try {
            Thread.sleep(retryInterval);
          } catch (InterruptedException ie) {
            // Restore the interrupted status, since we do not want to catch it.
            LOG.debug("Interrupted while sleeping before retrying.", ie);
            Thread.currentThread().interrupt();
          }
          // If we're here, we'll proceed down the next while loop iteration.
        } else {
          // we've reached our limit, throw the last one.
          if (retriable) {
            LOG.warn("Exception retry limit reached, not retrying any longer.", e);
          } else {
            LOG.debug("Non-retriable exception.", e);
          }
          ex = e;
        }
      }
    }
    throw new RuntimeException(ex);
  }
}
