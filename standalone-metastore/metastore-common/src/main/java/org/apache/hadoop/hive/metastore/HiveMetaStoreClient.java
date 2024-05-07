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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.convertToGetPartitionsByNamesRequest;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.createThriftPartitionsReq;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependCatalogToDbName;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.login.LoginException;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.hooks.URIResolverHook;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.utils.FilterUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

/**
 * Hive Metastore Client.
 * The public implementation of IMetaStoreClient. Methods not inherited from IMetaStoreClient
 * are not public and can change. Hence this is marked as unstable.
 * For users who require retry mechanism when the connection between metastore and client is
 * broken, RetryingMetaStoreClient class should be used.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HiveMetaStoreClient implements IMetaStoreClient, AutoCloseable {

  private final String CLASS_NAME = HiveMetaStoreClient.class.getName();

  public static final String MANUALLY_INITIATED_COMPACTION = "manual";
  public static final String TRUNCATE_SKIP_DATA_DELETION = "truncateSkipDataDeletion";
  public static final String SKIP_DROP_PARTITION = "dropPartitionSkip";

  public static final String SNAPSHOT_REF = "snapshot_ref";
  public static final String RENAME_PARTITION_MAKE_COPY = "renamePartitionMakeCopy";

  /**
   * Capabilities of the current client. If this client talks to a MetaStore server in a manner
   * implying the usage of some expanded features that require client-side support that this client
   * doesn't have (e.g. a getting a table of a new type), it will get back failures when the
   * capability checking is enabled (the default).
   */
  public final static ClientCapabilities VERSION = new ClientCapabilities(
      Lists.newArrayList(ClientCapability.INSERT_ONLY_TABLES));
  // Test capability for tests.
  public final static ClientCapabilities TEST_VERSION = new ClientCapabilities(
      Lists.newArrayList(ClientCapability.INSERT_ONLY_TABLES, ClientCapability.TEST_CAPABILITY));

  // Name of the HiveMetaStore class. It is used to initialize embedded metastore
  private static final String HIVE_METASTORE_CLASS =
      "org.apache.hadoop.hive.metastore.HiveMetaStore";

  // Method used to create Hive Metastore client. It is called as
  // HiveMetaStore.newHMSHandler("hive client", this.conf, true);
  private static final String HIVE_METASTORE_CREATE_HANDLER_METHOD = "newHMSHandler";

  ThriftHiveMetastore.Iface client = null;
  private TTransport transport = null;
  private boolean isConnected = false;
  private URI metastoreUris[];
  private final HiveMetaHookLoader hookLoader;
  protected final Configuration conf;  // Keep a copy of HiveConf so if Session conf changes, we may need to get a new HMS client.
  private String tokenStrForm;
  private final boolean localMetaStore;
  private final MetaStoreFilterHook filterHook;
  private final boolean isClientFilterEnabled;
  private final URIResolverHook uriResolverHook;
  private final int fileMetadataBatchSize;

  private Map<String, String> currentMetaVars;

  private static final AtomicInteger connCount = new AtomicInteger(0);

  // for thrift connects
  private int retries = 5;
  private long retryDelaySeconds = 0;
  private final ClientCapabilities version;
  private static String[] processorCapabilities;
  private static String processorIdentifier;

  //copied from ErrorMsg.java
  private static final String REPL_EVENTS_MISSING_IN_METASTORE = "Notification events are missing in the meta store.";

  private static final String REPL_EVENTS_WITH_DUPLICATE_ID_IN_METASTORE =
          "Notification events with duplicate event ids in the meta store.";

  static final protected Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClient.class);

  public HiveMetaStoreClient(Configuration conf) throws MetaException {
    this(conf, null, true);
  }

  public HiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
    this(conf, hookLoader, true);
  }

  public HiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded)
    throws MetaException {

    this.hookLoader = hookLoader;
    if (conf == null) {
      conf = MetastoreConf.newMetastoreConf();
      this.conf = conf;
    } else {
      this.conf = new Configuration(conf);
    }
    version = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) ? TEST_VERSION : VERSION;
    filterHook = loadFilterHooks();
    isClientFilterEnabled = getIfClientFilterEnabled();
    uriResolverHook = loadUriResolverHook();
    fileMetadataBatchSize = MetastoreConf.getIntVar(
        conf, ConfVars.BATCH_RETRIEVE_OBJECTS_MAX);

    if ((MetastoreConf.get(conf, "hive.metastore.client.capabilities")) != null) {
      String[] capabilities = MetastoreConf.get(conf, "hive.metastore.client.capabilities").split(",");
      setProcessorCapabilities(capabilities);
      String hostName = "unknown";
      try {
        hostName = InetAddress.getLocalHost().getCanonicalHostName();
      } catch (UnknownHostException ue) {
      }
      setProcessorIdentifier("HMSClient-" + "@" + hostName);
    }

    String msUri = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
    localMetaStore = MetastoreConf.isEmbeddedMetaStore(msUri);
    if (localMetaStore) {
      if (!allowEmbedded) {
        throw new MetaException("Embedded metastore is not allowed here. Please configure "
            + ConfVars.THRIFT_URIS.toString() + "; it is currently set to [" + msUri + "]");
      }

      client = callEmbeddedMetastore(this.conf);

      // instantiate the metastore server handler directly instead of connecting
      // through the network
      isConnected = true;
      snapshotActiveConf();
      return;
    }

    // get the number retries
    retries = MetastoreConf.getIntVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES);
    retryDelaySeconds = MetastoreConf.getTimeVar(conf,
        ConfVars.CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);

    // user wants file store based configuration
    if (MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS) != null) {
      resolveUris();
    } else {
      LOG.error("NOT getting uris from conf");
      throw new MetaException("MetaStoreURIs not found in conf file");
    }

    //If HADOOP_PROXY_USER is set in env or property,
    //then need to create metastore client that proxies as that user.
    String HADOOP_PROXY_USER = "HADOOP_PROXY_USER";
    String proxyUser = System.getenv(HADOOP_PROXY_USER);
    if (proxyUser == null) {
      proxyUser = System.getProperty(HADOOP_PROXY_USER);
    }
    //if HADOOP_PROXY_USER is set, create DelegationToken using real user
    if (proxyUser != null) {
      LOG.info(HADOOP_PROXY_USER + " is set. Using delegation "
          + "token for HiveMetaStore connection.");
      try {
        UserGroupInformation.getLoginUser().getRealUser().doAs(
            new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                open();
                return null;
              }
            });
        String delegationTokenPropString = "DelegationTokenForHiveMetaStoreServer";
        String delegationTokenStr = getDelegationToken(proxyUser, proxyUser);
        SecurityUtils.setTokenStr(UserGroupInformation.getCurrentUser(), delegationTokenStr,
            delegationTokenPropString);
        MetastoreConf.setVar(this.conf, ConfVars.TOKEN_SIGNATURE, delegationTokenPropString);
        close();
      } catch (Exception e) {
        LOG.error("Error while setting delegation token for " + proxyUser, e);
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw new MetaException(e.getMessage());
        }
      }
    }
    // finally open the store
    open();
  }

  /**
   * Instantiate the metastore server handler directly instead of connecting
   * through the network
   *
   * @param conf Configuration object passed to embedded metastore
   * @return embedded client instance
   * @throws MetaException
   */
  static ThriftHiveMetastore.Iface callEmbeddedMetastore(Configuration conf) throws MetaException {
    // Instantiate the metastore server handler directly instead of connecting
    // through the network
    //
    // The code below simulates the following code
    //
    // client = HiveMetaStore.newHMSHandler(this.conf);
    //
    // using reflection API. This is done to avoid dependency of MetastoreClient on Hive Metastore.
    // Note that newHMSHandler is static method, so we pass null as the object reference.
    //
    try {
      Class<?> clazz = Class.forName(HIVE_METASTORE_CLASS);
      //noinspection JavaReflectionMemberAccess
      Method method = clazz.getDeclaredMethod(HIVE_METASTORE_CREATE_HANDLER_METHOD,
          Configuration.class);
      method.setAccessible(true);
      return (ThriftHiveMetastore.Iface) method.invoke(null, conf);
    } catch (InvocationTargetException e) {
      if (e.getCause() != null) {
        MetaStoreUtils.throwMetaException((Exception) e.getCause());
      }
      MetaStoreUtils.throwMetaException(e);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  private boolean getIfClientFilterEnabled() {
    boolean isEnabled = MetastoreConf.getBoolVar(conf, ConfVars.METASTORE_CLIENT_FILTER_ENABLED);
    LOG.info("HMS client filtering is " + (isEnabled ? "enabled." : "disabled."));

    return isEnabled;
  }

  private void resolveUris() throws MetaException {
    String thriftUris = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
    String serviceDiscoveryMode = MetastoreConf.getVar(conf, ConfVars.THRIFT_SERVICE_DISCOVERY_MODE);
    List<String> metastoreUrisString = null;

    // The metastore URIs can come from THRIFT_URIS directly or need to be fetched from the
    // Zookeeper
    try {
      if (serviceDiscoveryMode == null || serviceDiscoveryMode.trim().isEmpty()) {
        metastoreUrisString = Arrays.asList(thriftUris.split(","));
      } else if (serviceDiscoveryMode.equalsIgnoreCase("zookeeper")) {
        metastoreUrisString = new ArrayList<String>();
        // Add scheme to the bare URI we get.
        for (String s : MetastoreConf.getZKConfig(conf).getServerUris()) {
          metastoreUrisString.add("thrift://" + s);
        }
      } else {
        throw new IllegalArgumentException("Invalid metastore dynamic service discovery mode " +
                serviceDiscoveryMode);
      }
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }

    if (metastoreUrisString.isEmpty() && "zookeeper".equalsIgnoreCase(serviceDiscoveryMode)) {
      throw new MetaException("No metastore service discovered in ZooKeeper. "
          + "Please ensure that at least one metastore server is online");
    }

    LOG.info("Resolved metastore uris: {}", metastoreUrisString);

    List<URI> metastoreURIArray = new ArrayList<URI>();
    try {
      for (String s : metastoreUrisString) {
        URI tmpUri = new URI(s);
        if (tmpUri.getScheme() == null) {
          throw new IllegalArgumentException("URI: " + s
                  + " does not have a scheme");
        }
        if (uriResolverHook != null) {
          metastoreURIArray.addAll(uriResolverHook.resolveURI(tmpUri));
        } else {
          metastoreURIArray.add(tmpUri);
        }
      }
      metastoreUris = new URI[metastoreURIArray.size()];
      for (int j = 0; j < metastoreURIArray.size(); j++) {
        metastoreUris[j] = metastoreURIArray.get(j);
      }

      if (MetastoreConf.getVar(conf, ConfVars.THRIFT_URI_SELECTION).equalsIgnoreCase("RANDOM")) {
        List<URI> uriList = Arrays.asList(metastoreUris);
        Collections.shuffle(uriList);
        metastoreUris = uriList.toArray(new URI[uriList.size()]);
      }
    } catch (IllegalArgumentException e) {
      throw (e);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
  }


  private MetaStoreFilterHook loadFilterHooks() throws IllegalStateException {
    Class<? extends MetaStoreFilterHook> authProviderClass = MetastoreConf.
        getClass(conf, ConfVars.FILTER_HOOK, DefaultMetaStoreFilterHookImpl.class,
            MetaStoreFilterHook.class);
    String msg = "Unable to create instance of " + authProviderClass.getName() + ": ";
    try {
      Constructor<? extends MetaStoreFilterHook> constructor =
          authProviderClass.getConstructor(Configuration.class);
      return constructor.newInstance(conf);
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InstantiationException | IllegalArgumentException | InvocationTargetException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    }
  }

  //multiple clients may initialize the hook at the same time
  synchronized private URIResolverHook loadUriResolverHook() throws IllegalStateException {

    String uriResolverClassName =
            MetastoreConf.getAsString(conf, ConfVars.URI_RESOLVER);
    if (uriResolverClassName.equals("")) {
      return null;
    } else {
      LOG.info("Loading uri resolver : " + uriResolverClassName);
      try {
        Class<?> uriResolverClass = Class.forName(uriResolverClassName, true,
                JavaUtils.getClassLoader());
        return (URIResolverHook) ReflectionUtils.newInstance(uriResolverClass, null);
      } catch (Exception e) {
        LOG.error("Exception loading uri resolver hook", e);
        return null;
      }
    }
  }

  /**
   * Swaps the first element of the metastoreUris array with a random element from the
   * remainder of the array.
   */
  private void promoteRandomMetaStoreURI() {
    if (metastoreUris.length <= 1) {
      return;
    }
    Random rng = new Random();
    int index = rng.nextInt(metastoreUris.length - 1) + 1;
    URI tmp = metastoreUris[0];
    metastoreUris[0] = metastoreUris[index];
    metastoreUris[index] = tmp;
  }

  @VisibleForTesting
  public TTransport getTTransport() {
    return transport;
  }

  @Override
  public boolean isLocalMetaStore() {
    return localMetaStore;
  }

  @Override
  public boolean isCompatibleWith(Configuration conf) {
    // Make a copy of currentMetaVars, there is a race condition that
    // currentMetaVars might be changed during the execution of the method
    Map<String, String> currentMetaVarsCopy = currentMetaVars;
    if (currentMetaVarsCopy == null) {
      return false; // recreate
    }
    boolean compatible = true;
    for (ConfVars oneVar : MetastoreConf.metaVars) {
      // Since metaVars are all of different types, use string for comparison
      String oldVar = currentMetaVarsCopy.get(oneVar.getVarname());
      String newVar = MetastoreConf.getAsString(conf, oneVar);
      if (oldVar == null ||
          (oneVar.isCaseSensitive() ? !oldVar.equals(newVar) : !oldVar.equalsIgnoreCase(newVar))) {
        LOG.info("Mestastore configuration {} changed from {} to {}",
            oneVar, oldVar, newVar);
        compatible = false;
      }
    }
    return compatible;
  }

  @Override
  public void setHiveAddedJars(String addedJars) {
    MetastoreConf.setVar(conf, ConfVars.ADDED_JARS, addedJars);
  }

  @Override
  public void reconnect() throws MetaException {
    if (localMetaStore) {
      // For direct DB connections we don't yet support reestablishing connections.
      throw new MetaException("Retries for direct MetaStore DB connections "
          + "are not supported by this client");
    } else {
      close();

      if (uriResolverHook != null) {
        //for dynamic uris, re-lookup if there are new metastore locations
        resolveUris();
      }

      if (MetastoreConf.getVar(conf, ConfVars.THRIFT_URI_SELECTION).equalsIgnoreCase("RANDOM")) {
        // Swap the first element of the metastoreUris[] with a random element from the rest
        // of the array. Rationale being that this method will generally be called when the default
        // connection has died and the default connection is likely to be the first array element.
        promoteRandomMetaStoreURI();
      }
      open();
    }
  }

  @Override
  public void alter_table(String dbname, String tbl_name, Table new_tbl) throws TException {
    alter_table_with_environmentContext(dbname, tbl_name, new_tbl, null);
  }

  @Override
  public void alter_table(String defaultDatabaseName, String tblName, Table table,
                          boolean cascade) throws TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    if (cascade) {
      environmentContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
    }
    alter_table_with_environmentContext(defaultDatabaseName, tblName, table, environmentContext);
  }

  @Override
  public void alter_table_with_environmentContext(String dbname, String tbl_name, Table new_tbl,
      EnvironmentContext envContext) throws InvalidOperationException, MetaException, TException {
    HiveMetaHook hook = getHook(new_tbl);
    if (hook != null) {
      hook.preAlterTable(new_tbl, envContext);
    }
    AlterTableRequest req = new AlterTableRequest(dbname, tbl_name, new_tbl);
    req.setCatName(MetaStoreUtils.getDefaultCatalog(conf));
    req.setEnvironmentContext(envContext);
    if (processorCapabilities != null) {
      req.setProcessorCapabilities(new ArrayList<String>(Arrays.asList(processorCapabilities)));
      req.setProcessorIdentifier(processorIdentifier);
    }
    boolean success = false;
    try {
      client.alter_table_req(req);
      if (hook != null) {
        hook.commitAlterTable(new_tbl, envContext);
      }
      success = true;
    } finally {
      if (!success && (hook != null)) {
        hook.rollbackAlterTable(new_tbl, envContext);
      }
    }
  }

  @Override
  public void alter_table(String catName, String dbName, String tblName, Table newTable,
                         EnvironmentContext envContext) throws TException {
    // This never used to call the hook. Why? There's overload madness in metastore...
    AlterTableRequest req = new AlterTableRequest(dbName, tblName, newTable);
    req.setCatName(catName);
    req.setEnvironmentContext(envContext);
    if (processorCapabilities != null) {
      req.setProcessorCapabilities(new ArrayList<String>(Arrays.asList(processorCapabilities)));
      req.setProcessorIdentifier(processorIdentifier);
    }
    client.alter_table_req(req);
  }

  @Override
  public void alter_table(String catName, String dbName, String tbl_name, Table new_tbl,
      EnvironmentContext envContext, String validWriteIds)
          throws InvalidOperationException, MetaException, TException {
    HiveMetaHook hook = getHook(new_tbl);
    if (hook != null) {
      hook.preAlterTable(new_tbl, envContext);
    }
    boolean success = false;
    try {
      boolean skipAlter = envContext != null && envContext.getProperties() != null &&
              Boolean.valueOf(envContext.getProperties().getOrDefault(HiveMetaHook.SKIP_METASTORE_ALTER, "false"));
      if (!skipAlter) {
        AlterTableRequest req = new AlterTableRequest(dbName, tbl_name, new_tbl);
        req.setCatName(catName);
        req.setValidWriteIdList(validWriteIds);
        req.setEnvironmentContext(envContext);
        if (processorCapabilities != null) {
          req.setProcessorCapabilities(new ArrayList<String>(Arrays.asList(processorCapabilities)));
          req.setProcessorIdentifier(processorIdentifier);
        }

        client.alter_table_req(req);
      }

      if (hook != null) {
        hook.commitAlterTable(new_tbl, envContext);
      }
      success = true;
    } finally {
      if (!success && (hook != null)) {
        hook.rollbackAlterTable(new_tbl, envContext);
      }
    }
  }

  @Deprecated
  @Override
  public void renamePartition(final String dbname, final String tableName, final List<String> part_vals,
                              final Partition newPart) throws TException {
    renamePartition(getDefaultCatalog(conf), dbname, tableName, part_vals, newPart, null);
  }

  @Override
  public void renamePartition(String catName, String dbname, String tableName, List<String> part_vals,
                              Partition newPart, String validWriteIds, long txnId, boolean makeCopy) throws TException {
    RenamePartitionRequest req = new RenamePartitionRequest(dbname, tableName, part_vals, newPart);
    req.setCatName(catName);
    req.setValidWriteIdList(validWriteIds);
    req.setTxnId(txnId);
    req.setClonePart(makeCopy);
    client.rename_partition_req(req);
  }

  private <T extends TTransport> T configureThriftMaxMessageSize(T transport) {
    int maxThriftMessageSize = (int) MetastoreConf.getSizeVar(
            conf, ConfVars.THRIFT_METASTORE_CLIENT_MAX_MESSAGE_SIZE);
    if (maxThriftMessageSize > 0) {
      if (transport.getConfiguration() == null) {
        LOG.warn("TTransport {} is returning a null Configuration, Thrift max message size is not getting configured",
            transport.getClass().getName());
        return transport;
      }
      transport.getConfiguration().setMaxMessageSize(maxThriftMessageSize);
    }
    return transport;
  }

  private Map<String, String> getAdditionalHeaders() {
    Map<String, String> headers = new HashMap<>();
    String keyValuePairs = MetastoreConf.getVar(conf,
        ConfVars.METASTORE_CLIENT_ADDITIONAL_HEADERS);
    try {
      String[] headerKeyValues = keyValuePairs.split(",");
      for (String header : headerKeyValues) {
        String[] parts = header.split("=");
        headers.put(parts[0].trim(), parts[1].trim());
      }
    } catch (Exception ex) {
      LOG.warn("Could not parse the headers provided in "
          + ConfVars.METASTORE_CLIENT_ADDITIONAL_HEADERS, ex);
    }
    return headers;
  }

  /*
  Creates a THttpClient if HTTP mode is enabled. If Client auth mode is set to JWT,
  then the method fetches JWT from environment variable: HMS_JWT and sets in auth
  header in http request
   */
  private THttpClient createHttpClient(URI store, boolean useSSL) throws MetaException,
      TTransportException {
    String path = MetaStoreUtils.getHttpPath(MetastoreConf.getVar(conf, ConfVars.THRIFT_HTTP_PATH));
    String urlScheme;
    if (useSSL || Objects.equals(store.getScheme(), "https")) {
      urlScheme = "https://";
    } else {
      urlScheme = "http://";
    }
    String httpUrl = urlScheme + store.getHost() + ":" + store.getPort() + path;

    HttpClientBuilder httpClientBuilder = createHttpClientBuilder();
    THttpClient tHttpClient;
    try {
      if (useSSL) {
        String trustStorePath = MetastoreConf.getVar(conf, ConfVars.SSL_TRUSTSTORE_PATH).trim();
        if (trustStorePath.isEmpty()) {
          throw new IllegalArgumentException(ConfVars.SSL_TRUSTSTORE_PATH + " Not configured for SSL connection");
        }
        String trustStorePassword = MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.SSL_TRUSTSTORE_PASSWORD);
        String trustStoreType = MetastoreConf.getVar(conf, ConfVars.SSL_TRUSTSTORE_TYPE).trim();
        String trustStoreAlgorithm = MetastoreConf.getVar(conf, ConfVars.SSL_TRUSTMANAGERFACTORY_ALGORITHM).trim();
        tHttpClient =
            SecurityUtils.getThriftHttpsClient(httpUrl, trustStorePath, trustStorePassword, trustStoreAlgorithm,
                trustStoreType, httpClientBuilder);
      } else {
        tHttpClient = new THttpClient(httpUrl, httpClientBuilder.build());
      }
    } catch (Exception e) {
      if (e instanceof TTransportException) {
        throw (TTransportException) e;
      } else {
        throw new MetaException("Failed to create http transport client to url: " + httpUrl + ". Error:" + e);
      }
    }
    LOG.debug("Created thrift http client for URL: " + httpUrl);
    return configureThriftMaxMessageSize(tHttpClient);
  }

  @VisibleForTesting
  protected HttpClientBuilder createHttpClientBuilder() throws MetaException {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    String authType = MetastoreConf.getVar(conf, ConfVars.METASTORE_CLIENT_AUTH_MODE);
    Map<String, String> additionalHeaders = getAdditionalHeaders();
    if (authType.equalsIgnoreCase("jwt")) {
      // fetch JWT token from environment and set it in Auth Header in HTTP request
      String jwtToken = System.getenv("HMS_JWT");
      if (jwtToken == null || jwtToken.isEmpty()) {
        LOG.debug("No jwt token set in environment variable: HMS_JWT");
        throw new MetaException("For auth mode JWT, valid signed jwt token must be provided in the "
            + "environment variable HMS_JWT");
      }
      httpClientBuilder.addInterceptorFirst(new HttpRequestInterceptor() {
        @Override
        public void process(HttpRequest httpRequest, HttpContext httpContext)
            throws HttpException, IOException {
          httpRequest.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + jwtToken);
          for (Map.Entry<String, String> entry : additionalHeaders.entrySet()) {
            httpRequest.addHeader(entry.getKey(), entry.getValue());
          }
        }
      });
    } else {
      String user = MetastoreConf.getVar(conf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME);
      if (user == null || user.equals("")) {
        try {
          user = UserGroupInformation.getCurrentUser().getShortUserName();
        } catch (IOException e) {
          throw new MetaException("Failed to get client username from UGI");
        }
      }
      final String httpUser = user;
      httpClientBuilder.addInterceptorFirst(new HttpRequestInterceptor() {
        @Override
        public void process(HttpRequest httpRequest, HttpContext httpContext)
            throws HttpException, IOException {
          httpRequest.addHeader(MetaStoreUtils.USER_NAME_HTTP_HEADER, httpUser);
          for (Map.Entry<String, String> entry : additionalHeaders.entrySet()) {
            httpRequest.addHeader(entry.getKey(), entry.getValue());
          }
        }
      });
    }
    return httpClientBuilder;
  }

  private TTransport createBinaryClient(URI store, boolean useSSL) throws TTransportException,
      MetaException {
    TTransport binaryTransport = null;
    try {
      int clientSocketTimeout = (int) MetastoreConf.getTimeVar(conf,
          ConfVars.CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);
      int connectionTimeout = (int) MetastoreConf.getTimeVar(conf,
          ConfVars.CLIENT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
      if (useSSL) {
        String trustStorePath = MetastoreConf.getVar(conf, ConfVars.SSL_TRUSTSTORE_PATH).trim();
        if (trustStorePath.isEmpty()) {
          throw new IllegalArgumentException(ConfVars.SSL_TRUSTSTORE_PATH
              + " Not configured for SSL connection");
        }
        String trustStorePassword =
            MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.SSL_TRUSTSTORE_PASSWORD);
        String trustStoreType =
            MetastoreConf.getVar(conf, ConfVars.SSL_TRUSTSTORE_TYPE).trim();
        String trustStoreAlgorithm =
            MetastoreConf.getVar(conf, ConfVars.SSL_TRUSTMANAGERFACTORY_ALGORITHM).trim();
        binaryTransport = SecurityUtils.getSSLSocket(store.getHost(), store.getPort(), clientSocketTimeout,
            connectionTimeout, trustStorePath, trustStorePassword, trustStoreType, trustStoreAlgorithm);
      } else {
        binaryTransport = new TSocket(new TConfiguration(), store.getHost(), store.getPort(),
            clientSocketTimeout, connectionTimeout);
      }
      binaryTransport = createAuthBinaryTransport(store, binaryTransport);
    } catch (Exception e) {
      if (e instanceof TTransportException) {
        throw (TTransportException)e;
      } else {
        throw new MetaException("Failed to create binary transport client to url: " + store
            + ". Error: " + e);
      }
    }
    LOG.debug("Created thrift binary client for URI: " + store);
    return configureThriftMaxMessageSize(binaryTransport);
  }

  private void open() throws MetaException {
    isConnected = false;
    TTransportException tte = null;
    MetaException recentME = null;
    boolean useSSL = MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
    boolean useSasl = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_SASL);
    String clientAuthMode = MetastoreConf.getVar(conf, ConfVars.METASTORE_CLIENT_AUTH_MODE);
    boolean usePasswordAuth = false;
    boolean useCompactProtocol = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
    String transportMode = MetastoreConf.getVar(conf, ConfVars.METASTORE_CLIENT_THRIFT_TRANSPORT_MODE);
    boolean isHttpTransportMode = transportMode.equalsIgnoreCase("http");

    if (clientAuthMode != null) {
      usePasswordAuth = "PLAIN".equalsIgnoreCase(clientAuthMode);
    }

    for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
      for (URI store : metastoreUris) {
        LOG.info("Trying to connect to metastore with URI ({}) in {} transport mode", store,
            transportMode);
        try {
          try {
            if (isHttpTransportMode) {
              transport = createHttpClient(store, useSSL);
            } else {
              transport = createBinaryClient(store, useSSL);
            }
          } catch (TTransportException te) {
            tte = te;
            throw new MetaException(te.toString());
          }

          final TProtocol protocol;
          if (useCompactProtocol) {
            protocol = new TCompactProtocol(transport);
          } else {
            protocol = new TBinaryProtocol(transport);
          }
          client = new ThriftHiveMetastore.Client(protocol);
          try {
            if (!transport.isOpen()) {
              transport.open();
              final int newCount = connCount.incrementAndGet();
              if (useSSL) {
                LOG.info(
                    "Opened an SSL connection to metastore, current connections: {}",
                    newCount);
                if (LOG.isTraceEnabled()) {
                  LOG.trace("METASTORE SSL CONNECTION TRACE - open [{}]",
                      System.identityHashCode(this), new Exception());
                }
              } else {
                LOG.info("Opened a connection to metastore, URI ({}) "
                    + "current connections: {}", store, newCount);
                if (LOG.isTraceEnabled()) {
                  LOG.trace("METASTORE CONNECTION TRACE - open [{}]",
                      System.identityHashCode(this), new Exception());
                }
              }
            }
            isConnected = true;
          } catch (TTransportException e) {
            tte = e;
            String errMsg = String.format("Failed to connect to the MetaStore Server URI (%s) in %s "
                    + "transport mode",   store, transportMode);
            LOG.warn(errMsg);
            LOG.debug(errMsg, e);
          }

          if (isConnected && !useSasl && !usePasswordAuth && !isHttpTransportMode &&
                  MetastoreConf.getBoolVar(conf, ConfVars.EXECUTE_SET_UGI)) {
            // Call set_ugi, only in unsecure mode.
            try {
              UserGroupInformation ugi = SecurityUtils.getUGI();
              client.set_ugi(ugi.getUserName(), Arrays.asList(ugi.getGroupNames()));
            } catch (LoginException e) {
              LOG.warn("Failed to do login. set_ugi() is not successful, " +
                  "Continuing without it.", e);
            } catch (IOException e) {
              LOG.warn("Failed to find ugi of client set_ugi() is not successful, " +
                  "Continuing without it.", e);
            } catch (TException e) {
              LOG.warn("set_ugi() not successful, Likely cause: new client talking to old server. "
                  + "Continuing without it.", e);
            }
          }
        } catch (MetaException e) {
          recentME = e;
          String errMsg = "Failed to connect to metastore with URI (" + store
              + ") transport mode:" + transportMode + " in attempt " + attempt;
          LOG.error(errMsg, e);
        }
        if (isConnected) {
          break;
        }
      }
      // Wait before launching the next round of connection retries.
      if (!isConnected && retryDelaySeconds > 0) {
        try {
          LOG.info("Waiting " + retryDelaySeconds + " seconds before next connection attempt.");
          Thread.sleep(retryDelaySeconds * 1000);
        } catch (InterruptedException ignore) {}
      }
    }

    if (!isConnected) {
      // Either tte or recentME should be set but protect from a bug which causes both of them to
      // be null. When MetaException wraps TTransportException, tte will be set so stringify that
      // directly.
      String exceptionString = "Unknown exception";
      if (tte != null) {
        exceptionString = StringUtils.stringifyException(tte);
      } else if (recentME != null) {
        exceptionString = StringUtils.stringifyException(recentME);
      }
      throw new MetaException("Could not connect to meta store using any of the URIs provided." +
          " Most recent failure: " + exceptionString);
    }

    snapshotActiveConf();
  }

  // wraps the underlyingTransport in the appropriate transport based on mode of authentication
  private TTransport createAuthBinaryTransport(URI store, TTransport underlyingTransport)
      throws MetaException {
    boolean isHttpTransportMode =
        MetastoreConf.getVar(conf, ConfVars.METASTORE_CLIENT_THRIFT_TRANSPORT_MODE).
        equalsIgnoreCase("http");
    Preconditions.checkArgument(!isHttpTransportMode);
    Preconditions.checkNotNull(underlyingTransport, "Underlying transport should not be null");
    // default transport is the underlying one
    TTransport transport = underlyingTransport;
    boolean useFramedTransport =
        MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_FRAMED_TRANSPORT);
    boolean useSSL = MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
    boolean useSasl = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_SASL);
    String clientAuthMode = MetastoreConf.getVar(conf, ConfVars.METASTORE_CLIENT_AUTH_MODE);
    boolean usePasswordAuth = false;

    if (clientAuthMode != null) {
      usePasswordAuth = "PLAIN".equalsIgnoreCase(clientAuthMode);
    }
    if (usePasswordAuth) {
      // we are using PLAIN Sasl connection with user/password
      LOG.debug("HMSC::open(): Creating plain authentication thrift connection.");
      String userName = MetastoreConf.getVar(conf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME);

      if (null == userName || userName.isEmpty()) {
        throw new MetaException("No user specified for plain transport.");
      }

      // The password is not directly provided. It should be obtained from a keystore pointed
      // by configuration "hadoop.security.credential.provider.path".
      try {
        String passwd = null;
        char[] pwdCharArray = conf.getPassword(userName);
        if (null != pwdCharArray) {
          passwd = new String(pwdCharArray);
        }
        if (null == passwd) {
          throw new MetaException("No password found for user " + userName);
        }
        // Overlay the SASL transport on top of the base socket transport (SSL or non-SSL)
        transport = MetaStorePlainSaslHelper.getPlainTransport(userName, passwd, underlyingTransport);
      } catch (IOException | TTransportException sasle) {
        // IOException covers SaslException
        LOG.error("Could not create client transport", sasle);
        throw new MetaException(sasle.toString());
      }
    } else if (useSasl) {
      // Wrap thrift connection with SASL for secure connection.
      try {
        HadoopThriftAuthBridge.Client authBridge =
            HadoopThriftAuthBridge.getBridge().createClient();

        // check if we should use delegation tokens to authenticate
        // the call below gets hold of the tokens if they are set up by hadoop
        // this should happen on the map/reduce tasks if the client added the
        // tokens into hadoop's credential store in the front end during job
        // submission.
        String tokenSig = MetastoreConf.getVar(conf, ConfVars.TOKEN_SIGNATURE);
        // tokenSig could be null
        tokenStrForm = SecurityUtils.getTokenStrForm(tokenSig);

        if (tokenStrForm != null) {
          LOG.debug("HMSC::open(): Found delegation token. Creating DIGEST-based thrift connection.");
          // authenticate using delegation tokens via the "DIGEST" mechanism
          transport = authBridge.createClientTransport(null, store.getHost(),
              "DIGEST", tokenStrForm, underlyingTransport,
              MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
        } else {
          LOG.debug("HMSC::open(): Could not find delegation token. Creating KERBEROS-based thrift connection.");
          String principalConfig =
              MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL);
          transport = authBridge.createClientTransport(
              principalConfig, store.getHost(), "KERBEROS", null,
              underlyingTransport, MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
        }
      } catch (IOException ioe) {
        LOG.error("Failed to create client transport", ioe);
        throw new MetaException(ioe.toString());
      }
    } else {
      if (useFramedTransport) {
        try {
          transport = new TFramedTransport(transport);
        } catch (TTransportException e) {
          LOG.error("Failed to create client transport", e);
          throw new MetaException(e.toString());
        }
      }
    }
    return transport;
  }

  private void snapshotActiveConf() {
    currentMetaVars = new HashMap<>(MetastoreConf.metaVars.length);
    for (ConfVars oneVar : MetastoreConf.metaVars) {
      currentMetaVars.put(oneVar.getVarname(), MetastoreConf.getAsString(conf, oneVar));
    }
  }

  @Override
  public String getTokenStrForm() throws IOException {
    return tokenStrForm;
  }

  @Override
  public void close() {
    isConnected = false;
    currentMetaVars = null;
    try {
      if (null != client) {
        client.shutdown();
        if ((transport == null) || !transport.isOpen()) {
          final int newCount = connCount.decrementAndGet();
          LOG.info("Closed a connection to metastore, current connections: {}",
                  newCount);
        }
      }
    } catch (TException e) {
      LOG.debug("Unable to shutdown metastore client. Will try closing transport directly.", e);
    }
    // Transport would have got closed via client.shutdown(), so we dont need this, but
    // just in case, we make this call.
    if ((transport != null) && transport.isOpen()) {
      transport.close();
      final int newCount = connCount.decrementAndGet();
      LOG.info("Closed a connection to metastore, current connections: {}",
          newCount);
      if (LOG.isTraceEnabled()) {
        LOG.trace("METASTORE CONNECTION TRACE - close [{}]",
            System.identityHashCode(this), new Exception());
      }
    }
  }

  public static void setProcessorCapabilities(final String[] capabilities) {
    processorCapabilities = capabilities != null ? Arrays.copyOf(capabilities, capabilities.length) : null;
  }

  public static void setProcessorIdentifier(final String id) {
    processorIdentifier = id;
  }

  public static String[] getProcessorCapabilities() {
    return processorCapabilities != null ? Arrays.copyOf(processorCapabilities, processorCapabilities.length) : null;
  }

  public static String getProcessorIdentifier() {
    return processorIdentifier;
  }

  @Override
  public void setMetaConf(String key, String value) throws TException {
    client.setMetaConf(key, value);
  }

  @Override
  public String getMetaConf(String key) throws TException {
    return client.getMetaConf(key);
  }

  @Override
  public void createCatalog(Catalog catalog) throws TException {
    client.create_catalog(new CreateCatalogRequest(catalog));
  }

  @Override
  public void alterCatalog(String catalogName, Catalog newCatalog) throws TException {
    client.alter_catalog(new AlterCatalogRequest(catalogName, newCatalog));
  }

  @Override
  public Catalog getCatalog(String catName) throws TException {
    GetCatalogResponse rsp = client.get_catalog(new GetCatalogRequest(catName));
    return rsp == null ?
        null : FilterUtils.filterCatalogIfEnabled(isClientFilterEnabled, filterHook, rsp.getCatalog());
  }

  @Override
  public List<String> getCatalogs() throws TException {
    GetCatalogsResponse rsp = client.get_catalogs();
    return rsp == null ?
        null : FilterUtils.filterCatalogNamesIfEnabled(isClientFilterEnabled, filterHook, rsp.getNames());
  }

  @Override
  public void dropCatalog(String catName) throws TException {
    client.drop_catalog(new DropCatalogRequest(catName));
  }

  /**
   * @param new_part
   * @return the added partition
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#add_partition(org.apache.hadoop.hive.metastore.api.Partition)
   */
  @Override
  public Partition add_partition(Partition new_part) throws TException {
    return add_partition(new_part, null);
  }

  public Partition add_partition(Partition new_part, EnvironmentContext envContext)
      throws TException {
    if (new_part != null && !new_part.isSetCatName()) {
      new_part.setCatName(getDefaultCatalog(conf));
    }
    Partition p = client.add_partition_with_environment_context(new_part, envContext);
    return deepCopy(p);
  }

  /**
   * @param new_parts
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#add_partitions(List)
   */
  @Override
  public int add_partitions(List<Partition> new_parts) throws TException {
    List<Partition> ret = add_partitions(new_parts, false, true);
    return ret != null ? ret.size() : 0;
  }

  @Override
  public List<Partition> add_partitions(
      List<Partition> parts, boolean ifNotExists, boolean needResults) throws TException {
    if (parts == null || parts.contains(null)) {
      throw new MetaException("Partitions cannot be null.");
    }
    if (parts.isEmpty()) {
      return needResults ? new ArrayList<>() : null;
    }
    Partition part = parts.get(0);
    // Have to set it for each partition too
    final String defaultCat = getDefaultCatalog(conf);
    AddPartitionsRequest req = new AddPartitionsRequest(
        part.getDbName(), part.getTableName(), parts, ifNotExists);
    boolean skipColumnSchemaForPartition =
            MetastoreConf.getBoolVar(conf, ConfVars.METASTORE_CLIENT_FIELD_SCHEMA_FOR_PARTITIONS);
    if (!part.isSetCatName() && part.getSd() != null
            && part.getSd().getCols() != null
            && skipColumnSchemaForPartition) {
      for (Partition p : parts) {
        p.setCatName(defaultCat);
        p.getSd().getCols().clear();
        StatsSetupConst.clearColumnStatsState(p.getParameters());
      }
    } else if (!part.isSetCatName()) {
      parts.forEach(p -> p.setCatName(defaultCat));
    } else if (part.getSd() != null
            && part.getSd().getCols() != null
            && skipColumnSchemaForPartition) {
      parts.forEach(p -> {
        p.getSd().getCols().clear();
        StatsSetupConst.clearColumnStatsState(p.getParameters());
      });
    }
    req.setParts(parts);
    req.setCatName(part.isSetCatName() ? part.getCatName() : getDefaultCatalog(conf));
    req.setNeedResult(needResults);
    req.setSkipColumnSchemaForPartition(skipColumnSchemaForPartition);
    AddPartitionsResult result = client.add_partitions_req(req);
    if (needResults) {
      List<Partition> new_parts = FilterUtils.filterPartitionsIfEnabled(
              isClientFilterEnabled, filterHook, result.getPartitions());
      if (skipColumnSchemaForPartition) {
        new_parts.forEach(partition -> partition.getSd().setCols(result.getPartitionColSchema()));
      }
      return new_parts;
    }
    return null;
  }

  @Override
  public int add_partitions_pspec(PartitionSpecProxy partitionSpec) throws TException {
    if (partitionSpec == null) {
      throw new MetaException("PartitionSpec cannot be null.");
    }
    if (partitionSpec.getCatName() == null) {
      partitionSpec.setCatName(getDefaultCatalog(conf));
    }
    return client.add_partitions_pspec(partitionSpec.toPartitionSpec());
  }

  @Override
  public Partition appendPartition(String db_name, String table_name,
      List<String> part_vals) throws TException {
    return appendPartition(getDefaultCatalog(conf), db_name, table_name, part_vals);
  }

  @Override
  public Partition appendPartition(String dbName, String tableName, String partName)
      throws TException {
    return appendPartition(getDefaultCatalog(conf), dbName, tableName, partName);
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName,
                                   String name) throws TException {
    Partition p = client.append_partition_by_name(prependCatalogToDbName(
        catName, dbName, conf), tableName, name);
    return deepCopy(p);
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName,
                                   List<String> partVals) throws TException {
    Partition p = client.append_partition(prependCatalogToDbName(
        catName, dbName, conf), tableName, partVals);
    return deepCopy(p);
  }

  @Deprecated
  public Partition appendPartition(String dbName, String tableName, List<String> partVals,
                                   EnvironmentContext ec) throws TException {
    return client.append_partition_with_environment_context(prependCatalogToDbName(dbName, conf),
        tableName, partVals, ec).deepCopy();
  }

  /**
   * Exchange the partition between two tables
   *
   * @param partitionSpecs       partitions specs of the parent partition to be exchanged
   * @param destDb               the db of the destination table
   * @param destinationTableName the destination table name
   * @return new partition after exchanging
   */
  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs,
                                      String sourceDb, String sourceTable, String destDb,
                                      String destinationTableName) throws TException {
    return exchange_partition(partitionSpecs, getDefaultCatalog(conf), sourceDb, sourceTable,
        getDefaultCatalog(conf), destDb, destinationTableName);
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceCat,
      String sourceDb, String sourceTable, String destCat,
      String destDb, String destTableName) throws TException {
    return client.exchange_partition(partitionSpecs, prependCatalogToDbName(sourceCat, sourceDb, conf),
        sourceTable, prependCatalogToDbName(destCat, destDb, conf), destTableName);
  }

  /**
   * Exchange the partitions between two tables
   *
   * @param partitionSpecs       partitions specs of the parent partition to be exchanged
   * @param destDb               the db of the destination table
   * @param destinationTableName the destination table name
   * @return new partitions after exchanging
   */
  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs,
                                             String sourceDb, String sourceTable, String destDb,
                                             String destinationTableName) throws TException {
    return exchange_partitions(partitionSpecs, getDefaultCatalog(conf), sourceDb, sourceTable,
        getDefaultCatalog(conf), destDb, destinationTableName);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName, String tableName, List<String> partNames, List<String> colNames,
      String engine, String validWriteIdList)
      throws NoSuchObjectException, MetaException, TException {
    return getPartitionColumnStatistics(getDefaultCatalog(conf), dbName, tableName,
        partNames, colNames, engine, validWriteIdList);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String catName, String dbName, String tableName, List<String> partNames,
      List<String> colNames, String engine, String validWriteIdList)
      throws NoSuchObjectException, MetaException, TException {
    PartitionsStatsRequest rqst = new PartitionsStatsRequest(dbName, tableName, colNames,
        partNames == null ? new ArrayList<String>() : partNames);
    rqst.setEngine(engine);
    rqst.setCatName(catName);
    rqst.setValidWriteIdList(validWriteIdList);
    return client.get_partitions_statistics_req(rqst).getPartStats();
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames,
      List<String> partNames, String engine, String writeIdList)
      throws NoSuchObjectException, MetaException, TException {
    return getAggrColStatsFor(getDefaultCatalog(conf), dbName, tblName, colNames,
        partNames, engine, writeIdList);
  }

  @Override
  public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames,
      List<String> partNames, String engine, String writeIdList)
      throws NoSuchObjectException, MetaException, TException {
    long t1 = System.currentTimeMillis();

    try {
      if (colNames.isEmpty() || partNames.isEmpty()) {
        LOG.debug("Columns is empty or partNames is empty : Short-circuiting stats eval on client side.");
        return new AggrStats(new ArrayList<>(), 0); // Nothing to aggregate
      }
      PartitionsStatsRequest req = new PartitionsStatsRequest(dbName, tblName, colNames, partNames);
      req.setEngine(engine);
      req.setCatName(catName);
      req.setValidWriteIdList(writeIdList);

      return getAggrStatsForInternal(req);
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getAggrColStatsFor",
            diff, "HMS client");
      }
    }
  }

  protected AggrStats getAggrStatsForInternal(PartitionsStatsRequest req) throws TException {
    return client.get_aggr_stats_for(req);
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceCat,
                                             String sourceDb, String sourceTable, String destCat,
                                             String destDb, String destTableName) throws TException {
    return client.exchange_partitions(partitionSpecs, prependCatalogToDbName(sourceCat, sourceDb, conf),
        sourceTable, prependCatalogToDbName(destCat, destDb, conf), destTableName);
  }

  @Override
  public void validatePartitionNameCharacters(List<String> partVals)
      throws TException, MetaException {
    client.partition_name_has_valid_characters(partVals, true);
  }

  /**
   * Create a new Database
   *
   * @param db
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_database(Database)
   */
  @Override
  public void createDatabase(Database db)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    if (!db.isSetCatalogName()) {
      db.setCatalogName(getDefaultCatalog(conf));
    }
    client.create_database(db);
  }

  /**
   * Create a new DataConnector // TODO
   *
   * @param connector
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_dataconnector(DataConnector)
   */
  @Override
  public void createDataConnector(DataConnector connector)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    client.create_dataconnector(connector);
  }

  /**
   * Drop an existing DataConnector by name // TODO
   * @param name name of the dataconnector to drop.
   * @throws NoSuchObjectException
   * @throws InvalidOperationException
   * @throws MetaException
   * @throws TException
   */
  @Override
  public void dropDataConnector(String name, boolean ifNotExists, boolean checkReferences)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    client.drop_dataconnector(name, ifNotExists, checkReferences);
  }

  /**
   * Alter an existing dataconnector.
   * @param name dataconnector name.
   * @param connector new dataconnector object.
   * @throws NoSuchObjectException No dataconnector with this name exists.
   * @throws MetaException Operation could not be completed, usually in the RDBMS.
   * @throws TException thrift transport layer error.
   */
  @Override
  public void alterDataConnector(String name, DataConnector connector)
      throws NoSuchObjectException, MetaException, TException {
    client.alter_dataconnector(name, connector);
  }

  /**
   * Get the dataconnector by name
   * @return DataConnector if there is a match
   * @throws MetaException error complete the operation
   * @throws TException thrift transport error
   */
  @Override
  public DataConnector getDataConnector(String name)
      throws MetaException, TException {
    GetDataConnectorRequest request = new GetDataConnectorRequest(name);
    return client.get_dataconnector_req(request);
  }

  /**
   * Get the names of all dataconnectors in the MetaStore.
   * @return List of dataconnector names.
   * @throws MetaException error accessing RDBMS.
   * @throws TException thrift transport error
   */
  @Override
  public List<String> getAllDataConnectorNames() throws MetaException, TException {
    List<String> connectorNames = client.get_dataconnectors();
    return FilterUtils.filterDataConnectorsIfEnabled(isClientFilterEnabled, filterHook, connectorNames);
  }

  /**
   * Dry run that translates table
   *    *
   *    * @param tbl
   *    *          a table object
   *    * @throws HiveException
   */
  @Override
  public Table getTranslateTableDryrun(Table tbl) throws AlreadyExistsException,
          InvalidObjectException, MetaException, NoSuchObjectException, TException {
    CreateTableRequest request = new CreateTableRequest(tbl);

    if (processorCapabilities != null) {
      request.setProcessorCapabilities(new ArrayList<String>(Arrays.asList(processorCapabilities)));
      request.setProcessorIdentifier(processorIdentifier);
    }
    return client.translate_table_dryrun(request);
  }

  /**
   * @param tbl
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface #create_table(org.apache.hadoop.hive.metastore.api.CreateTableRequest)
   */
  @Override
  public void createTable(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException {
    createTable(tbl, null);
  }

  public void createTable(Table tbl, EnvironmentContext envContext) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException {
    CreateTableRequest request = new CreateTableRequest(tbl);
    if (envContext != null) {
      request.setEnvContext(envContext);
    }
    createTable(request);
  }

  /**
   * @param request
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  @Override
  public void createTable(CreateTableRequest request) throws
          InvalidObjectException, MetaException, NoSuchObjectException, TException {
    Table tbl = request.getTable();
    if (!tbl.isSetCatName()) {
      tbl.setCatName(getDefaultCatalog(conf));
    }

    if (processorCapabilities != null) {
      request.setProcessorCapabilities(new ArrayList<String>(Arrays.asList(processorCapabilities)));
      request.setProcessorIdentifier(processorIdentifier);
    }

    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preCreateTable(request);
    }
    boolean success = false;
    try {
      // Subclasses can override this step (for example, for temporary tables)
      if (hook == null || !hook.createHMSTableInHook()) {
        create_table(request);
      }
      if (hook != null) {
        hook.commitCreateTable(tbl);
      }
      success = true;
    } finally {
      if (!success && (hook != null)) {
        try {
          hook.rollbackCreateTable(tbl);
        } catch (Exception e) {
          LOG.error("Create rollback failed with", e);
        }
      }
    }
  }

  @Override
  public void createTableWithConstraints(Table tbl,
    List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
    List<SQLUniqueConstraint> uniqueConstraints,
    List<SQLNotNullConstraint> notNullConstraints,
    List<SQLDefaultConstraint> defaultConstraints,
    List<SQLCheckConstraint> checkConstraints)
        throws AlreadyExistsException, InvalidObjectException,
        MetaException, NoSuchObjectException, TException {

    CreateTableRequest createTableRequest = new CreateTableRequest(tbl);

    if (!tbl.isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      tbl.setCatName(defaultCat);
      if (primaryKeys != null) {
        primaryKeys.forEach(pk -> pk.setCatName(defaultCat));
      }
      if (foreignKeys != null) {
        foreignKeys.forEach(fk -> fk.setCatName(defaultCat));
      }
      if (uniqueConstraints != null) {
        uniqueConstraints.forEach(uc -> uc.setCatName(defaultCat));
        createTableRequest.setUniqueConstraints(uniqueConstraints);
      }
      if (notNullConstraints != null) {
        notNullConstraints.forEach(nn -> nn.setCatName(defaultCat));
      }
      if (defaultConstraints != null) {
        defaultConstraints.forEach(def -> def.setCatName(defaultCat));
      }
      if (checkConstraints != null) {
        checkConstraints.forEach(cc -> cc.setCatName(defaultCat));
      }
    }

    if (primaryKeys != null)
      createTableRequest.setPrimaryKeys(primaryKeys);

    if (foreignKeys != null)
      createTableRequest.setForeignKeys(foreignKeys);

    if (uniqueConstraints != null)
      createTableRequest.setUniqueConstraints(uniqueConstraints);

    if (notNullConstraints != null)
      createTableRequest.setNotNullConstraints(notNullConstraints);

    if (defaultConstraints != null)
      createTableRequest.setDefaultConstraints(defaultConstraints);

    if (checkConstraints != null)
      createTableRequest.setCheckConstraints(checkConstraints);

    createTable(createTableRequest);
  }

  @Override
  public void dropConstraint(String dbName, String tableName, String constraintName)
      throws TException {
    dropConstraint(getDefaultCatalog(conf), dbName, tableName, constraintName);
  }

  @Override
  public void dropConstraint(String catName, String dbName, String tableName, String constraintName)
      throws TException {
    DropConstraintRequest rqst = new DropConstraintRequest(dbName, tableName, constraintName);
    rqst.setCatName(catName);
    client.drop_constraint(rqst);
  }

  @Override
  public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws TException {
    if (!primaryKeyCols.isEmpty() && !primaryKeyCols.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      primaryKeyCols.forEach(pk -> pk.setCatName(defaultCat));
    }
    client.add_primary_key(new AddPrimaryKeyRequest(primaryKeyCols));
  }

  @Override
  public void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws TException {
    if (!foreignKeyCols.isEmpty() && !foreignKeyCols.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      foreignKeyCols.forEach(fk -> fk.setCatName(defaultCat));
    }
    client.add_foreign_key(new AddForeignKeyRequest(foreignKeyCols));
  }

  @Override
  public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols) throws
      NoSuchObjectException, MetaException, TException {
    if (!uniqueConstraintCols.isEmpty() && !uniqueConstraintCols.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      uniqueConstraintCols.forEach(uc -> uc.setCatName(defaultCat));
    }
    client.add_unique_constraint(new AddUniqueConstraintRequest(uniqueConstraintCols));
  }

  @Override
  public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols) throws
      NoSuchObjectException, MetaException, TException {
    if (!notNullConstraintCols.isEmpty() && !notNullConstraintCols.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      notNullConstraintCols.forEach(nn -> nn.setCatName(defaultCat));
    }
    client.add_not_null_constraint(new AddNotNullConstraintRequest(notNullConstraintCols));
  }

  @Override
  public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints) throws
      NoSuchObjectException, MetaException, TException {
    if (!defaultConstraints.isEmpty() && !defaultConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      defaultConstraints.forEach(def -> def.setCatName(defaultCat));
    }
    client.add_default_constraint(new AddDefaultConstraintRequest(defaultConstraints));
  }

  @Override
  public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints) throws
      NoSuchObjectException, MetaException, TException {
    if (!checkConstraints.isEmpty() && !checkConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      checkConstraints.forEach(cc -> cc.setCatName(defaultCat));
    }
    client.add_check_constraint(new AddCheckConstraintRequest(checkConstraints));
  }

  /**
   * @param type
   * @return true or false
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_type(org.apache.hadoop.hive.metastore.api.Type)
   */
  public boolean createType(Type type) throws AlreadyExistsException,
      InvalidObjectException, MetaException, TException {
    return client.create_type(type);
  }

  /**
   * @param name
   * @throws NoSuchObjectException
   * @throws InvalidOperationException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_database(java.lang.String, boolean, boolean)
   */
  @Override
  public void dropDatabase(String name)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropDatabase(getDefaultCatalog(conf), name, true, false, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws TException {
    dropDatabase(getDefaultCatalog(conf), name, deleteData, ignoreUnknownDb, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws TException {
    dropDatabase(getDefaultCatalog(conf), name, deleteData, ignoreUnknownDb, cascade);
  }

  @Override
  public void dropDatabase(DropDatabaseRequest req)
      throws TException {
    try {
      getDatabase(req.getCatalogName(), req.getName());
    } catch (NoSuchObjectException e) {
      if (!req.isIgnoreUnknownDb()) {
        throw e;
      }
      return;
    }

    String dbNameWithCatalog = prependCatalogToDbName(req.getCatalogName(), req.getName(), conf);

    if (req.isCascade()) {
      // Note that this logic may drop some of the tables of the database
      // even if the drop database fail for any reason
      // TODO: Fix this
      List<String> materializedViews = getTables(req.getName(), ".*", TableType.MATERIALIZED_VIEW);
      for (String table : materializedViews) {
        // First we delete the materialized views
        Table materializedView = getTable(getDefaultCatalog(conf), req.getName(), table);
        boolean isSoftDelete = req.isSoftDelete() && Boolean.parseBoolean(
          materializedView.getParameters().get(SOFT_DELETE_TABLE));
        materializedView.setTxnId(req.getTxnId());
        dropTable(materializedView, req.isDeleteData() && !isSoftDelete, true, false);
      }

      /**
       * When dropping db cascade, client side hooks have to be called at each table removal.
       * If {@link org.apache.hadoop.hive.metastore.conf.MetastoreConf#ConfVars.BATCH_RETRIEVE_MAX
       * BATCH_RETRIEVE_MAX} is less than the number of tables in the DB, we'll have to call the
       * hooks one by one each alongside with a
       * {@link #dropTable(String, String, boolean, boolean, EnvironmentContext) dropTable} call to
       * ensure transactionality.
       */
      List<String> tableNameList = getAllTables(req.getName());
      int tableCount = tableNameList.size();
      int maxBatchSize = MetastoreConf.getIntVar(conf, ConfVars.BATCH_RETRIEVE_MAX);
      LOG.debug("Selecting dropDatabase method for " + req.getName() + " (" + tableCount + " tables), " +
             ConfVars.BATCH_RETRIEVE_MAX.getVarname() + "=" + maxBatchSize);

      if (tableCount > maxBatchSize) {
        LOG.debug("Dropping database in a per table batch manner.");
        dropDatabaseCascadePerTable(req, tableNameList, maxBatchSize);
      } else {
        LOG.debug("Dropping database in a per DB manner.");
        dropDatabaseCascadePerDb(req, tableNameList);
      }

    } else {
      client.drop_database(dbNameWithCatalog, req.isDeleteData(), req.isCascade());
    }
  }

  /**
   * Handles dropDatabase by invoking drop_table in HMS for each table.
   * Useful when table list in DB is too large to fit in memory. It will retrieve tables in
   * chunks and for each table with a drop_table hook it will invoke drop_table on both HMS and
   * the hook. This is a timely operation so hookless tables are skipped and will be dropped on
   * server side when the client invokes drop_database.
   * Note that this is 'less transactional' than dropDatabaseCascadePerDb since we're dropping
   * table level objects, so the overall outcome of this method might result in a halfly dropped DB.
   * @param tableList
   * @param maxBatchSize
   * @throws TException
   */
  private void dropDatabaseCascadePerTable(DropDatabaseRequest req, List<String> tableList, int maxBatchSize)
      throws TException {
    String dbNameWithCatalog = prependCatalogToDbName(req.getCatalogName(), req.getName(), conf);
    for (Table table : new TableIterable(
        this, req.getCatalogName(), req.getName(), tableList, maxBatchSize)) {
      boolean success = false;
      HiveMetaHook hook = getHook(table);
      try {
        if (hook != null) {
          hook.preDropTable(table);
        }
        boolean isSoftDelete = req.isSoftDelete() && Boolean.parseBoolean(
          table.getParameters().get(SOFT_DELETE_TABLE));
        EnvironmentContext context = null;
        if (req.isSetTxnId()) {
          context = new EnvironmentContext();
          context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(req.getTxnId()));
          req.setDeleteManagedDir(false);
        }
        client.drop_table_with_environment_context(dbNameWithCatalog, table.getTableName(),
            req.isDeleteData() && !isSoftDelete, context);
        if (hook != null) {
          hook.commitDropTable(table, req.isDeleteData());
        }
        success = true;
      } finally {
        if (!success && hook != null) {
          hook.rollbackDropTable(table);
        }
      }
    }
    client.drop_database_req(req);
  }

  /**
   * Handles dropDatabase by invoking drop_database in HMS.
   * Useful when table list in DB can fit in memory, it will retrieve all tables at once and
   * call drop_database once. Also handles drop_table hooks.
   * @param req
   * @param tableList
   * @throws TException
   */
  private void dropDatabaseCascadePerDb(DropDatabaseRequest req, List<String> tableList) throws TException {
    List<Table> tables = getTableObjectsByName(req.getCatalogName(), req.getName(), tableList);
    boolean success = false;
    try {
      for (Table table : tables) {
        HiveMetaHook hook = getHook(table);
        if (hook == null) {
          continue;
        }
        hook.preDropTable(table);
      }
      client.drop_database_req(req);
      for (Table table : tables) {
        HiveMetaHook hook = getHook(table);
        if (hook == null) {
          continue;
        }
        hook.commitDropTable(table, req.isDeleteData());
      }
      success = true;
    } finally {
      if (!success) {
        for (Table table : tables) {
          HiveMetaHook hook = getHook(table);
          if (hook == null) {
            continue;
          }
          hook.rollbackDropTable(table);
        }
      }
    }
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, String partName, boolean deleteData)
      throws TException {
    return dropPartition(getDefaultCatalog(conf), dbName, tableName, partName, deleteData);
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name, String name,
                               boolean deleteData) throws TException {
    return client.drop_partition_by_name_with_environment_context(prependCatalogToDbName(
        catName, db_name, conf), tbl_name, name, deleteData, null);
  }

  private static EnvironmentContext getEnvironmentContextWithIfPurgeSet() {
    Map<String, String> warehouseOptions = new HashMap<>();
    warehouseOptions.put("ifPurge", "TRUE");
    return new EnvironmentContext(warehouseOptions);
  }

  // A bunch of these are in HiveMetaStoreClient but not IMetaStoreClient.  I have marked these
  // as deprecated and not updated them for the catalogs.  If we really want to support them we
  // should add them to IMetaStoreClient.

  @Deprecated
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
      EnvironmentContext env_context) throws TException {
    return client.drop_partition_with_environment_context(prependCatalogToDbName(db_name, conf),
        tbl_name, part_vals, true, env_context);
  }

  @Deprecated
  public boolean dropPartition(String dbName, String tableName, String partName, boolean dropData,
                               EnvironmentContext ec) throws TException {
    return client.drop_partition_by_name_with_environment_context(prependCatalogToDbName(dbName, conf),
        tableName, partName, dropData, ec);
  }

  @Deprecated
  public boolean dropPartition(String dbName, String tableName, List<String> partVals)
      throws TException {
    return client.drop_partition(prependCatalogToDbName(dbName, conf), tableName, partVals, true);
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, boolean deleteData) throws TException {
    return dropPartition(getDefaultCatalog(conf), db_name, tbl_name, part_vals,
        PartitionDropOptions.instance().deleteData(deleteData));
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name,
                               List<String> part_vals, boolean deleteData) throws TException {
    return dropPartition(catName, db_name, tbl_name, part_vals, PartitionDropOptions.instance()
            .deleteData(deleteData));
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name,
                               List<String> part_vals, PartitionDropOptions options) throws TException {
    return dropPartition(getDefaultCatalog(conf), db_name, tbl_name, part_vals, options);
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name,
                               List<String> part_vals, PartitionDropOptions options)
      throws TException {
    if (options == null) {
      options = PartitionDropOptions.instance();
    }
    if (part_vals != null) {
      for (String partVal : part_vals) {
        if (partVal == null) {
          throw new MetaException("The partition value must not be null.");
        }
      }
    }
    return client.drop_partition_with_environment_context(prependCatalogToDbName(
        catName, db_name, conf), tbl_name, part_vals, options.deleteData,
        options.purgeData ? getEnvironmentContextWithIfPurgeSet() : null);
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName,
                                        List<Pair<Integer, byte[]>> partExprs,
                                        PartitionDropOptions options)
      throws TException {
    return dropPartitions(getDefaultCatalog(conf), dbName, tblName, partExprs, options);
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName,
      List<Pair<Integer, byte[]>> partExprs, boolean deleteData,
      boolean ifExists, boolean needResult) throws NoSuchObjectException, MetaException, TException {

    return dropPartitions(getDefaultCatalog(conf), dbName, tblName, partExprs,
                          PartitionDropOptions.instance()
                                              .deleteData(deleteData)
                                              .ifExists(ifExists)
                                              .returnResults(needResult));

  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName,
      List<Pair<Integer, byte[]>> partExprs, boolean deleteData,
      boolean ifExists) throws NoSuchObjectException, MetaException, TException {
    // By default, we need the results from dropPartitions();
    return dropPartitions(getDefaultCatalog(conf), dbName, tblName, partExprs,
                          PartitionDropOptions.instance()
                                              .deleteData(deleteData)
                                              .ifExists(ifExists));
  }

  @Override
  public List<Partition> dropPartitions(String catName, String dbName, String tblName,
                                        List<Pair<Integer, byte[]>> partExprs,
                                        PartitionDropOptions options) throws TException {
    RequestPartsSpec rps = new RequestPartsSpec();
    List<DropPartitionsExpr> exprs = new ArrayList<>(partExprs.size());
    Table table = getTable(catName, dbName, tblName);
    HiveMetaHook hook = getHook(table);
    EnvironmentContext context = new EnvironmentContext();
    if (hook != null) {
      hook.preDropPartitions(table, context, partExprs);
    }
    if (context.getProperties() != null &&
        Boolean.parseBoolean(context.getProperties().get(SKIP_DROP_PARTITION))) {
      return Lists.newArrayList();
    }
    for (Pair<Integer, byte[]> partExpr : partExprs) {
      DropPartitionsExpr dpe = new DropPartitionsExpr();
      dpe.setExpr(partExpr.getRight());
      dpe.setPartArchiveLevel(partExpr.getLeft());
      exprs.add(dpe);
    }
    rps.setExprs(exprs);
    DropPartitionsRequest req = new DropPartitionsRequest(dbName, tblName, rps);
    req.setCatName(catName);
    req.setDeleteData(options.deleteData);
    req.setNeedResult(options.returnResults);
    req.setIfExists(options.ifExists);

    if (options.purgeData) {
      LOG.info("Dropped partitions will be purged!");
      context.putToProperties("ifPurge", "true");
    }
    if (options.writeId != null) {
      context = Optional.ofNullable(context).orElse(new EnvironmentContext());
      context.putToProperties(hive_metastoreConstants.WRITE_ID, options.writeId.toString());
    }
    if (options.txnId != null) {
      context = Optional.ofNullable(context).orElse(new EnvironmentContext());
      context.putToProperties(hive_metastoreConstants.TXN_ID, options.txnId.toString());
    }
    req.setEnvironmentContext(context);
    boolean skipColumnSchemaForPartition = MetastoreConf.getBoolVar(conf, ConfVars.METASTORE_CLIENT_FIELD_SCHEMA_FOR_PARTITIONS);
    req.setSkipColumnSchemaForPartition(skipColumnSchemaForPartition);

    return client.drop_partitions_req(req).getPartitions();
  }

  @Override
  public void dropTable(String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab) throws MetaException, TException,
      NoSuchObjectException, UnsupportedOperationException {
    dropTable(getDefaultCatalog(conf), dbname, name, deleteData, ignoreUnknownTab, null);
  }

  @Override
  public void dropTable(String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab, boolean ifPurge) throws TException {
    dropTable(getDefaultCatalog(conf), dbname, name, deleteData, ignoreUnknownTab, ifPurge);
  }

  @Override
  public void dropTable(Table tbl, boolean deleteData, boolean ignoreUnknownTbl, boolean ifPurge) throws TException {
    EnvironmentContext context = null;
    if (ifPurge) {
      context = getEnvironmentContextWithIfPurgeSet();
    }
    if (tbl.isSetTxnId()) {
      context = Optional.ofNullable(context).orElse(new EnvironmentContext());
      context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(tbl.getTxnId()));
    }
    String catName = Optional.ofNullable(tbl.getCatName()).orElse(getDefaultCatalog(conf));

    dropTable(catName, tbl.getDbName(), tbl.getTableName(), deleteData,
        ignoreUnknownTbl, context);
  }

  @Override
  public void dropTable(String dbname, String name) throws TException {
    dropTable(getDefaultCatalog(conf), dbname, name, true, true, null);
  }

  @Override
  public void dropTable(String catName, String dbName, String tableName, boolean deleteData,
                        boolean ignoreUnknownTable, boolean ifPurge) throws TException {
    //build new environmentContext with ifPurge;
    EnvironmentContext envContext = null;
    if (ifPurge) {
      Map<String, String> warehouseOptions;
      warehouseOptions = new HashMap<>();
      warehouseOptions.put("ifPurge", "TRUE");
      envContext = new EnvironmentContext(warehouseOptions);
    }
    dropTable(catName, dbName, tableName, deleteData, ignoreUnknownTable, envContext);
  }

  /**
   * Drop the table and choose whether to: delete the underlying table data;
   * throw if the table doesn't exist; save the data in the trash.
   *
   * @param catName          catalog name
   * @param dbname           database name
   * @param name             table name
   * @param deleteData       delete the underlying data or just delete the table in metadata
   * @param ignoreUnknownTab don't throw if the requested table doesn't exist
   * @param envContext       for communicating with thrift
   * @throws MetaException                 could not drop table properly
   * @throws NoSuchObjectException         the table wasn't found
   * @throws TException                    a thrift communication error occurred
   * @throws UnsupportedOperationException dropping an index table is not allowed
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_table(java.lang.String,
   * java.lang.String, boolean)
   */
  public void dropTable(String catName, String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab, EnvironmentContext envContext) throws MetaException, TException,
      NoSuchObjectException, UnsupportedOperationException {
    Table tbl;
    try {
      tbl = getTable(catName, dbname, name);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw e;
      }
      return;
    }
    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preDropTable(tbl, deleteData || (envContext != null && "TRUE".equals(envContext.getProperties().get("ifPurge"))));
    }
    boolean success = false;
    try {
      drop_table_with_environment_context(catName, dbname, name, deleteData, envContext);
      if (hook != null) {
        hook.commitDropTable(tbl, deleteData || (envContext != null && "TRUE".equals(envContext.getProperties().get("ifPurge"))));
      }
      success = true;
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw e;
      }
    } finally {
      if (!success && (hook != null)) {
        hook.rollbackDropTable(tbl);
      }
    }
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames,
      String validWriteIds, long writeId, boolean deleteData) throws TException {
    truncateTableInternal(getDefaultCatalog(conf), dbName, tableName, null, partNames, validWriteIds, writeId,
        deleteData);
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames,
      String validWriteIds, long writeId) throws TException {
    truncateTable(dbName, tableName, partNames, validWriteIds, writeId, true);
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames) throws TException {
    truncateTable(getDefaultCatalog(conf), dbName, tableName, partNames);
  }

  @Override
  public void truncateTable(TableName table, List<String> partNames) throws TException {
    truncateTableInternal(table.getCat(), table.getDb(), table.getTable(), table.getTableMetaRef(), partNames,
        null, -1, true);
  }

  @Override
  public void truncateTable(String catName, String dbName, String tableName, List<String> partNames)
      throws TException {
    truncateTable(TableName.fromString(tableName, catName, dbName), partNames);
  }

  private void truncateTableInternal(String catName, String dbName, String tableName, String ref,
      List<String> partNames, String validWriteIds, long writeId, boolean deleteData)
          throws TException {
    Table table = getTable(catName, dbName, tableName);
    HiveMetaHook hook = getHook(table);
    EnvironmentContext context = new EnvironmentContext();
    if (ref != null) {
      context.putToProperties(SNAPSHOT_REF, ref);
    }
    context.putToProperties(TRUNCATE_SKIP_DATA_DELETION, Boolean.toString(!deleteData));
    if (hook != null) {
      hook.preTruncateTable(table, context, partNames);
    }
    TruncateTableRequest req = new TruncateTableRequest(
        prependCatalogToDbName(catName, dbName, conf), tableName);
    req.setPartNames(partNames);
    req.setValidWriteIdList(validWriteIds);
    req.setWriteId(writeId);
    req.setEnvironmentContext(context);
    client.truncate_table_req(req);
  }

  /**
   * Recycles the files recursively from the input path to the cmroot directory either by copying or moving it.
   *
   * @param request Inputs for path of the data files to be recycled to cmroot and
   *                isPurge flag when set to true files which needs to be recycled are not moved to Trash
   * @return Response which is currently void
   */
  @Override
  public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest request) throws MetaException, TException {
    return client.cm_recycle(request);
  }

  /**
   * @param type
   * @return true if the type is dropped
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_type(java.lang.String)
   */
  public boolean dropType(String type) throws NoSuchObjectException, MetaException, TException {
    return client.drop_type(type);
  }

  /**
   * @param name
   * @return map of types
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_type_all(java.lang.String)
   */
  public Map<String, Type> getTypeAll(String name) throws MetaException,
      TException {
    Map<String, Type> result = null;
    Map<String, Type> fromClient = client.get_type_all(name);
    if (fromClient != null) {
      result = new LinkedHashMap<>();
      for (Map.Entry<String, Type> entry: fromClient.entrySet()) {
        result.put(entry.getKey(), deepCopy(entry.getValue()));
      }
    }
    return result;
  }

  @Override
  public List<String> getDatabases(String databasePattern) throws TException {
    return getDatabases(getDefaultCatalog(conf), databasePattern);
  }

  @Override
  public List<String> getDatabases(String catName, String databasePattern) throws TException {
    List<String> databases = client.get_databases(prependCatalogToDbName(
        catName, databasePattern, conf));
    return FilterUtils.filterDbNamesIfEnabled(isClientFilterEnabled, filterHook, databases);
  }

  @Override
  public List<String> getAllDatabases() throws TException {
    return getAllDatabases(getDefaultCatalog(conf));
  }

  @Override
  public List<String> getAllDatabases(String catName) throws TException {
    List<String> databases = client.get_databases(prependCatalogToDbName(catName, null, conf));
    return FilterUtils.filterDbNamesIfEnabled(isClientFilterEnabled, filterHook, databases);
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
      throws TException {
    // TODO should we add capabilities here as well as it returns Partition objects
    return listPartitions(getDefaultCatalog(conf), db_name, tbl_name, max_parts);
  }

  @Override
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
                                        int max_parts) throws TException {
    if (db_name == null || tbl_name == null) {
      throw new MetaException("Database name/Table name should not be null");
    }
    // TODO should we add capabilities here as well as it returns Partition objects
    PartitionsRequest req = createThriftPartitionsReq(PartitionsRequest.class, conf);
    req.setDbName(db_name);
    req.setTblName(tbl_name);
    req.setCatName(catName);
    req.setMaxParts(shrinkMaxtoShort(max_parts));
    List<Partition> parts = client.get_partitions_req(req).getPartitions();
    return deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
    return listPartitionSpecs(getDefaultCatalog(conf), dbName, tableName, maxParts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName,
                                               int maxParts) throws TException {
    List<PartitionSpec> partitionSpecs =
        client.get_partitions_pspec(prependCatalogToDbName(catName, dbName, conf), tableName, maxParts);
    partitionSpecs = FilterUtils.filterPartitionSpecsIfEnabled(isClientFilterEnabled, filterHook, partitionSpecs);
    return PartitionSpecProxy.Factory.get(partitionSpecs);
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name,
                                        List<String> part_vals, short max_parts) throws TException {
    // TODO should we add capabilities here as well as it returns Partition objects
    return listPartitions(getDefaultCatalog(conf), db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
                                        List<String> part_vals, int max_parts) throws TException {
    // TODO should we add capabilities here as well as it returns Partition objects
    if (db_name == null || tbl_name == null || part_vals == null) {
      throw new MetaException("Database name/Table name/partition values should not be null");
    }
    GetPartitionsPsWithAuthRequest req = createThriftPartitionsReq(GetPartitionsPsWithAuthRequest.class, conf);
    req.setDbName(db_name);
    req.setTblName(tbl_name);
    req.setCatName(catName);
    req.setPartVals(part_vals);
    req.setMaxParts(shrinkMaxtoShort(max_parts));
    List<Partition> parts = client.get_partitions_ps_with_auth_req(req).getPartitions();
    return deepCopyPartitions(FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String db_name, String tbl_name,
                                                    short max_parts, String user_name,
                                                    List<String> group_names) throws TException {
    // TODO should we add capabilities here as well as it returns Partition objects
    return listPartitionsWithAuthInfo(getDefaultCatalog(conf), db_name, tbl_name, max_parts, user_name,
        group_names);
  }

  @Override
  public GetPartitionsPsWithAuthResponse listPartitionsWithAuthInfoRequest(GetPartitionsPsWithAuthRequest req)
      throws MetaException, TException, NoSuchObjectException {
    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }
    if(req.getCatName() == null) {
      req.setCatName(getDefaultCatalog(conf));
    }
    req.setMaxParts(shrinkMaxtoShort(req.getMaxParts()));
    GetPartitionsPsWithAuthResponse res = listPartitionsWithAuthInfoRequestInternal(req);
    List<Partition> parts = deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, res.getPartitions()));
    res.setPartitions(parts);
    return res;
  }

  protected GetPartitionsPsWithAuthResponse listPartitionsWithAuthInfoRequestInternal(GetPartitionsPsWithAuthRequest req)
      throws TException {
    return client.get_partitions_ps_with_auth_req(
        createThriftPartitionsReq(GetPartitionsPsWithAuthRequest.class, conf, req));
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                                    int maxParts, String userName,
                                                    List<String> groupNames) throws TException {
    // TODO should we add capabilities here as well as it returns Partition objects
    long t1 = System.currentTimeMillis();
    try {
      List<Partition> parts = listPartitionsWithAuthInfoInternal(catName, dbName, tableName,
          maxParts, userName, groupNames);

      return deepCopyPartitions(FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "listPartitionsWithAuthInfo",
            diff, "HMS client");
      }
    }
  }

  protected List<Partition> listPartitionsWithAuthInfoInternal(String catName, String dbName, String tableName,
        int maxParts, String userName, List<String> groupNames) throws TException {
    if (dbName == null || tableName == null) {
      throw new MetaException("Database name/Table name should not be null");
    }
    GetPartitionsPsWithAuthRequest req = createThriftPartitionsReq(GetPartitionsPsWithAuthRequest.class, conf);
    req.setTblName(tableName);
    req.setDbName(dbName);
    req.setCatName(catName);
    req.setMaxParts(shrinkMaxtoShort(maxParts));
    req.setUserName(userName);
    req.setGroupNames(groupNames);
    List<Partition> partsList = client.get_partitions_ps_with_auth_req(req).getPartitions();
    return partsList;
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String db_name, String tbl_name,
                                                    List<String> part_vals, short max_parts,
                                                    String user_name, List<String> group_names)
      throws TException {
    // TODO should we add capabilities here as well as it returns Partition objects
    return listPartitionsWithAuthInfo(getDefaultCatalog(conf), db_name, tbl_name, part_vals, max_parts,
        user_name, group_names);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                                    List<String> partialPvals, int maxParts,
                                                    String userName, List<String> groupNames)
      throws TException {
    // TODO should we add capabilities here as well as it returns Partition objects
    long t1 = System.currentTimeMillis();
    try {
      List<Partition> parts = listPartitionsWithAuthInfoInternal(
          catName, dbName, tableName, partialPvals, maxParts, userName, groupNames);

      return deepCopyPartitions(FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook,
              parts));
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "listPartitionsWithAuthInfo",
            diff, "HMS client");
      }
    }
  }

  protected List<Partition> listPartitionsWithAuthInfoInternal(String catName, String dbName, String tableName,
      List<String> partialPvals, int maxParts, String userName, List<String> groupNames)
      throws TException {
    if (dbName == null || tableName == null || partialPvals == null) {
      throw new MetaException("Database name/Table name/partition values should not be null");
    }
    GetPartitionsPsWithAuthRequest req = createThriftPartitionsReq(GetPartitionsPsWithAuthRequest.class, conf);
    req.setTblName(tableName);
    req.setDbName(dbName);
    req.setCatName(catName);
    req.setPartVals(partialPvals);
    req.setMaxParts(shrinkMaxtoShort(maxParts));
    req.setUserName(userName);
    req.setGroupNames(groupNames);
    return client.get_partitions_ps_with_auth_req(req).getPartitions();
  }

  @Override
  public List<Partition> listPartitionsByFilter(String db_name, String tbl_name,
      String filter, short max_parts) throws TException {
    return listPartitionsByFilter(getDefaultCatalog(conf), db_name, tbl_name, filter, max_parts);
  }

  @Override
  public List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name,
                                                String filter, int max_parts) throws TException {
    GetPartitionsByFilterRequest req = createThriftPartitionsReq(GetPartitionsByFilterRequest.class, conf);
    req.setTblName(tbl_name);
    req.setDbName(db_name);
    req.setCatName(catName);
    req.setFilter(filter);
    req.setMaxParts(shrinkMaxtoShort(max_parts));
    // TODO should we add capabilities here as well as it returns Partition objects
    List<Partition> parts = client.get_partitions_by_filter_req(req);
    return deepCopyPartitions(FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name,
                                                       String filter, int max_parts)
      throws TException {
    return listPartitionSpecsByFilter(getDefaultCatalog(conf), db_name, tbl_name, filter, max_parts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String db_name,
                                                       String tbl_name, String filter,
                                                       int max_parts) throws TException {
    List<PartitionSpec> partitionSpecs =
        client.get_part_specs_by_filter(prependCatalogToDbName(catName, db_name, conf), tbl_name, filter,
        max_parts);
    return PartitionSpecProxy.Factory.get(
        FilterUtils.filterPartitionSpecsIfEnabled(isClientFilterEnabled, filterHook, partitionSpecs));
  }

  @Override
  public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr,
                                      String default_partition_name, short max_parts,
                                      List<Partition> result) throws TException {
    return listPartitionsByExpr(getDefaultCatalog(conf), db_name, tbl_name, expr,
        default_partition_name, max_parts, result);
  }

  protected PartitionsByExprRequest buildPartitionsByExprRequest(String catName, String db_name, String tbl_name, byte[] expr,
                                                                 String default_partition_name, int max_parts) {
    PartitionsByExprRequest req = new PartitionsByExprRequest(
            db_name, tbl_name, ByteBuffer.wrap(expr));

    if( catName == null ) {
      req.setCatName(getDefaultCatalog(conf));
    }else {
      req.setCatName(catName);
    }
    if (default_partition_name != null) {
      req.setDefaultPartitionName(default_partition_name);
    }
    if (max_parts >= 0) {
      req.setMaxParts(shrinkMaxtoShort(max_parts));
    }
    req.setValidWriteIdList(getValidWriteIdList(db_name, tbl_name));
    return req;
  }

  protected PartitionsByExprResult getPartitionsByExprInternal(PartitionsByExprRequest req) throws TException {
    return client.get_partitions_by_expr(createThriftPartitionsReq(PartitionsByExprRequest.class, conf, req));
  }

  @Override
  public boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr,
      String default_partition_name, int max_parts, List<Partition> result)
          throws TException {
    long t1 = System.currentTimeMillis();

    try {
      assert result != null;
      PartitionsByExprRequest req = buildPartitionsByExprRequest(catName, db_name, tbl_name, expr, default_partition_name,
              max_parts);

      PartitionsByExprResult r = null;

      try {
        r = getPartitionsByExprInternal(req);
      } catch (TApplicationException te) {
        rethrowException(te);
      }

      assert r != null;
      r.setPartitions(FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, r.getPartitions()));
      // TODO: in these methods, do we really need to deepcopy?
      //deepCopyPartitions(r.getPartitions(), result);
      result.addAll(r.getPartitions());

      return !r.isSetHasUnknownPartitions() || r.isHasUnknownPartitions();
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "listPartitionsByExpr",
            diff, "HMS client");
      }
    }
  }


  private void rethrowException(TApplicationException te) throws TException{
    // TODO: backward compat for Hive <= 0.12. Can be removed later.
    if (te.getType() != TApplicationException.UNKNOWN_METHOD
            && te.getType() != TApplicationException.WRONG_METHOD_NAME) {
      throw te;
    }
    throw new IncompatibleMetastoreException(
            "Metastore doesn't support listPartitionsByExpr: " + te.getMessage());
  }

  protected PartitionsSpecByExprResult getPartitionsSpecByExprInternal(PartitionsByExprRequest req) throws TException {
    return client.get_partitions_spec_by_expr(createThriftPartitionsReq(PartitionsByExprRequest.class, conf, req));
  }

  @Override
  public boolean listPartitionsSpecByExpr(PartitionsByExprRequest req, List<PartitionSpec> result)
      throws TException {
    long t1 = System.currentTimeMillis();

    try {
      assert result != null;
      PartitionsSpecByExprResult r = null;
      try {
        r = getPartitionsSpecByExprInternal(req);
      } catch (TApplicationException te) {
        rethrowException(te);
      }

      assert r != null;
      // do client side filtering
      r.setPartitionsSpec(FilterUtils.filterPartitionSpecsIfEnabled(
              isClientFilterEnabled, filterHook, r.getPartitionsSpec()));

      result.addAll(r.getPartitionsSpec());

      return !r.isSetHasUnknownPartitions() || r.isHasUnknownPartitions();
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "listPartitionsSpecByExpr",
            diff, "HMS client");
      }
    }
  }

  @Override
  public Database getDatabase(String name) throws TException {
    return getDatabase(getDefaultCatalog(conf), name);
  }

  @Override
  public Database getDatabase(String catalogName, String databaseName) throws TException {
    long t1 = System.currentTimeMillis();

    try {
      GetDatabaseRequest request = new GetDatabaseRequest();
      if (databaseName != null)
        request.setName(databaseName);
      if (catalogName != null)
        request.setCatalogName(catalogName);
      if (processorCapabilities != null) {
        request.setProcessorCapabilities(new ArrayList<>(Arrays.asList(processorCapabilities)));
      }
      if (processorIdentifier != null) {
        request.setProcessorIdentifier(processorIdentifier);
      }
      Database d = getDatabaseInternal(request);

      return deepCopy(FilterUtils.filterDbIfEnabled(isClientFilterEnabled, filterHook, d));
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getDatabase",
            diff, "HMS client");
      }
    }
  }

  protected Database getDatabaseInternal(GetDatabaseRequest request) throws TException {
    return client.get_database_req(request);
  }

  @Override
  public Partition getPartition(String db_name, String tbl_name, List<String> part_vals)
      throws TException {
    return getPartition(getDefaultCatalog(conf), db_name, tbl_name, part_vals);
  }

  @Override
  public GetPartitionResponse getPartitionRequest(GetPartitionRequest req)
      throws NoSuchObjectException, MetaException, TException {
    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }
    GetPartitionResponse res = client.get_partition_req(req);
    res.setPartition(deepCopy(
        FilterUtils.filterPartitionIfEnabled(isClientFilterEnabled, filterHook, res.getPartition())));
    return res;
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName,
                                List<String> partVals) throws TException {
    Partition p = client.get_partition(prependCatalogToDbName(catName, dbName, conf), tblName, partVals);
    return deepCopy(FilterUtils.filterPartitionIfEnabled(isClientFilterEnabled, filterHook, p));
  }

  @Override
  public List<Partition> getPartitionsByNames(String db_name, String tbl_name,
      List<String> part_names) throws TException {
    return getPartitionsByNames(getDefaultCatalog(conf), db_name, tbl_name, part_names);
  }

  @Override
  public PartitionsResponse getPartitionsRequest(PartitionsRequest req)
      throws NoSuchObjectException, MetaException, TException {
    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }
    PartitionsResponse res = client.get_partitions_req(createThriftPartitionsReq(PartitionsRequest.class, conf, req));
    List<Partition> parts = deepCopyPartitions(
            FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, res.getPartitions()));
    res.setPartitions(parts);
    return res;
  }

  /**
   * @deprecated Use {@link #getPartitionsByNames(GetPartitionsByNamesRequest)} instead
   */
  @Deprecated
  @Override
  public List<Partition> getPartitionsByNames(String catName, String db_name, String tbl_name,
      List<String> part_names) throws TException {
    GetPartitionsByNamesRequest req = convertToGetPartitionsByNamesRequest(
        MetaStoreUtils.prependCatalogToDbName(catName, db_name, conf), tbl_name, part_names);
    return getPartitionsByNames(req).getPartitions();
  }

  @Override
  public GetPartitionsByNamesResult getPartitionsByNames(GetPartitionsByNamesRequest req)
          throws NoSuchObjectException, MetaException, TException {
    checkDbAndTableFilters(getDefaultCatalog(conf), req.getDb_name(), req.getTbl_name());
    if (processorCapabilities != null)
      req.setProcessorCapabilities(new ArrayList<>(Arrays.asList(processorCapabilities)));
    if (processorIdentifier != null)
      req.setProcessorIdentifier(processorIdentifier);
    List<Partition> parts = getPartitionsByNamesInternal(req).getPartitions();
    GetPartitionsByNamesResult res = new GetPartitionsByNamesResult();
    res.setPartitions(deepCopyPartitions(FilterUtils.filterPartitionsIfEnabled(
            isClientFilterEnabled, filterHook, parts)));
    return res;
  }

  protected GetPartitionsByNamesResult getPartitionsByNamesInternal(GetPartitionsByNamesRequest gpbnr)
      throws TException {
    return client.get_partitions_by_names_req(createThriftPartitionsReq(GetPartitionsByNamesRequest.class, conf, gpbnr));
  }

  @Override
  public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request)
      throws MetaException, TException, NoSuchObjectException {
    if (!request.isSetCatName()) {
      request.setCatName(getDefaultCatalog(conf));
    }

    String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
    String dbName = request.getDbName();
    String tblName = request.getTblName();

    checkDbAndTableFilters(catName, dbName, tblName);
    return client.get_partition_values(request);
  }

  @Override
  public Partition getPartitionWithAuthInfo(String db_name, String tbl_name,
      List<String> part_vals, String user_name, List<String> group_names)
      throws TException {
    return getPartitionWithAuthInfo(getDefaultCatalog(conf), db_name, tbl_name, part_vals,
        user_name, group_names);
  }

  @Override
  public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName,
                                            List<String> pvals, String userName,
                                            List<String> groupNames) throws TException {
    Partition p = client.get_partition_with_auth(prependCatalogToDbName(catName, dbName, conf), tableName,
        pvals, userName, groupNames);
    return deepCopy(FilterUtils.filterPartitionIfEnabled(isClientFilterEnabled, filterHook, p));
  }

  /**
   * @deprecated use getTable(GetTableRequest getTableRequest)
   * @param dbname
   * @param name
   * @return
   * @throws TException
   */
  @Override
  @Deprecated
  public Table getTable(String dbname, String name) throws TException {
    GetTableRequest req = new GetTableRequest(dbname, name);
    req.setCatName(getDefaultCatalog(conf));
    return getTable(req);
  }

  /**
   * @deprecated use getTable(GetTableRequest getTableRequest)
   * @param dbname
   * @param name
   * @param getColumnStats
   *          get the column stats, if available, when true
   * @param engine engine sending the request
   * @return
   * @throws TException
   */
  @Override
  @Deprecated
  public Table getTable(String dbname, String name, boolean getColumnStats, String engine) throws TException {
    GetTableRequest req = new GetTableRequest(dbname, name);
    req.setCatName(getDefaultCatalog(conf));
    req.setGetColumnStats(getColumnStats);
    if (getColumnStats) {
      req.setEngine(engine);
    }
    return getTable(req);
  }

  /**
   * @deprecated use getTable(GetTableRequest getTableRequest)
   * @param catName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @return
   * @throws TException
   */
  @Override
  @Deprecated
  public Table getTable(String catName, String dbName, String tableName) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    return getTable(req);
  }

  /**
   * @deprecated use getTable(GetTableRequest getTableRequest)
   * @param catName
   * @param dbName
   * @param tableName
   * @param getColumnStats
   * @param engine
   * @return
   * @throws TException
   */
  @Deprecated
  public Table getTable(String catName, String dbName, String tableName,
      boolean getColumnStats, String engine) throws TException {
      GetTableRequest req = new GetTableRequest(dbName, tableName);
      req.setCatName(catName);
      req.setGetColumnStats(getColumnStats);
      if (getColumnStats) {
        req.setEngine(engine);
      }
      return getTable(req);
  }

  protected GetTableResult getTableInternal(GetTableRequest req) throws TException {
    return client.get_table_req(req);
  }

  /**
   * @deprecated use getTable(GetTableRequest getTableRequest)
   * @param catName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @param validWriteIdList applicable snapshot
   * @return
   * @throws TException
   */
  @Override
  @Deprecated
  public Table getTable(String catName, String dbName, String tableName,
      String validWriteIdList) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    req.setValidWriteIdList(validWriteIdList);
    return getTable(req);
  }

  /**
   * @deprecated use getTable(GetTableRequest getTableRequest)
   * @param catName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @param validWriteIdList applicable snapshot
   * @param getColumnStats get the column stats, if available, when true
   * @param engine engine sending the request
   * @return
   * @throws TException
   */
  @Override
  @Deprecated
  public Table getTable(String catName, String dbName, String tableName, String validWriteIdList,
      boolean getColumnStats, String engine) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    req.setValidWriteIdList(validWriteIdList);
    req.setGetColumnStats(getColumnStats);
    if (getColumnStats) {
      req.setEngine(engine);
    }
    return getTable(req);
  }

  @Override
  public Table getTable(GetTableRequest getTableRequest) throws MetaException, TException, NoSuchObjectException {
    long t1 = System.currentTimeMillis();

    try {
      getTableRequest.setCapabilities(version);
      if (processorCapabilities != null)
        getTableRequest.setProcessorCapabilities(new ArrayList<String>(Arrays.asList(processorCapabilities)));
      if (processorIdentifier != null)
        getTableRequest.setProcessorIdentifier(processorIdentifier);

      Table t = getTableInternal(getTableRequest).getTable();
      executePostGetTableHook(t);
      return deepCopy(FilterUtils.filterTableIfEnabled(isClientFilterEnabled, filterHook, t));
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getTable",
            diff, "HMS client");
      }
    }
  }

  private void executePostGetTableHook(Table t) throws MetaException {
    HiveMetaHook hook = getHook(t);
    if (hook != null) {
      hook.postGetTable(t);
    }
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws TException {
    return getTables(getDefaultCatalog(conf), dbName, tableNames, null);
  }

  @Override
  public List<Table> getTableObjectsByName(String catName, String dbName,
                                           List<String> tableNames) throws TException {
    return getTables(catName, dbName, tableNames, null);
  }

  @Override
  public List<Table> getTables(String catName, String dbName, List<String> tableNames,
      GetProjectionsSpec projectionsSpec) throws TException {
    GetTablesRequest req = new GetTablesRequest(dbName);
    req.setCatName(catName);
    req.setTblNames(tableNames);
    req.setCapabilities(version);
    if (processorCapabilities != null)
      req.setProcessorCapabilities(new ArrayList<String>(Arrays.asList(processorCapabilities)));
    req.setProjectionSpec(projectionsSpec);
    List<Table> tabs = client.get_table_objects_by_name_req(req).getTables();
    for (Table tbl : tabs) {
      executePostGetTableHook(tbl);
    }
    return deepCopyTables(FilterUtils.filterTablesIfEnabled(isClientFilterEnabled, filterHook, tabs));
  }

  @Override
  public Materialization getMaterializationInvalidationInfo(CreationMetadata cm, String validTxnList)
          throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return client.get_materialization_invalidation_info(cm, validTxnList);
  }

  @Override
  public void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    client.update_creation_metadata(getDefaultCatalog(conf), dbName, tableName, cm);
  }

  @Override
  public void updateCreationMetadata(String catName, String dbName, String tableName,
                                     CreationMetadata cm) throws MetaException, TException {
    client.update_creation_metadata(catName, dbName, tableName, cm);

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws TException {
    return listTableNamesByFilter(getDefaultCatalog(conf), dbName, filter, maxTables);
  }

  @Override
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter,
                                             int maxTables) throws TException {
    List<String> tableNames =
        client.get_table_names_by_filter(prependCatalogToDbName(catName, dbName, conf), filter,
        shrinkMaxtoShort(maxTables));
    return FilterUtils.filterTableNamesIfEnabled(
        isClientFilterEnabled, filterHook, catName, dbName, tableNames);
  }

  /**
   * @param name
   * @return the type
   * @throws MetaException
   * @throws TException
   * @throws NoSuchObjectException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_type(java.lang.String)
   */
  public Type getType(String name) throws NoSuchObjectException, MetaException, TException {
    return deepCopy(client.get_type(name));
  }

  @Override
  public List<String> getTables(String dbname, String tablePattern) throws MetaException {
    try {
      return getTables(getDefaultCatalog(conf), dbname, tablePattern);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern)
      throws TException {
    List<String> tables = new ArrayList<>();
    GetProjectionsSpec projectionsSpec = new GetProjectionsSpec();
    projectionsSpec.setFieldList(Arrays.asList("dbName", "tableName", "owner", "ownerType"));
    GetTablesRequest req = new GetTablesRequest(dbName);
    req.setCatName(catName);
    req.setCapabilities(version);
    req.setTblNames(null);
    if(tablePattern == null){
      tablePattern = ".*";
    }
    req.setTablesPattern(tablePattern);
    if (processorCapabilities != null)
      req.setProcessorCapabilities(new ArrayList<String>(Arrays.asList(processorCapabilities)));
    if (processorIdentifier != null)
      req.setProcessorIdentifier(processorIdentifier);
    req.setProjectionSpec(projectionsSpec);
    List<Table> tableObjects = client.get_table_objects_by_name_req(req).getTables();
    tableObjects = deepCopyTables(FilterUtils.filterTablesIfEnabled(isClientFilterEnabled, filterHook, tableObjects));
    for (Table tbl : tableObjects) {
      tables.add(tbl.getTableName());
    }
    return tables;
  }

  @Override
  public List<String> getTables(String dbname, String tablePattern, TableType tableType) throws MetaException {
    try {
      return getTables(getDefaultCatalog(conf), dbname, tablePattern, tableType);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern,
                                TableType tableType) throws TException {
    List<String> tables =
        client.get_tables_by_type(prependCatalogToDbName(catName, dbName, conf), tablePattern,
        tableType.toString());
    return FilterUtils.filterTableNamesIfEnabled(isClientFilterEnabled, filterHook, catName, dbName, tables);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ExtendedTableInfo> getTablesExt(String catName, String dbName, String tablePattern,
                 int requestedFields, int limit) throws MetaException, TException {
    if (catName == null)
      catName = getDefaultCatalog(conf);
    GetTablesExtRequest req = new GetTablesExtRequest(catName, dbName, tablePattern, requestedFields);
    req.setLimit(limit);
    if (processorIdentifier != null)
      req.setProcessorIdentifier(processorIdentifier);
    if (processorCapabilities != null)
      req.setProcessorCapabilities(new ArrayList<String>(Arrays.asList(processorCapabilities)));
    return client.get_tables_ext(req);
  }

  @Override
  public List<Table> getAllMaterializedViewObjectsForRewriting() throws TException {
    try {
      List<Table> views = client.get_all_materialized_view_objects_for_rewriting();
      return FilterUtils.filterTablesIfEnabled(isClientFilterEnabled, filterHook, views);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String dbName) throws TException {
    return getMaterializedViewsForRewriting(getDefaultCatalog(conf), dbName);
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catName, String dbname)
      throws MetaException {
    try {
      List<String> views =
          client.get_materialized_views_for_rewriting(prependCatalogToDbName(catName, dbname, conf));
      return FilterUtils.filterTableNamesIfEnabled(isClientFilterEnabled, filterHook, catName, dbname, views);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException {
    try {
      return getTableMeta(getDefaultCatalog(conf), dbPatterns, tablePatterns, tableTypes);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
                                      List<String> tableTypes) throws TException {
    List<TableMeta> tableMetas = client.get_table_meta(prependCatalogToDbName(catName, dbPatterns, conf),
        tablePatterns, tableTypes);
    return FilterUtils.filterTableMetasIfEnabled(isClientFilterEnabled, filterHook, tableMetas);
  }

  @Override
  public List<String> getAllTables(String dbname) throws MetaException {
    try {
      return getAllTables(getDefaultCatalog(conf), dbname);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) throws TException {
    List<String> tableNames = client.get_all_tables(
        prependCatalogToDbName(catName, dbName, conf));
    return FilterUtils.filterTableNamesIfEnabled(isClientFilterEnabled, filterHook, catName, dbName, tableNames);
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) throws TException {
    return tableExists(getDefaultCatalog(conf), databaseName, tableName);
  }

  @Override
  public boolean tableExists(String catName, String dbName, String tableName) throws TException {
    try {
      GetTableRequest req = new GetTableRequest(dbName, tableName);
      req.setCatName(catName);
      req.setCapabilities(version);
      Table table = getTableInternal(req).getTable();
      return FilterUtils.filterTableIfEnabled(isClientFilterEnabled, filterHook, table) != null;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName,
      short max) throws NoSuchObjectException, MetaException, TException {
    return listPartitionNames(getDefaultCatalog(conf), dbName, tblName, max);
  }

  @Override
  public GetPartitionNamesPsResponse listPartitionNamesRequest(GetPartitionNamesPsRequest req)
          throws NoSuchObjectException, MetaException, TException {
    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }
    if( req.getCatName() == null ) {
      req.setCatName(getDefaultCatalog(conf));
    }
    GetPartitionNamesPsResponse res = listPartitionNamesRequestInternal(req);
    List<String> partNames = FilterUtils.filterPartitionNamesIfEnabled(
            isClientFilterEnabled, filterHook, getDefaultCatalog(conf), req.getDbName(),
            req.getTblName(), res.getNames());
    res.setNames(partNames);
    return res;
  }

  protected GetPartitionNamesPsResponse listPartitionNamesRequestInternal(GetPartitionNamesPsRequest req)
      throws TException {
    return client.get_partition_names_ps_req(req);
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tableName,
                                         int maxParts) throws TException {
    List<String> partNames = listPartitionNamesInternal(
        catName, dbName, tableName, maxParts);
    return FilterUtils.filterPartitionNamesIfEnabled(
        isClientFilterEnabled, filterHook, catName, dbName, tableName, partNames);
  }

  protected List<String> listPartitionNamesInternal(String catName, String dbName, String tableName,
      int maxParts) throws TException {
    return client.get_partition_names(
        prependCatalogToDbName(catName, dbName, conf), tableName, shrinkMaxtoShort(maxParts));
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name,
      List<String> part_vals, short max_parts) throws TException {
    return listPartitionNames(getDefaultCatalog(conf), db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<String> listPartitionNames(String catName, String db_name, String tbl_name,
                                         List<String> part_vals, int max_parts) throws TException {
    List<String> partNames = listPartitionNamesInternal(
        catName, db_name, tbl_name, part_vals, max_parts);
    return FilterUtils.filterPartitionNamesIfEnabled(
        isClientFilterEnabled, filterHook, catName, db_name, tbl_name, partNames);
  }

  protected List<String> listPartitionNamesInternal(String catName, String db_name, String tbl_name,
      List<String> part_vals, int max_parts) throws TException {
    return client.get_partition_names_ps(prependCatalogToDbName(catName, db_name, conf), tbl_name,
        part_vals, shrinkMaxtoShort(max_parts));
  }

  @Override
  public List<String> listPartitionNames(PartitionsByExprRequest req)
      throws MetaException, TException, NoSuchObjectException {
    return FilterUtils.filterPartitionNamesIfEnabled(isClientFilterEnabled, filterHook, req.getCatName(),
        req.getDbName(), req.getTblName(), client.get_partition_names_req(req));
  }

  @Override
  public int getNumPartitionsByFilter(String db_name, String tbl_name,
                                      String filter) throws TException {
    return getNumPartitionsByFilter(getDefaultCatalog(conf), db_name, tbl_name, filter);
  }

  @Override
  public int getNumPartitionsByFilter(String catName, String dbName, String tableName,
                                      String filter) throws TException {
    return client.get_num_partitions_by_filter(prependCatalogToDbName(catName, dbName, conf), tableName,
        filter);
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    alter_partition(getDefaultCatalog(conf), dbName, tblName, newPart, null);
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    alter_partition(getDefaultCatalog(conf), dbName, tblName, newPart, environmentContext);
  }

  @Override
  public void alter_partition(String catName, String dbName, String tblName, Partition newPart,
                              EnvironmentContext environmentContext) throws TException {
    AlterPartitionsRequest req = new AlterPartitionsRequest(dbName, tblName, Lists.newArrayList(newPart));
    req.setCatName(catName);
    req.setEnvironmentContext(environmentContext);
    client.alter_partitions_req(req);
  }

  @Override
  public void alter_partition(String catName, String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext, String writeIdList)
      throws InvalidOperationException, MetaException, TException {
    AlterPartitionsRequest req = new AlterPartitionsRequest(
        dbName, tblName, Lists.newArrayList(newPart));
    req.setCatName(catName);
    req.setEnvironmentContext(environmentContext);
    req.setValidWriteIdList(writeIdList);
    client.alter_partitions_req(req);
  }

  @Deprecated
  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts)
      throws TException {
    alter_partitions(
        getDefaultCatalog(conf), dbName, tblName, newParts, new EnvironmentContext(), null, -1);
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts,
                               EnvironmentContext environmentContext) throws TException {
    alter_partitions(
        getDefaultCatalog(conf), dbName, tblName, newParts, environmentContext, null, -1);
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts,
                               EnvironmentContext environmentContext,
                               String writeIdList, long writeId)
      throws InvalidOperationException, MetaException, TException {
    alter_partitions(getDefaultCatalog(conf),
        dbName, tblName, newParts, environmentContext, writeIdList, writeId);

  }

  @Override
  public void alter_partitions(String catName, String dbName, String tblName,
                               List<Partition> newParts,
                               EnvironmentContext environmentContext,
                               String writeIdList, long writeId) throws TException {
    AlterPartitionsRequest req = new AlterPartitionsRequest();
    req.setCatName(catName);
    req.setDbName(dbName);
    req.setTableName(tblName);
    boolean skipColumnSchemaForPartition =
            MetastoreConf.getBoolVar(conf, ConfVars.METASTORE_CLIENT_FIELD_SCHEMA_FOR_PARTITIONS);
    if (newParts != null && !newParts.isEmpty() && newParts.get(0).getSd() != null
            && newParts.get(0).getSd().getCols() != null
            && skipColumnSchemaForPartition) {
      newParts.forEach(p -> p.getSd().getCols().clear());
    }
    req.setPartitions(newParts);
    req.setSkipColumnSchemaForPartition(skipColumnSchemaForPartition);
    req.setEnvironmentContext(environmentContext);
    req.setValidWriteIdList(writeIdList);
    req.setWriteId(writeId);
    client.alter_partitions_req(req);
  }

  @Override
  public void alterDatabase(String dbName, Database db) throws TException {
    alterDatabase(getDefaultCatalog(conf), dbName, db);
  }

  @Override
  public void alterDatabase(String catName, String dbName, Database newDb) throws TException {
    client.alter_database(prependCatalogToDbName(catName, dbName, conf), newDb);
  }

  @Override
  public List<FieldSchema> getFields(String db, String tableName) throws TException {
    return getFields(getDefaultCatalog(conf), db, tableName);
  }

  @Override
  public List<FieldSchema> getFields(String catName, String db, String tableName)
      throws TException {
    List<FieldSchema> fields = client.get_fields(prependCatalogToDbName(catName, db, conf), tableName);
    return deepCopyFieldSchemas(fields);
  }

  @Override
  public GetFieldsResponse getFieldsRequest(GetFieldsRequest req)
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    return client.get_fields_req(req);
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest req) throws TException {
    long t1 = System.currentTimeMillis();

    try {
      if (!req.isSetCatName()) {
        req.setCatName(getDefaultCatalog(conf));
      }

      return getPrimaryKeysInternal(req).getPrimaryKeys();
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getPrimaryKeys",
            diff, "HMS client");
      }
    }
  }

  protected PrimaryKeysResponse getPrimaryKeysInternal(PrimaryKeysRequest req) throws TException {
    return client.get_primary_keys(req);
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest req) throws MetaException,
      NoSuchObjectException, TException {
    long t1 = System.currentTimeMillis();

    try {
      if (!req.isSetCatName()) {
        req.setCatName(getDefaultCatalog(conf));
      }

      return getForeignKeysInternal(req).getForeignKeys();
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getForeignKeys",
            diff, "HMS client");
      }
    }
  }

  protected ForeignKeysResponse getForeignKeysInternal(ForeignKeysRequest req) throws TException {
    return client.get_foreign_keys(req);
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest req)
      throws MetaException, NoSuchObjectException, TException {
    long t1 = System.currentTimeMillis();

    try {
      if (!req.isSetCatName()) {
        req.setCatName(getDefaultCatalog(conf));
      }

      return getUniqueConstraintsInternal(req).getUniqueConstraints();
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getUniqueConstraints",
            diff, "HMS client");
      }
    }
  }

  protected UniqueConstraintsResponse getUniqueConstraintsInternal(UniqueConstraintsRequest req) throws TException {
    return client.get_unique_constraints(req);
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest req)
      throws MetaException, NoSuchObjectException, TException {
    long t1 = System.currentTimeMillis();

    try {
      if (!req.isSetCatName()) {
        req.setCatName(getDefaultCatalog(conf));
      }

      return getNotNullConstraintsInternal(req).getNotNullConstraints();
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getNotNullConstraints",
            diff, "HMS client");
      }
    }
  }

  protected NotNullConstraintsResponse getNotNullConstraintsInternal(NotNullConstraintsRequest req) throws TException {
    return client.get_not_null_constraints(req);
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest req)
      throws MetaException, NoSuchObjectException, TException {
    if (!req.isSetCatName()) {
      req.setCatName(getDefaultCatalog(conf));
    }
    return client.get_default_constraints(req).getDefaultConstraints();
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest req)
      throws MetaException, NoSuchObjectException, TException {
    if (!req.isSetCatName()) {
      req.setCatName(getDefaultCatalog(conf));
    }
    return client.get_check_constraints(req).getCheckConstraints();
  }

  /**
   * Api to fetch all table constraints at once
   * @param req request info
   * @return all constraints attached to given table
   * @throws MetaException
   * @throws TException
   */
  @Override
  public SQLAllTableConstraints getAllTableConstraints(AllTableConstraintsRequest req)
      throws MetaException, TException {
    long t1 = 0;

    if (LOG.isDebugEnabled()) {
      t1 = System.currentTimeMillis();
    }

    try {
      return client.get_all_table_constraints(req).getAllTableConstraints();
    } finally {
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getAllTableConstraints",
            System.currentTimeMillis() - t1, "HMS client");
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj) throws TException {
    if (!statsObj.getStatsDesc().isSetCatName()) {
      statsObj.getStatsDesc().setCatName(getDefaultCatalog(conf));
    }
    // Note: currently this method doesn't set txn properties and thus won't work on txn tables.
    SetPartitionsStatsRequest req = new SetPartitionsStatsRequest();
    req.addToColStats(statsObj);
    req.setEngine(statsObj.getEngine());
    req.setNeedMerge(false);
    return client.update_table_column_statistics_req(req).isResult();
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj) throws TException {
    if (!statsObj.getStatsDesc().isSetCatName()) {
      statsObj.getStatsDesc().setCatName(getDefaultCatalog(conf));
    }
    // Note: currently this method doesn't set txn properties and thus won't work on txn tables.
    SetPartitionsStatsRequest req = new SetPartitionsStatsRequest();
    req.addToColStats(statsObj);
    req.setEngine(statsObj.getEngine());
    req.setNeedMerge(false);
    return client.update_partition_column_statistics_req(req).isResult();
  }

  @Override
  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request) throws TException {
    String defaultCat = getDefaultCatalog(conf);
    for (ColumnStatistics stats : request.getColStats()) {
      if (!stats.getStatsDesc().isSetCatName()) {
        stats.getStatsDesc().setCatName(defaultCat);
      }
    }
    return client.set_aggr_stats_for(request);
  }

  @Override
  public void flushCache() {
    try {
      client.flushCache();
    } catch (TException e) {
      // Not much we can do about it honestly
      LOG.warn("Got error flushing the cache", e);
    }
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames, String engine) throws TException {
    return getTableColumnStatistics(getDefaultCatalog(conf), dbName, tableName, colNames, engine);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName,
      String tableName, List<String> colNames, String engine) throws TException {
    long t1 = System.currentTimeMillis();

    try {
      if (colNames.isEmpty()) {
        return Collections.emptyList();
      }
      TableStatsRequest rqst = new TableStatsRequest(dbName, tableName, colNames);
      rqst.setEngine(engine);
      rqst.setCatName(catName);
      rqst.setEngine(engine);
      return getTableColumnStatisticsInternal(rqst).getTableStats();
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getTableColumnStatistics",
            diff, "HMS client");
      }
    }
  }

  protected TableStatsResult getTableColumnStatisticsInternal(TableStatsRequest rqst) throws TException {
    return client.get_table_statistics_req(rqst);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames, String engine, String validWriteIdList) throws TException {
    return getTableColumnStatistics(getDefaultCatalog(conf), dbName, tableName, colNames,
        engine, validWriteIdList);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName,
      String tableName, List<String> colNames, String engine, String validWriteIdList) throws TException {
    long t1 = System.currentTimeMillis();

    try {
      if (colNames.isEmpty()) {
        return Collections.emptyList();
      }
      TableStatsRequest rqst = new TableStatsRequest(dbName, tableName, colNames);
      rqst.setEngine(engine);
      rqst.setCatName(catName);
      rqst.setValidWriteIdList(validWriteIdList);

      return getTableColumnStatisticsInternal(rqst).getTableStats();
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getTableColumnStatistics",
            diff, "HMS client");
      }
    }
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName, String tableName, List<String> partNames, List<String> colNames, String engine)
          throws TException {
    return getPartitionColumnStatistics(getDefaultCatalog(conf), dbName, tableName, partNames, colNames, engine);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String catName, String dbName, String tableName, List<String> partNames,
      List<String> colNames, String engine) throws TException {
    PartitionsStatsRequest rqst = new PartitionsStatsRequest(dbName, tableName, colNames, partNames);
    rqst.setEngine(engine);
    rqst.setCatName(catName);
    rqst.setValidWriteIdList(getValidWriteIdList(dbName, tableName));
    return client.get_partitions_statistics_req(rqst).getPartStats();
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
      String colName, String engine) throws TException {
    return deletePartitionColumnStatistics(getDefaultCatalog(conf), dbName, tableName, partName,
        colName, engine);
  }

  @Override
  public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName,
      String partName, String colName, String engine) throws TException {
    return client.delete_partition_column_statistics(prependCatalogToDbName(catName, dbName, conf),
        tableName, partName, colName, engine);
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName, String engine)
      throws TException {
    return deleteTableColumnStatistics(getDefaultCatalog(conf), dbName, tableName, colName, engine);
  }

  @Override
  public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName,
      String colName, String engine) throws TException {
    return client.delete_table_column_statistics(prependCatalogToDbName(catName, dbName, conf),
        tableName, colName, engine);
  }

  @Override
  public void updateTransactionalStatistics(UpdateTransactionalStatsRequest req)  throws TException {
    client.update_transaction_statistics(req);
  }

  @Override
  public List<FieldSchema> getSchema(String db, String tableName) throws TException {
    return getSchema(getDefaultCatalog(conf), db, tableName);
  }

  @Override
  public List<FieldSchema> getSchema(String catName, String db, String tableName) throws TException {
    EnvironmentContext envCxt = null;
    String addedJars = MetastoreConf.getVar(conf, ConfVars.ADDED_JARS);
    if (org.apache.commons.lang3.StringUtils.isNotBlank(addedJars)) {
      Map<String, String> props = new HashMap<>();
      props.put("hive.added.jars.path", addedJars);
      envCxt = new EnvironmentContext(props);
    }

    List<FieldSchema> fields = client.get_schema_with_environment_context(prependCatalogToDbName(
        catName, db, conf), tableName, envCxt);
    return deepCopyFieldSchemas(fields);
  }

  @Override
  public GetSchemaResponse getSchemaRequest(GetSchemaRequest req)
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    EnvironmentContext envCxt = null;
    String addedJars = MetastoreConf.getVar(conf, ConfVars.ADDED_JARS);
    if (org.apache.commons.lang3.StringUtils.isNotBlank(addedJars)) {
      Map<String, String> props = new HashMap<>();
      props.put("hive.added.jars.path", addedJars);
      envCxt = new EnvironmentContext(props);
      req.setEnvContext(envCxt);
    }
    return client.get_schema_req(req);
  }

  @Override
  public String getConfigValue(String name, String defaultValue)
      throws TException, ConfigValSecurityException {
    long t1 = System.currentTimeMillis();

    try {
      return getConfigValueInternal(name, defaultValue);
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getConfigValue",
            diff, "HMS client");
      }
    }
  }

  protected String getConfigValueInternal(String name, String defaultValue)
      throws TException, ConfigValSecurityException {
    return client.get_config_value(name, defaultValue);
  }

  @Override
  public Partition getPartition(String db, String tableName, String partName) throws TException {
    return getPartition(getDefaultCatalog(conf), db, tableName, partName);
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, String name)
      throws TException {
    Partition p = client.get_partition_by_name(prependCatalogToDbName(catName, dbName, conf), tblName,
        name);
    return deepCopy(FilterUtils.filterPartitionIfEnabled(isClientFilterEnabled, filterHook, p));
  }

  public Partition appendPartitionByName(String dbName, String tableName, String partName)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return appendPartitionByName(dbName, tableName, partName, null);
  }

  public Partition appendPartitionByName(String dbName, String tableName, String partName,
      EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    Partition p = client.append_partition_by_name_with_environment_context(dbName, tableName,
        partName, envContext);
    return deepCopy(p);
  }

  public boolean dropPartitionByName(String dbName, String tableName, String partName,
      boolean deleteData) throws NoSuchObjectException, MetaException, TException {
    return dropPartitionByName(dbName, tableName, partName, deleteData, null);
  }

  public boolean dropPartitionByName(String dbName, String tableName, String partName,
      boolean deleteData, EnvironmentContext envContext) throws NoSuchObjectException,
      MetaException, TException {
    return client.drop_partition_by_name_with_environment_context(dbName, tableName, partName,
            deleteData, envContext);
  }

  private HiveMetaHook getHook(Table tbl) throws MetaException {
    if (hookLoader == null) {
      return null;
    }
    return hookLoader.getHook(tbl);
  }

  /**
   * Check if the current user has access to a given database and table name. Throw
   * NoSuchObjectException if user has no access. When the db or table is filtered out, we don't need
   * to even fetch the partitions. Therefore this check ensures table-level security and
   * could improve performance when filtering partitions.
   *
   * @param catName the catalog name
   * @param dbName  the database name
   * @param tblName the table name contained in the database
   * @throws NoSuchObjectException if the database or table is filtered out
   */
  private void checkDbAndTableFilters(final String catName, final String dbName, final String tblName)
      throws NoSuchObjectException, MetaException {

    // HIVE-20776 causes view access regression
    // Therefore, do not do filtering here. Call following function only to check
    // if dbName and tblName is valid
    FilterUtils.checkDbAndTableFilters(
        false, filterHook, catName, dbName, tblName);
  }

  @Override
  public List<String> partitionNameToVals(String name) throws MetaException, TException {
    return client.partition_name_to_vals(name);
  }

  @Override
  public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
    return client.partition_name_to_spec(name);
  }

  /**
   * @param partition
   * @return
   */
  protected Partition deepCopy(Partition partition) {
    Partition copy = null;
    if (partition != null) {
      copy = new Partition(partition);
    }
    return copy;
  }

  private Database deepCopy(Database database) {
    Database copy = null;
    if (database != null) {
      copy = new Database(database);
    }
    return copy;
  }

  protected Table deepCopy(Table table) {
    Table copy = null;
    if (table != null) {
      copy = new Table(table);
    }
    return copy;
  }

  private Type deepCopy(Type type) {
    Type copy = null;
    if (type != null) {
      copy = new Type(type);
    }
    return copy;
  }

  private FieldSchema deepCopy(FieldSchema schema) {
    FieldSchema copy = null;
    if (schema != null) {
      copy = new FieldSchema(schema);
    }
    return copy;
  }

  private Function deepCopy(Function func) {
    Function copy = null;
    if (func != null) {
      copy = new Function(func);
    }
    return copy;
  }

  protected PrincipalPrivilegeSet deepCopy(PrincipalPrivilegeSet pps) {
    PrincipalPrivilegeSet copy = null;
    if (pps != null) {
      copy = new PrincipalPrivilegeSet(pps);
    }
    return copy;
  }

  protected List<Partition> deepCopyPartitions(List<Partition> partitions) {
    return deepCopyPartitions(partitions, null);
  }

  private List<Partition> deepCopyPartitions(
     Collection<Partition> src, List<Partition> dest) {
    if (src == null) {
      return dest;
    }
    if (dest == null) {
      dest = new ArrayList<Partition>(src.size());
    }
    for (Partition part : src) {
      dest.add(deepCopy(part));
    }
    return dest;
  }

  private List<Table> deepCopyTables(List<Table> tables) {
    List<Table> copy = null;
    if (tables != null) {
      copy = new ArrayList<Table>();
      for (Table tab : tables) {
        copy.add(deepCopy(tab));
      }
    }
    return copy;
  }

  protected List<FieldSchema> deepCopyFieldSchemas(List<FieldSchema> schemas) {
    List<FieldSchema> copy = null;
    if (schemas != null) {
      copy = new ArrayList<FieldSchema>();
      for (FieldSchema schema : schemas) {
        copy.add(deepCopy(schema));
      }
    }
    return copy;
  }

  @Override
  public boolean grant_role(String roleName, String userName,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption) throws MetaException, TException {
    GrantRevokeRoleRequest req = new GrantRevokeRoleRequest();
    req.setRequestType(GrantRevokeType.GRANT);
    req.setRoleName(roleName);
    req.setPrincipalName(userName);
    req.setPrincipalType(principalType);
    req.setGrantor(grantor);
    req.setGrantorType(grantorType);
    req.setGrantOption(grantOption);
    GrantRevokeRoleResponse res = client.grant_revoke_role(req);
    if (!res.isSetSuccess()) {
      throw new MetaException("GrantRevokeResponse missing success field");
    }
    return res.isSuccess();
  }

  @Override
  public boolean create_role(Role role)
      throws MetaException, TException {
    return client.create_role(role);
  }

  @Override
  public boolean drop_role(String roleName) throws MetaException, TException {
    return client.drop_role(roleName);
  }

  @Override
  public List<Role> list_roles(String principalName,
      PrincipalType principalType) throws MetaException, TException {
    return client.list_roles(principalName, principalType);
  }

  @Override
  public List<String> listRoleNames() throws MetaException, TException {
    return client.get_role_names();
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest req)
      throws MetaException, TException {
    return client.get_principals_in_role(req);
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
          GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException {
    return client.get_role_grants_for_principal(getRolePrincReq);
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges)
      throws MetaException, TException {
    String defaultCat = getDefaultCatalog(conf);
    for (HiveObjectPrivilege priv : privileges.getPrivileges()) {
      if (!priv.getHiveObject().isSetCatName()) {
        priv.getHiveObject().setCatName(defaultCat);
      }
    }
    GrantRevokePrivilegeRequest req = new GrantRevokePrivilegeRequest();
    req.setRequestType(GrantRevokeType.GRANT);
    req.setPrivileges(privileges);
    GrantRevokePrivilegeResponse res = client.grant_revoke_privileges(req);
    if (!res.isSetSuccess()) {
      throw new MetaException("GrantRevokePrivilegeResponse missing success field");
    }
    return res.isSuccess();
  }

  @Override
  public boolean revoke_role(String roleName, String userName,
      PrincipalType principalType, boolean grantOption) throws MetaException, TException {
    GrantRevokeRoleRequest req = new GrantRevokeRoleRequest();
    req.setRequestType(GrantRevokeType.REVOKE);
    req.setRoleName(roleName);
    req.setPrincipalName(userName);
    req.setPrincipalType(principalType);
    req.setGrantOption(grantOption);
    GrantRevokeRoleResponse res = client.grant_revoke_role(req);
    if (!res.isSetSuccess()) {
      throw new MetaException("GrantRevokeResponse missing success field");
    }
    return res.isSuccess();
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption) throws MetaException,
      TException {
    String defaultCat = getDefaultCatalog(conf);
    for (HiveObjectPrivilege priv : privileges.getPrivileges()) {
      if (!priv.getHiveObject().isSetCatName()) {
        priv.getHiveObject().setCatName(defaultCat);
      }
    }
    GrantRevokePrivilegeRequest req = new GrantRevokePrivilegeRequest();
    req.setRequestType(GrantRevokeType.REVOKE);
    req.setPrivileges(privileges);
    req.setRevokeGrantOption(grantOption);
    GrantRevokePrivilegeResponse res = client.grant_revoke_privileges(req);
    if (!res.isSetSuccess()) {
      throw new MetaException("GrantRevokePrivilegeResponse missing success field");
    }
    return res.isSuccess();
  }

  @Override
  public boolean refresh_privileges(HiveObjectRef objToRefresh, String authorizer,
      PrivilegeBag grantPrivileges) throws MetaException,
      TException {
    String defaultCat = getDefaultCatalog(conf);
    objToRefresh.setCatName(defaultCat);

    if (grantPrivileges.getPrivileges() != null) {
      for (HiveObjectPrivilege priv : grantPrivileges.getPrivileges()) {
        if (!priv.getHiveObject().isSetCatName()) {
          priv.getHiveObject().setCatName(defaultCat);
        }
      }
    }
    GrantRevokePrivilegeRequest grantReq = new GrantRevokePrivilegeRequest();
    grantReq.setRequestType(GrantRevokeType.GRANT);
    grantReq.setPrivileges(grantPrivileges);

    GrantRevokePrivilegeResponse res = client.refresh_privileges(objToRefresh, authorizer, grantReq);
    if (!res.isSetSuccess()) {
      throw new MetaException("GrantRevokePrivilegeResponse missing success field");
    }
    return res.isSuccess();
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
      String userName, List<String> groupNames) throws MetaException,
      TException {
    if (!hiveObject.isSetCatName()) {
      hiveObject.setCatName(getDefaultCatalog(conf));
    }
    return client.get_privilege_set(hiveObject, userName, groupNames);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String principalName,
      PrincipalType principalType, HiveObjectRef hiveObject)
      throws MetaException, TException {
    if (!hiveObject.isSetCatName()) {
      hiveObject.setCatName(getDefaultCatalog(conf));
    }
    return client.list_privileges(principalName, principalType, hiveObject);
  }

  public String getDelegationToken(String renewerKerberosPrincipalName) throws
      MetaException, TException, IOException {
    //a convenience method that makes the intended owner for the delegation
    //token request the current user
    String owner = SecurityUtils.getUser();
    return getDelegationToken(owner, renewerKerberosPrincipalName);
  }

  @Override
  public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws
      MetaException, TException {
    // This is expected to be a no-op, so we will return null when we use local metastore.
    if (localMetaStore) {
      return null;
    }
    return client.get_delegation_token(owner, renewerKerberosPrincipalName);
  }

  @Override
  public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
    if (localMetaStore) {
      return 0;
    }
    return client.renew_delegation_token(tokenStrForm);

  }

  @Override
  public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
    if (localMetaStore) {
      return;
    }
    client.cancel_delegation_token(tokenStrForm);
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
    return client.add_token(tokenIdentifier, delegationToken);
  }

  @Override
  public boolean removeToken(String tokenIdentifier) throws TException {
    return client.remove_token(tokenIdentifier);
  }

  @Override
  public String getToken(String tokenIdentifier) throws TException {
    return client.get_token(tokenIdentifier);
  }

  @Override
  public List<String> getAllTokenIdentifiers() throws TException {
    return client.get_all_token_identifiers();
  }

  @Override
  public int addMasterKey(String key) throws MetaException, TException {
    return client.add_master_key(key);
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key)
      throws NoSuchObjectException, MetaException, TException {
    client.update_master_key(seqNo, key);
  }

  @Override
  public boolean removeMasterKey(Integer keySeq) throws TException {
    return client.remove_master_key(keySeq);
  }

  @Override
  public String[] getMasterKeys() throws TException {
    List<String> keyList = client.get_master_keys();
    return keyList.toArray(new String[keyList.size()]);
  }

  @Override
  public GetOpenTxnsResponse getOpenTxns() throws TException {
    GetOpenTxnsRequest getOpenTxnsRequest = new GetOpenTxnsRequest();
    getOpenTxnsRequest.setExcludeTxnTypes(Arrays.asList(TxnType.READ_ONLY));
    return client.get_open_txns_req(getOpenTxnsRequest);
  }

  @Override
  public ValidTxnList getValidTxns() throws TException {
    GetOpenTxnsRequest getOpenTxnsRequest = new GetOpenTxnsRequest();
    getOpenTxnsRequest.setExcludeTxnTypes(Arrays.asList(TxnType.READ_ONLY));
    return TxnCommonUtils.createValidReadTxnList(client.get_open_txns_req(getOpenTxnsRequest), 0);
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn) throws TException {
    GetOpenTxnsRequest getOpenTxnsRequest = new GetOpenTxnsRequest();
    getOpenTxnsRequest.setExcludeTxnTypes(Arrays.asList(TxnType.READ_ONLY));
    return TxnCommonUtils.createValidReadTxnList(client.get_open_txns_req(getOpenTxnsRequest), currentTxn);
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn, List<TxnType> excludeTxnTypes) throws TException {
    GetOpenTxnsRequest getOpenTxnsRequest = new GetOpenTxnsRequest();
    getOpenTxnsRequest.setExcludeTxnTypes(excludeTxnTypes);
    return TxnCommonUtils.createValidReadTxnList(client.get_open_txns_req(getOpenTxnsRequest),
      currentTxn);
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
    GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(Collections.singletonList(fullTableName));
    GetValidWriteIdsResponse validWriteIds = getValidWriteIdsInternal(rqst);
    return TxnCommonUtils.createValidReaderWriteIdList(validWriteIds.getTblValidWriteIds().get(0));
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName, Long writeId) throws TException {
    GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(Collections.singletonList(fullTableName));
    rqst.setWriteId(writeId);
    GetValidWriteIdsResponse validWriteIds = getValidWriteIdsInternal(rqst);
    return TxnCommonUtils.createValidReaderWriteIdList(validWriteIds.getTblValidWriteIds().get(0));
  }

  @Override
  public List<TableValidWriteIds> getValidWriteIds(
      List<String> tablesList, String validTxnList) throws TException {
    GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(tablesList);
    rqst.setValidTxnList(validTxnList);
    return getValidWriteIdsInternal(rqst).getTblValidWriteIds();
  }

  protected GetValidWriteIdsResponse getValidWriteIdsInternal(GetValidWriteIdsRequest rqst) throws TException {
    return client.get_valid_write_ids(rqst);
  }

  @Override
  public void addWriteIdsToMinHistory(long txnId, Map<String, Long> writeIds) throws TException {
    client.add_write_ids_to_min_history(txnId, writeIds);
  }

  @Override
  public long openTxn(String user) throws TException {
    OpenTxnsResponse txns = openTxnsIntr(user, 1, null, null, null);
    return txns.getTxn_ids().get(0);
  }

  @Override
  public long openTxn(String user, TxnType txnType) throws TException {
    OpenTxnsResponse txns = openTxnsIntr(user, 1, null, null, txnType);
    return txns.getTxn_ids().get(0);
  }

  @Override
  public List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user, TxnType txnType) throws TException {
    // As this is called from replication task, the user is the user who has fired the repl command.
    // This is required for standalone metastore authentication.
    OpenTxnsResponse txns = openTxnsIntr(user, srcTxnIds != null ? srcTxnIds.size() : 1,
                                        replPolicy, srcTxnIds, txnType);
    return txns.getTxn_ids();
  }

  @Override
  public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    return openTxnsIntr(user, numTxns, null, null, null);
  }

  private OpenTxnsResponse openTxnsIntr(String user, int numTxns, String replPolicy,
                                        List<Long> srcTxnIds, TxnType txnType) throws TException {
    String hostname;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to resolve my host name " + e.getMessage());
      throw new RuntimeException(e);
    }
    OpenTxnRequest rqst = new OpenTxnRequest(numTxns, user, hostname);
    if (replPolicy != null) {
      rqst.setReplPolicy(replPolicy);
      if (txnType == TxnType.REPL_CREATED) {
        assert srcTxnIds != null;
        assert numTxns == srcTxnIds.size();
        rqst.setReplSrcTxnIds(srcTxnIds);
      }
    } else {
      assert srcTxnIds == null;
    }
    if (txnType != null) {
      rqst.setTxn_type(txnType);
    }
    return client.open_txns(rqst);
  }

  @Override
  public void rollbackTxn(long txnid) throws NoSuchTxnException, TException {
    client.abort_txn(new AbortTxnRequest(txnid));
  }

  @Override
  public void rollbackTxn(AbortTxnRequest abortTxnRequest) throws NoSuchTxnException, TException {
    client.abort_txn(abortTxnRequest);
  }

  @Override
  public void replRollbackTxn(long srcTxnId, String replPolicy, TxnType txnType) throws NoSuchTxnException, TException {
    AbortTxnRequest rqst = new AbortTxnRequest(srcTxnId);
    rqst.setReplPolicy(replPolicy);
    rqst.setTxn_type(txnType);
    client.abort_txn(rqst);
  }

  @Override
  public void commitTxn(long txnid)
          throws NoSuchTxnException, TxnAbortedException, TException {
    client.commit_txn(new CommitTxnRequest(txnid));
  }

  @Override
  public void commitTxnWithKeyValue(long txnid, long tableId, String key,
      String value) throws NoSuchTxnException,
      TxnAbortedException, TException {
    CommitTxnRequest ctr = new CommitTxnRequest(txnid);
    Preconditions.checkNotNull(key, "The key to commit together"
       + " with the transaction can't be null");
    Preconditions.checkNotNull(value, "The value to commit together"
       + " with the transaction can't be null");
    ctr.setKeyValue(new CommitTxnKeyValue(tableId, key, value));

    client.commit_txn(ctr);
  }

  @Override
  public void commitTxn(CommitTxnRequest rqst)
          throws NoSuchTxnException, TxnAbortedException, TException {
    client.commit_txn(rqst);
  }

  @Override
  public GetOpenTxnsInfoResponse showTxns() throws TException {
    return client.get_open_txns_info();
  }

  @Override
  public void abortTxns(List<Long> txnids) throws NoSuchTxnException, TException {
    client.abort_txns(new AbortTxnsRequest(txnids));
  }

  @Override
  public void abortTxns(AbortTxnsRequest abortTxnsRequest) throws NoSuchTxnException, TException {
    client.abort_txns(abortTxnsRequest);
  }

  @Override
  public void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames)
          throws TException {
    String user;
    try {
      user = UserGroupInformation.getCurrentUser().getUserName();
    } catch (IOException e) {
      LOG.error("Unable to resolve current user name " + e.getMessage());
      throw new RuntimeException(e);
    }

    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to resolve my host name " + e.getMessage());
      throw new RuntimeException(e);
    }

    ReplTblWriteIdStateRequest rqst
            = new ReplTblWriteIdStateRequest(validWriteIdList, user, hostName, dbName, tableName);
    if (partNames != null) {
      rqst.setPartNames(partNames);
    }
    client.repl_tbl_writeid_state(rqst);
  }

  @Override
  public long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
    return allocateTableWriteId(txnId, dbName, tableName, false);
  }

  @Override
  public long allocateTableWriteId(long txnId, String dbName, String tableName, boolean shouldRealloc) throws TException {
    return allocateTableWriteIdsBatch(Collections.singletonList(txnId), dbName, tableName, shouldRealloc).get(0).getWriteId();
  }


  @Override
  public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName) throws TException {
    return allocateTableWriteIdsBatch(txnIds, dbName, tableName, false);
  }

  private List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName,
      boolean shouldRealloc) throws TException {
    AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(dbName, tableName);
    rqst.setTxnIds(txnIds);
    rqst.setReallocate(shouldRealloc);
    return allocateTableWriteIdsBatchIntr(rqst);
  }

  @Override
  public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName,
      String replPolicy, List<TxnToWriteId> srcTxnToWriteIdList) throws TException {
    AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(dbName, tableName);
    rqst.setReplPolicy(replPolicy);
    rqst.setSrcTxnToWriteIdList(srcTxnToWriteIdList);
    return allocateTableWriteIdsBatchIntr(rqst);
  }

  private List<TxnToWriteId> allocateTableWriteIdsBatchIntr(AllocateTableWriteIdsRequest rqst) throws TException {
    return client.allocate_table_write_ids(rqst).getTxnToWriteIds();
  }

  @Override
  public long getMaxAllocatedWriteId(String dbName, String tableName) throws TException {
    return client.get_max_allocated_table_write_id(new MaxAllocatedTableWriteIdRequest(dbName, tableName)).getMaxWriteId();
  }

  @Override
  public void seedWriteId(String dbName, String tableName, long seedWriteId) throws TException {
    client.seed_write_id(new SeedTableWriteIdsRequest(dbName, tableName, seedWriteId));
  }

  @Override
  public void seedTxnId(long seedTxnId) throws TException {
    client.seed_txn_id(new SeedTxnIdRequest(seedTxnId));
  }

  @Override
  public LockResponse lock(LockRequest request)
      throws NoSuchTxnException, TxnAbortedException, TException {
    return client.lock(request);
  }

  @Override
  public LockResponse checkLock(long lockid)
      throws NoSuchTxnException, TxnAbortedException, NoSuchLockException,
      TException {
    return client.check_lock(new CheckLockRequest(lockid));
  }

  @Override
  public void unlock(long lockid)
      throws NoSuchLockException, TxnOpenException, TException {
    client.unlock(new UnlockRequest(lockid));
  }

  @Override
  @Deprecated
  public ShowLocksResponse showLocks() throws TException {
    return client.show_locks(new ShowLocksRequest());
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest request) throws TException {
    return client.show_locks(request);
  }

  @Override
  public void heartbeat(long txnid, long lockid)
      throws NoSuchLockException, NoSuchTxnException, TxnAbortedException,
      TException {
    HeartbeatRequest hb = new HeartbeatRequest();
    hb.setLockid(lockid);
    hb.setTxnid(txnid);
    client.heartbeat(hb);
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max)
      throws NoSuchTxnException, TxnAbortedException, TException {
    HeartbeatTxnRangeRequest rqst = new HeartbeatTxnRangeRequest(min, max);
    return client.heartbeat_txn_range(rqst);
  }

  @Override
  @Deprecated
  public void compact(String dbname, String tableName, String partitionName, CompactionType type)
      throws TException {
    CompactionRequest cr = new CompactionRequest();
    if (dbname == null) {
      cr.setDbname(DEFAULT_DATABASE_NAME);
    } else {
      cr.setDbname(dbname);
    }
    cr.setTablename(tableName);
    if (partitionName != null) {
      cr.setPartitionname(partitionName);
    }
    cr.setType(type);
    client.compact(cr);
  }

  @Deprecated
  @Override
  public void compact(String dbname, String tableName, String partitionName, CompactionType type,
                      Map<String, String> tblproperties) throws TException {
    compact2(dbname, tableName, partitionName, type, tblproperties);
  }

  @Override
  public CompactionResponse compact2(String dbname, String tableName, String partitionName, CompactionType type,
      Map<String, String> tblproperties) throws TException {
    CompactionRequest cr = new CompactionRequest();
    if (dbname == null) {
      cr.setDbname(DEFAULT_DATABASE_NAME);
    } else {
      cr.setDbname(dbname);
    }
    cr.setTablename(tableName);
    if (partitionName != null) {
      cr.setPartitionname(partitionName);
    }
    cr.setType(type);
    cr.setProperties(tblproperties);
    cr.setInitiatorId(JavaUtils.hostname() + "-" + MANUALLY_INITIATED_COMPACTION);
    cr.setInitiatorVersion(HiveMetaStoreClient.class.getPackage().getImplementationVersion());
    return client.compact2(cr);
  }

  @Override
  public CompactionResponse compact2(CompactionRequest request) throws TException {
    return client.compact2(request);
  }

  @Override
  public ShowCompactResponse showCompactions() throws TException {
    ShowCompactResponse response = client.show_compact(new ShowCompactRequest());
    response.setCompacts(FilterUtils.filterCompactionsIfEnabled(isClientFilterEnabled,
            filterHook, getDefaultCatalog(conf), response.getCompacts()));
    return response;
  }

  @Override public ShowCompactResponse showCompactions(ShowCompactRequest request) throws TException {
    ShowCompactResponse response = client.show_compact(request);
    response.setCompacts(FilterUtils.filterCompactionsIfEnabled(isClientFilterEnabled,
            filterHook, getDefaultCatalog(conf), response.getCompacts()));
    return response;
  }

  @Override
  public boolean submitForCleanup(CompactionRequest rqst, long highestWriteId,
                                  long txnId) throws TException {
    return client.submit_for_cleanup(rqst, highestWriteId, txnId);
  }

  @Override
  public GetLatestCommittedCompactionInfoResponse getLatestCommittedCompactionInfo(
      GetLatestCommittedCompactionInfoRequest request)
      throws TException {
    GetLatestCommittedCompactionInfoResponse response = client.get_latest_committed_compaction_info(request);
    return FilterUtils.filterCommittedCompactionInfoStructIfEnabled(isClientFilterEnabled, filterHook,
        getDefaultCatalog(conf), request.getDbname(), request.getTablename(), response);
  }

  @Deprecated
  @Override
  public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName,
                                   List<String> partNames) throws TException {
    client.add_dynamic_partitions(new AddDynamicPartitions(txnId, writeId, dbName, tableName, partNames));
  }

  @Override
  public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName,
                                   List<String> partNames, DataOperationType operationType) throws TException {
    AddDynamicPartitions adp = new AddDynamicPartitions(txnId, writeId, dbName, tableName, partNames);
    adp.setOperationType(operationType);
    client.add_dynamic_partitions(adp);
  }

  @Override
  public void insertTable(Table table, boolean overwrite) throws MetaException {
    boolean failed = true;
    HiveMetaHook hook = getHook(table);
    if (hook == null || !(hook instanceof DefaultHiveMetaHook)) {
      return;
    }
    DefaultHiveMetaHook hiveMetaHook = (DefaultHiveMetaHook) hook;
    try {
      hiveMetaHook.commitInsertTable(table, overwrite);
      failed = false;
    } finally {
      if (failed) {
        hiveMetaHook.rollbackInsertTable(table, overwrite);
      }
    }
  }

  @Override
  public long getLatestTxnIdInConflict(long txnId) throws TException {
    return client.get_latest_txnid_in_conflict(txnId);
  }

  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  @Override
  public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents,
                                                       NotificationFilter filter) throws TException {
    NotificationEventRequest rqst = new NotificationEventRequest(lastEventId);
    rqst.setMaxEvents(maxEvents);
    return getNextNotificationsInternal(rqst, false, filter);
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest request,
      boolean allowGapsInEventIds, NotificationFilter filter) throws TException {
    return getNextNotificationsInternal(request, allowGapsInEventIds, filter);
  }

  @Nullable
  private NotificationEventResponse getNextNotificationsInternal(
      NotificationEventRequest rqst, boolean allowGapsInEventIds,
      NotificationFilter filter) throws TException {
    long lastEventId = rqst.getLastEvent();
    NotificationEventResponse rsp = client.get_next_notification(rqst);
    LOG.debug("Got back {} events", rsp!= null ? rsp.getEventsSize() : 0);
    NotificationEventResponse filtered = new NotificationEventResponse();
    if (rsp != null && rsp.getEvents() != null) {
      long nextEventId = lastEventId + 1;
      long prevEventId = lastEventId;
      for (NotificationEvent e : rsp.getEvents()) {
        LOG.debug("Got event with id : {}", e.getEventId());
        if (!allowGapsInEventIds && e.getEventId() != nextEventId) {
          if (e.getEventId() == prevEventId) {
            LOG.error("NOTIFICATION_LOG table has multiple events with the same event Id {}. " +
                    "Something went wrong when inserting notification events.  Bootstrap the system " +
                    "again to get back teh consistent replicated state.", prevEventId);
            throw new IllegalStateException(REPL_EVENTS_WITH_DUPLICATE_ID_IN_METASTORE);
          } else {
            LOG.error("Requested events are found missing in NOTIFICATION_LOG table. Expected: {}, Actual: {}. "
                            + "Probably, cleaner would've cleaned it up. "
                            + "Try setting higher value for hive.metastore.event.db.listener.timetolive. "
                            + "Also, bootstrap the system again to get back the consistent replicated state.",
                    nextEventId, e.getEventId());
            throw new IllegalStateException(REPL_EVENTS_MISSING_IN_METASTORE);
          }
        }
        if ((filter != null) && filter.accept(e)) {
          filtered.addToEvents(e);
        }
        prevEventId = nextEventId;
        nextEventId++;
      }
    }
    return (filter != null) ? filtered : rsp;
  }

  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    return client.get_current_notificationEventId();
  }

  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst)
          throws TException {
    if (!rqst.isSetCatName()) {
      rqst.setCatName(getDefaultCatalog(conf));
    }
    return client.get_notification_events_count(rqst);
  }

  @InterfaceAudience.LimitedPrivate({"Apache Hive, HCatalog"})
  @Override
  public FireEventResponse fireListenerEvent(FireEventRequest rqst) throws TException {
    if (!rqst.isSetCatName()) {
      rqst.setCatName(getDefaultCatalog(conf));
    }
    return client.fire_listener_event(rqst);
  }

  @InterfaceAudience.LimitedPrivate({"Apache Hive, HCatalog"})
  @Override
  public void addWriteNotificationLog(WriteNotificationLogRequest rqst) throws TException {
    client.add_write_notification_log(rqst);
  }

  @InterfaceAudience.LimitedPrivate({"Apache Hive, HCatalog"})
  @Override
  public void addWriteNotificationLogInBatch(WriteNotificationLogBatchRequest rqst) throws TException {
    client.add_write_notification_log_in_batch(rqst);
  }

  /**
   * Creates a synchronized wrapper for any {@link IMetaStoreClient}.
   * This may be used by multi-threaded applications until we have
   * fixed all reentrancy bugs.
   *
   * @param client unsynchronized client
   * @return synchronized client
   */
  public static IMetaStoreClient newSynchronizedClient(
      IMetaStoreClient client) {
    return (IMetaStoreClient) Proxy.newProxyInstance(
      HiveMetaStoreClient.class.getClassLoader(),
      new Class[]{IMetaStoreClient.class},
      new SynchronizedHandler(client));
  }

  private static class SynchronizedHandler implements InvocationHandler {
    private final IMetaStoreClient client;

    SynchronizedHandler(IMetaStoreClient client) {
      this.client = client;
    }

    @Override
    public synchronized Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      try {
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        throw e.getTargetException();
      }
    }
  }

  @Override
  public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType)
      throws TException {
    markPartitionForEvent(getDefaultCatalog(conf), db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public void markPartitionForEvent(String catName, String db_name, String tbl_name,
                                    Map<String, String> partKVs,
                                    PartitionEventType eventType) throws TException {
    client.markPartitionForEvent(prependCatalogToDbName(catName, db_name, conf), tbl_name, partKVs,
        eventType);

  }

  @Override
  public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType)
      throws TException {
    return isPartitionMarkedForEvent(getDefaultCatalog(conf), db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String catName, String db_name, String tbl_name,
                                           Map<String, String> partKVs,
                                           PartitionEventType eventType) throws TException {
    return client.isPartitionMarkedForEvent(prependCatalogToDbName(catName, db_name, conf), tbl_name,
        partKVs, eventType);
  }

  @Override
  public void createFunction(Function func) throws TException {
    if (func == null) {
      throw new MetaException("Function cannot be null.");
    }
    if (!func.isSetCatName()) {
      func.setCatName(getDefaultCatalog(conf));
    }
    client.create_function(func);
  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws TException {
    alterFunction(getDefaultCatalog(conf), dbName, funcName, newFunction);
  }

  @Override
  public void alterFunction(String catName, String dbName, String funcName,
                            Function newFunction) throws TException {
    client.alter_function(prependCatalogToDbName(catName, dbName, conf), funcName, newFunction);
  }

  @Override
  public void dropFunction(String dbName, String funcName) throws TException {
    dropFunction(getDefaultCatalog(conf), dbName, funcName);
  }

  @Override
  public void dropFunction(String catName, String dbName, String funcName) throws TException {
    client.drop_function(prependCatalogToDbName(catName, dbName, conf), funcName);
  }

  @Override
  public Function getFunction(String dbName, String funcName) throws TException {
    return getFunction(getDefaultCatalog(conf), dbName, funcName);
  }

  @Override
  public Function getFunction(String catName, String dbName, String funcName) throws TException {
    return deepCopy(client.get_function(prependCatalogToDbName(catName, dbName, conf), funcName));
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws TException {
    return getFunctions(getDefaultCatalog(conf), dbName, pattern);
  }

  @Override
  public List<String> getFunctions(String catName, String dbName, String pattern) throws TException {
    return client.get_functions(prependCatalogToDbName(catName, dbName, conf), pattern);
  }

  @Override
  public GetAllFunctionsResponse getAllFunctions() throws TException {
    return client.get_all_functions();
  }

  protected void create_table(CreateTableRequest request) throws
      InvalidObjectException, MetaException, NoSuchObjectException, TException {
    client.create_table_req(request);
  }

  protected void drop_table_with_environment_context(String catName, String dbname, String name,
      boolean deleteData, EnvironmentContext envContext) throws TException {
    client.drop_table_with_environment_context(prependCatalogToDbName(catName, dbname, conf),
        name, deleteData, envContext);
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName,
    List<String> colNames, List<String> partNames, String engine) throws NoSuchObjectException, MetaException, TException {
    return getAggrColStatsFor(getDefaultCatalog(conf), dbName, tblName, colNames, partNames, engine);
  }

  @Override
  public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName,
      List<String> colNames, List<String> partNames, String engine) throws TException {
    long t1 = System.currentTimeMillis();

    try {
      if (colNames.isEmpty() || partNames.isEmpty()) {
        LOG.debug("Columns is empty or partNames is empty : Short-circuiting stats eval on client side.");
        return new AggrStats(new ArrayList<>(), 0); // Nothing to aggregate
      }
      PartitionsStatsRequest req = new PartitionsStatsRequest(dbName, tblName, colNames, partNames);
      req.setEngine(engine);
      req.setCatName(catName);
      req.setValidWriteIdList(getValidWriteIdList(dbName, tblName));

      return getAggrStatsForInternal(req);
    } finally {
      long diff = System.currentTimeMillis() - t1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("class={}, method={}, duration={}, comments={}", CLASS_NAME, "getAggrColStatsFor",
            diff, "HMS client");
      }
    }
  }

  @Override
  public Iterable<Entry<Long, ByteBuffer>> getFileMetadata(
      final List<Long> fileIds) throws TException {
    return new MetastoreMapIterable<Long, ByteBuffer>() {
      private int listIndex = 0;
      @Override
      protected Map<Long, ByteBuffer> fetchNextBatch() throws TException {
        if (listIndex == fileIds.size()) {
          return null;
        }
        int endIndex = Math.min(listIndex + fileMetadataBatchSize, fileIds.size());
        List<Long> subList = fileIds.subList(listIndex, endIndex);
        GetFileMetadataResult resp = sendGetFileMetadataReq(subList);
        // TODO: we could remember if it's unsupported and stop sending calls; although, it might
        //       be a bad idea for HS2+standalone metastore that could be updated with support.
        //       Maybe we should just remember this for some time.
        if (!resp.isIsSupported()) {
          return null;
        }
        listIndex = endIndex;
        return resp.getMetadata();
      }
    };
  }

  private GetFileMetadataResult sendGetFileMetadataReq(List<Long> fileIds) throws TException {
    return client.get_file_metadata(new GetFileMetadataRequest(fileIds));
  }

  @Override
  public Iterable<Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
      final List<Long> fileIds, final ByteBuffer sarg, final boolean doGetFooters)
          throws TException {
    return new MetastoreMapIterable<Long, MetadataPpdResult>() {
      private int listIndex = 0;
      @Override
      protected Map<Long, MetadataPpdResult> fetchNextBatch() throws TException {
        if (listIndex == fileIds.size()) {
          return null;
        }
        int endIndex = Math.min(listIndex + fileMetadataBatchSize, fileIds.size());
        List<Long> subList = fileIds.subList(listIndex, endIndex);
        GetFileMetadataByExprResult resp = sendGetFileMetadataBySargReq(
            sarg, subList, doGetFooters);
        if (!resp.isIsSupported()) {
          return null;
        }
        listIndex = endIndex;
        return resp.getMetadata();
      }
    };
  }

  private GetFileMetadataByExprResult sendGetFileMetadataBySargReq(
      ByteBuffer sarg, List<Long> fileIds, boolean doGetFooters) throws TException {
    GetFileMetadataByExprRequest req = new GetFileMetadataByExprRequest(fileIds, sarg);
    req.setDoGetFooters(doGetFooters); // No need to get footers
    return client.get_file_metadata_by_expr(req);
  }

  public static abstract class MetastoreMapIterable<K, V>
    implements Iterable<Entry<K, V>>, Iterator<Entry<K, V>> {
    private Iterator<Entry<K, V>> currentIter;

    protected abstract Map<K, V> fetchNextBatch() throws TException;

    @Override
    public Iterator<Entry<K, V>> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      ensureCurrentBatch();
      return currentIter != null;
    }

    private void ensureCurrentBatch() {
      if (currentIter != null && currentIter.hasNext()) {
        return;
      }
      currentIter = null;
      Map<K, V> currentBatch;
      do {
        try {
          currentBatch = fetchNextBatch();
        } catch (TException ex) {
          throw new RuntimeException(ex);
        }
        if (currentBatch == null)
         {
          return; // No more data.
        }
      } while (currentBatch.isEmpty());
      currentIter = currentBatch.entrySet().iterator();
    }

    @Override
    public Entry<K, V> next() {
      ensureCurrentBatch();
      if (currentIter == null) {
        throw new NoSuchElementException();
      }
      return currentIter.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void clearFileMetadata(List<Long> fileIds) throws TException {
    ClearFileMetadataRequest req = new ClearFileMetadataRequest();
    req.setFileIds(fileIds);
    client.clear_file_metadata(req);
  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
    PutFileMetadataRequest req = new PutFileMetadataRequest();
    req.setFileIds(fileIds);
    req.setMetadata(metadata);
    client.put_file_metadata(req);
  }

  @Override
  public boolean isSameConfObj(Configuration c) {
    return conf == c;
  }

  @Override
  public boolean cacheFileMetadata(
      String dbName, String tableName, String partName, boolean allParts) throws TException {
    CacheFileMetadataRequest req = new CacheFileMetadataRequest();
    req.setDbName(dbName);
    req.setTblName(tableName);
    if (partName != null) {
      req.setPartName(partName);
    } else {
      req.setIsAllParts(allParts);
    }
    CacheFileMetadataResult result = client.cache_file_metadata(req);
    return result.isIsSupported();
  }

  @Override
  public String getMetastoreDbUuid() throws TException {
    return client.get_metastore_db_uuid();
  }

  @Override
  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName)
      throws InvalidObjectException, MetaException, TException {
    WMCreateResourcePlanRequest request = new WMCreateResourcePlanRequest();
    request.setResourcePlan(resourcePlan);
    request.setCopyFrom(copyFromName);
    client.create_resource_plan(request);
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException, TException {
    WMGetResourcePlanRequest request = new WMGetResourcePlanRequest();
    request.setResourcePlanName(resourcePlanName);
    request.setNs(ns);
    return client.get_resource_plan(request).getResourcePlan();
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans(String ns)
      throws NoSuchObjectException, MetaException, TException {
    WMGetAllResourcePlanRequest request = new WMGetAllResourcePlanRequest();
    request.setNs(ns);
    return client.get_all_resource_plans(request).getResourcePlans();
  }

  @Override
  public void dropResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException, TException {
    WMDropResourcePlanRequest request = new WMDropResourcePlanRequest();
    request.setResourcePlanName(resourcePlanName);
    request.setNs(ns);
    client.drop_resource_plan(request);
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String resourcePlanName, String ns,
      WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean isForceDeactivate, boolean isReplace)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMAlterResourcePlanRequest request = new WMAlterResourcePlanRequest();
    request.setResourcePlanName(resourcePlanName);
    request.setNs(ns);
    request.setResourcePlan(resourcePlan);
    request.setIsEnableAndActivate(canActivateDisabled);
    request.setIsForceDeactivate(isForceDeactivate);
    request.setIsReplace(isReplace);
    WMAlterResourcePlanResponse resp = client.alter_resource_plan(request);
    return resp.isSetFullResourcePlan() ? resp.getFullResourcePlan() : null;
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan(String ns) throws MetaException, TException {
    WMGetActiveResourcePlanRequest request = new WMGetActiveResourcePlanRequest();
    request.setNs(ns);
    return client.get_active_resource_plan(request).getResourcePlan();
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMValidateResourcePlanRequest request = new WMValidateResourcePlanRequest();
    request.setResourcePlanName(resourcePlanName);
    request.setNs(ns);
    return client.validate_resource_plan(request);
  }

  @Override
  public void createWMTrigger(WMTrigger trigger)
      throws InvalidObjectException, MetaException, TException {
    WMCreateTriggerRequest request = new WMCreateTriggerRequest();
    request.setTrigger(trigger);
    client.create_wm_trigger(request);
  }

  @Override
  public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMAlterTriggerRequest request = new WMAlterTriggerRequest();
    request.setTrigger(trigger);
    client.alter_wm_trigger(request);
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName, String ns)
      throws NoSuchObjectException, MetaException, TException {
    WMDropTriggerRequest request = new WMDropTriggerRequest();
    request.setResourcePlanName(resourcePlanName);
    request.setTriggerName(triggerName);
    request.setNs(ns);
    client.drop_wm_trigger(request);
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlan, String ns)
      throws NoSuchObjectException, MetaException, TException {
    WMGetTriggersForResourePlanRequest request = new WMGetTriggersForResourePlanRequest();
    request.setResourcePlanName(resourcePlan);
    request.setNs(ns);
    return client.get_triggers_for_resourceplan(request).getTriggers();
  }

  @Override
  public void createWMPool(WMPool pool)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMCreatePoolRequest request = new WMCreatePoolRequest();
    request.setPool(pool);
    client.create_wm_pool(request);
  }

  @Override
  public void alterWMPool(WMNullablePool pool, String poolPath)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMAlterPoolRequest request = new WMAlterPoolRequest();
    request.setPool(pool);
    request.setPoolPath(poolPath);
    client.alter_wm_pool(request);
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath, String ns)
      throws NoSuchObjectException, MetaException, TException {
    WMDropPoolRequest request = new WMDropPoolRequest();
    request.setResourcePlanName(resourcePlanName);
    request.setPoolPath(poolPath);
    request.setNs(ns);
    client.drop_wm_pool(request);
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMCreateOrUpdateMappingRequest request = new WMCreateOrUpdateMappingRequest();
    request.setMapping(mapping);
    request.setUpdate(isUpdate);
    client.create_or_update_wm_mapping(request);
  }

  @Override
  public void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, MetaException, TException {
    WMDropMappingRequest request = new WMDropMappingRequest();
    request.setMapping(mapping);
    client.drop_wm_mapping(request);
  }

  @Override
  public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, boolean shouldDrop, String ns) throws AlreadyExistsException, NoSuchObjectException,
      InvalidObjectException, MetaException, TException {
    WMCreateOrDropTriggerToPoolMappingRequest request = new WMCreateOrDropTriggerToPoolMappingRequest();
    request.setResourcePlanName(resourcePlanName);
    request.setTriggerName(triggerName);
    request.setPoolPath(poolPath);
    request.setDrop(shouldDrop);
    request.setNs(ns);
    client.create_or_drop_wm_trigger_to_pool_mapping(request);
  }

  @Override
  public void createISchema(ISchema schema) throws TException {
    if (!schema.isSetCatName()) {
      schema.setCatName(getDefaultCatalog(conf));
    }
    client.create_ischema(schema);
  }

  @Override
  public void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema) throws TException {
    client.alter_ischema(new AlterISchemaRequest(new ISchemaName(catName, dbName, schemaName), newSchema));
  }

  @Override
  public ISchema getISchema(String catName, String dbName, String name) throws TException {
    return client.get_ischema(new ISchemaName(catName, dbName, name));
  }

  @Override
  public void dropISchema(String catName, String dbName, String name) throws TException {
    client.drop_ischema(new ISchemaName(catName, dbName, name));
  }

  @Override
  public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {
    if (!schemaVersion.getSchema().isSetCatName()) {
      schemaVersion.getSchema().setCatName(getDefaultCatalog(conf));
    }
    client.add_schema_version(schemaVersion);
  }

  @Override
  public SchemaVersion getSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {
    return client.get_schema_version(new SchemaVersionDescriptor(new ISchemaName(catName, dbName, schemaName), version));
  }

  @Override
  public SchemaVersion getSchemaLatestVersion(String catName, String dbName, String schemaName) throws TException {
    return client.get_schema_latest_version(new ISchemaName(catName, dbName, schemaName));
  }

  @Override
  public List<SchemaVersion> getSchemaAllVersions(String catName, String dbName, String schemaName) throws TException {
    return client.get_schema_all_versions(new ISchemaName(catName, dbName, schemaName));
  }

  @Override
  public void dropSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {
    client.drop_schema_version(new SchemaVersionDescriptor(new ISchemaName(catName, dbName, schemaName), version));
  }

  @Override
  public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst rqst) throws TException {
    return client.get_schemas_by_cols(rqst);
  }

  @Override
  public void mapSchemaVersionToSerde(String catName, String dbName, String schemaName, int version, String serdeName)
      throws TException {
    client.map_schema_version_to_serde(new MapSchemaVersionToSerdeRequest(
        new SchemaVersionDescriptor(new ISchemaName(catName, dbName, schemaName), version), serdeName));
  }

  @Override
  public void setSchemaVersionState(String catName, String dbName, String schemaName, int version, SchemaVersionState state)
      throws TException {
    client.set_schema_version_state(new SetSchemaVersionStateRequest(new SchemaVersionDescriptor(
        new ISchemaName(catName, dbName, schemaName), version), state));
  }

  @Override
  public void addSerDe(SerDeInfo serDeInfo) throws TException {
    client.add_serde(serDeInfo);
  }

  @Override
  public SerDeInfo getSerDe(String serDeName) throws TException {
    return client.get_serde(new GetSerdeRequest(serDeName));
  }

  /**
   * This method is called to get the ValidWriteIdList in order to send the same in HMS get_* APIs,
   * if the validWriteIdList is not explicitly passed (as a method argument) to the HMS APIs.
   * This method returns the ValidWriteIdList based on the VALID_TABLES_WRITEIDS_KEY key.
   * Since, VALID_TABLES_WRITEIDS_KEY is set during the lock acquisition phase after query compilation
   * ( DriverTxnHandler.acquireLocks -&gt; recordValidWriteIds -&gt; setValidWriteIds ),
   * this only covers a subset of cases, where we invoke get_* APIs after query compilation,
   * if the validWriteIdList is not explicitly passed (as a method argument) to the HMS APIs.
   */
  protected String getValidWriteIdList(String dbName, String tblName) {
    if (conf.get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY) == null) {
      return null;
    }

    ValidTxnWriteIdList validTxnWriteIdList = new ValidTxnWriteIdList(
        conf.get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY));
    ValidWriteIdList writeIdList = validTxnWriteIdList.getTableValidWriteIdList(
        TableName.getDbTable(dbName, tblName));
    return writeIdList!=null?writeIdList.toString():null;
  }

  private short shrinkMaxtoShort(int max) {
    if (max < 0) {
      return -1;
    } else if (max <= Short.MAX_VALUE) {
      return (short)max;
    } else {
      return Short.MAX_VALUE;
    }
  }

  @Override
  public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
    return client.get_lock_materialization_rebuild(dbName, tableName, txnId);
  }

  @Override
  public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
    return client.heartbeat_lock_materialization_rebuild(dbName, tableName, txnId);
  }

  @Override
  public void addRuntimeStat(RuntimeStat stat) throws TException {
    client.add_runtime_stats(stat);
  }

  @Override
  public List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException {
    GetRuntimeStatsRequest req = new GetRuntimeStatsRequest();
    req.setMaxWeight(maxWeight);
    req.setMaxCreateTime(maxCreateTime);
    return client.get_runtime_stats(req);
  }

  @Override
  public GetPartitionsResponse getPartitionsWithSpecs(GetPartitionsRequest request)
      throws TException {
    if (processorCapabilities != null)
      request.setProcessorCapabilities(new ArrayList<String>(Arrays.asList(processorCapabilities)));
    if (processorIdentifier != null)
      request.setProcessorIdentifier(processorIdentifier);
    if (request.isSetProjectionSpec()) {
      if (!request.getProjectionSpec().isSetExcludeParamKeyPattern()) {
        request.getProjectionSpec().setExcludeParamKeyPattern(MetastoreConf.getAsString(conf,
            MetastoreConf.ConfVars.METASTORE_PARTITIONS_PARAMETERS_EXCLUDE_PATTERN));
      }
      if (!request.getProjectionSpec().isSetIncludeParamKeyPattern()) {
        request.getProjectionSpec().setIncludeParamKeyPattern(MetastoreConf.getAsString(conf,
            MetastoreConf.ConfVars.METASTORE_PARTITIONS_PARAMETERS_INCLUDE_PATTERN));
      }
    }
    return client.get_partitions_with_specs(request);
  }

  @Deprecated
  @Override
  public OptionalCompactionInfoStruct findNextCompact(String workerId) throws TException {
    return client.find_next_compact(workerId);
  }

  @Override
  public OptionalCompactionInfoStruct findNextCompact(FindNextCompactRequest rqst) throws TException {
    return client.find_next_compact2(rqst);
  }

  @Override
  public void updateCompactorState(CompactionInfoStruct cr, long txnId) throws TException {
    client.update_compactor_state(cr, txnId);
  }

  @Override
  public List<String> findColumnsWithStats(CompactionInfoStruct cr) throws TException {
    return client.find_columns_with_stats(cr);

  }

  @Override
  public void markCleaned(CompactionInfoStruct cr) throws MetaException, TException {
    client.mark_cleaned(cr);
  }

  @Override
  public void markCompacted(CompactionInfoStruct cr) throws MetaException, TException {
    client.mark_compacted(cr);
  }

  @Override
  public void markFailed(CompactionInfoStruct cr) throws MetaException, TException {
    client.mark_failed(cr);
  }

  @Override
  public void markRefused(CompactionInfoStruct cr) throws MetaException, TException {
    client.mark_refused(cr);
  }

  @Override
  public boolean updateCompactionMetricsData(CompactionMetricsDataStruct struct)
      throws MetaException, TException {
    return client.update_compaction_metrics_data(struct);
  }

  @Override
  public void removeCompactionMetricsData(CompactionMetricsDataRequest request) throws MetaException, TException {
    client.remove_compaction_metrics_data(request);
  }

  @Override
  public void setHadoopJobid(String jobId, long cqId) throws MetaException, TException {
    client.set_hadoop_jobid(jobId, cqId);
  }

  @Override
  public String getServerVersion() throws TException {
    return client.getVersion();
  }

  @Override
  public ScheduledQuery getScheduledQuery(ScheduledQueryKey scheduleKey) throws TException {
    return client.get_scheduled_query(scheduleKey);
  }

  @Override
  public void scheduledQueryProgress(ScheduledQueryProgressInfo info) throws TException {
    client.scheduled_query_progress(info);
  }

  @Override
  public ScheduledQueryPollResponse scheduledQueryPoll(ScheduledQueryPollRequest request)
      throws MetaException, TException {
    return client.scheduled_query_poll(request);
  }

  @Override
  public void scheduledQueryMaintenance(ScheduledQueryMaintenanceRequest request) throws MetaException, TException {
    client.scheduled_query_maintenance(request);
  }

  @Override
  public void addReplicationMetrics(ReplicationMetricList replicationMetricList) throws MetaException, TException {
    client.add_replication_metrics(replicationMetricList);
  }

  @Override
  public ReplicationMetricList getReplicationMetrics(GetReplicationMetricsRequest
                                                         replicationMetricsRequest) throws MetaException, TException {
    return client.get_replication_metrics(replicationMetricsRequest);
  }

  @Override
  public void createStoredProcedure(StoredProcedure proc) throws NoSuchObjectException, MetaException, TException {
    client.create_stored_procedure(proc);
  }

  @Override
  public StoredProcedure getStoredProcedure(StoredProcedureRequest request) throws MetaException, NoSuchObjectException, TException {
    return client.get_stored_procedure(request);
  }

  @Override
  public void dropStoredProcedure(StoredProcedureRequest request) throws MetaException, NoSuchObjectException, TException {
    client.drop_stored_procedure(request);
  }

  @Override
  public List<String> getAllStoredProcedures(ListStoredProcedureRequest request) throws MetaException, TException {
    return client.get_all_stored_procedures(request);
  }

  @Override
  public void addPackage(AddPackageRequest request) throws NoSuchObjectException, MetaException, TException {
    client.add_package(request);
  }

  @Override
  public Package findPackage(GetPackageRequest request) throws TException {
    return client.find_package(request);
  }

  @Override
  public List<String> listPackages(ListPackageRequest request) throws TException {
    return client.get_all_packages(request);
  }

  @Override
  public void dropPackage(DropPackageRequest request) throws TException {
    client.drop_package(request);
  }

  @Override
  public List<WriteEventInfo> getAllWriteEventInfo(GetAllWriteEventInfoRequest request)
      throws TException {
    return client.get_all_write_event_info(request);
  }

  @Override
  public AbortCompactResponse abortCompactions(AbortCompactionRequest request) throws TException{
    return client.abort_Compactions(request);
  }

  @Override
  public boolean setProperties(String nameSpace, Map<String, String> properties) throws TException {
    PropertySetRequest psr = new PropertySetRequest();
    psr.setNameSpace(nameSpace);
    psr.setPropertyMap(properties);
    return client.set_properties(psr);
  }

  @Override
  public Map<String, Map<String, String>> getProperties(String nameSpace, String mapPrefix, String mapPredicate, String... selection) throws TException {
    PropertyGetRequest request = new PropertyGetRequest();
    request.setNameSpace(nameSpace);
    request.setMapPrefix(mapPrefix);
    request.setMapPredicate(mapPredicate);
    if (selection != null && selection.length > 0) {
      request.setMapSelection(Arrays.asList(selection));
    }
    PropertyGetResponse response = client.get_properties(request);
    return response.getProperties();
  }
}
