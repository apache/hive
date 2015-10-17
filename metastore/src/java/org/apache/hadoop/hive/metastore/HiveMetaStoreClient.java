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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.isIndexTable;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Public;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsExpr;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeType;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Hive Metastore Client.
 * The public implementation of IMetaStoreClient. Methods not inherited from IMetaStoreClient
 * are not public and can change. Hence this is marked as unstable.
 * For users who require retry mechanism when the connection between metastore and client is
 * broken, RetryingMetaStoreClient class should be used.
 */
@Public
@Unstable
public class HiveMetaStoreClient implements IMetaStoreClient {
  ThriftHiveMetastore.Iface client = null;
  private TTransport transport = null;
  private boolean isConnected = false;
  private URI metastoreUris[];
  private final HiveMetaHookLoader hookLoader;
  protected final HiveConf conf;
  private String tokenStrForm;
  private final boolean localMetaStore;
  private final MetaStoreFilterHook filterHook;

  private Map<String, String> currentMetaVars;

  // for thrift connects
  private int retries = 5;
  private long retryDelaySeconds = 0;

  static final protected Log LOG = LogFactory.getLog("hive.metastore");

  public HiveMetaStoreClient(HiveConf conf)
    throws MetaException {
    this(conf, null);
  }

  public HiveMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader)
    throws MetaException {

    this.hookLoader = hookLoader;
    if (conf == null) {
      conf = new HiveConf(HiveMetaStoreClient.class);
    }
    this.conf = conf;
    filterHook = loadFilterHooks();

    String msUri = conf.getVar(HiveConf.ConfVars.METASTOREURIS);
    localMetaStore = HiveConfUtil.isEmbeddedMetaStore(msUri);
    if (localMetaStore) {
      // instantiate the metastore server handler directly instead of connecting
      // through the network
      client = HiveMetaStore.newRetryingHMSHandler("hive client", conf, true);
      isConnected = true;
      snapshotActiveConf();
      return;
    }

    // get the number retries
    retries = HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES);
    retryDelaySeconds = conf.getTimeVar(
        ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);

    // user wants file store based configuration
    if (conf.getVar(HiveConf.ConfVars.METASTOREURIS) != null) {
      String metastoreUrisString[] = conf.getVar(
          HiveConf.ConfVars.METASTOREURIS).split(",");
      metastoreUris = new URI[metastoreUrisString.length];
      try {
        int i = 0;
        for (String s : metastoreUrisString) {
          URI tmpUri = new URI(s);
          if (tmpUri.getScheme() == null) {
            throw new IllegalArgumentException("URI: " + s
                + " does not have a scheme");
          }
          metastoreUris[i++] = tmpUri;

        }
      } catch (IllegalArgumentException e) {
        throw (e);
      } catch (Exception e) {
        MetaStoreUtils.logAndThrowMetaException(e);
      }
    } else {
      LOG.error("NOT getting uris from conf");
      throw new MetaException("MetaStoreURIs not found in conf file");
    }
    // finally open the store
    open();
  }

  private MetaStoreFilterHook loadFilterHooks() throws IllegalStateException {
    Class<? extends MetaStoreFilterHook> authProviderClass = conf.
        getClass(HiveConf.ConfVars.METASTORE_FILTER_HOOK.varname,
            DefaultMetaStoreFilterHookImpl.class,
            MetaStoreFilterHook.class);
    String msg = "Unable to create instance of " + authProviderClass.getName() + ": ";
    try {
      Constructor<? extends MetaStoreFilterHook> constructor =
          authProviderClass.getConstructor(HiveConf.class);
      return constructor.newInstance(conf);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (SecurityException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (InstantiationException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
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

  @Override
  public boolean isCompatibleWith(HiveConf conf) {
    if (currentMetaVars == null) {
      return false; // recreate
    }
    boolean compatible = true;
    for (ConfVars oneVar : HiveConf.metaVars) {
      // Since metaVars are all of different types, use string for comparison
      String oldVar = currentMetaVars.get(oneVar.varname);
      String newVar = conf.get(oneVar.varname, "");
      if (oldVar == null ||
          (oneVar.isCaseSensitive() ? !oldVar.equals(newVar) : !oldVar.equalsIgnoreCase(newVar))) {
        LOG.info("Mestastore configuration " + oneVar.varname +
            " changed from " + oldVar + " to " + newVar);
        compatible = false;
      }
    }
    return compatible;
  }

  @Override
  public void setHiveAddedJars(String addedJars) {
    HiveConf.setVar(conf, ConfVars.HIVEADDEDJARS, addedJars);
  }

  @Override
  public void reconnect() throws MetaException {
    if (localMetaStore) {
      // For direct DB connections we don't yet support reestablishing connections.
      throw new MetaException("For direct MetaStore DB connections, we don't support retries" +
          " at the client level.");
    } else {
      close();
      // Swap the first element of the metastoreUris[] with a random element from the rest
      // of the array. Rationale being that this method will generally be called when the default
      // connection has died and the default connection is likely to be the first array element.
      promoteRandomMetaStoreURI();
      open();
    }
  }

  /**
   * @param dbname
   * @param tbl_name
   * @param new_tbl
   * @throws InvalidOperationException
   * @throws MetaException
   * @throws TException
   * @see
   *   org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#alter_table(
   *   java.lang.String, java.lang.String,
   *   org.apache.hadoop.hive.metastore.api.Table)
   */
  @Override
  public void alter_table(String dbname, String tbl_name, Table new_tbl)
      throws InvalidOperationException, MetaException, TException {
    alter_table(dbname, tbl_name, new_tbl, null);
  }

  @Override
  public void alter_table(String dbname, String tbl_name, Table new_tbl, boolean cascade)
      throws InvalidOperationException, MetaException, TException {
    client.alter_table_with_cascade(dbname, tbl_name, new_tbl, cascade);
  }

  public void alter_table(String dbname, String tbl_name, Table new_tbl,
      EnvironmentContext envContext) throws InvalidOperationException, MetaException, TException {
    client.alter_table_with_environment_context(dbname, tbl_name, new_tbl, envContext);
  }

  /**
   * @param dbname
   * @param name
   * @param part_vals
   * @param newPart
   * @throws InvalidOperationException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#rename_partition(
   *      java.lang.String, java.lang.String, java.util.List, org.apache.hadoop.hive.metastore.api.Partition)
   */
  @Override
  public void renamePartition(final String dbname, final String name, final List<String> part_vals, final Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    client.rename_partition(dbname, name, part_vals, newPart);
  }

  private void open() throws MetaException {
    isConnected = false;
    TTransportException tte = null;
    boolean useSasl = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL);
    boolean useFramedTransport = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);
    boolean useCompactProtocol = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL);
    int clientSocketTimeout = (int) conf.getTimeVar(
        ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

    for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
      for (URI store : metastoreUris) {
        LOG.info("Trying to connect to metastore with URI " + store);
        try {
          transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout);
          if (useSasl) {
            // Wrap thrift connection with SASL for secure connection.
            try {
              HadoopThriftAuthBridge.Client authBridge =
                ShimLoader.getHadoopThriftAuthBridge().createClient();

              // check if we should use delegation tokens to authenticate
              // the call below gets hold of the tokens if they are set up by hadoop
              // this should happen on the map/reduce tasks if the client added the
              // tokens into hadoop's credential store in the front end during job
              // submission.
              String tokenSig = conf.get("hive.metastore.token.signature");
              // tokenSig could be null
              tokenStrForm = Utils.getTokenStrForm(tokenSig);
              if(tokenStrForm != null) {
                // authenticate using delegation tokens via the "DIGEST" mechanism
                transport = authBridge.createClientTransport(null, store.getHost(),
                    "DIGEST", tokenStrForm, transport,
                        MetaStoreUtils.getMetaStoreSaslProperties(conf));
              } else {
                String principalConfig =
                    conf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL);
                transport = authBridge.createClientTransport(
                    principalConfig, store.getHost(), "KERBEROS", null,
                    transport, MetaStoreUtils.getMetaStoreSaslProperties(conf));
              }
            } catch (IOException ioe) {
              LOG.error("Couldn't create client transport", ioe);
              throw new MetaException(ioe.toString());
            }
          } else if (useFramedTransport) {
            transport = new TFramedTransport(transport);
          }
          final TProtocol protocol;
          if (useCompactProtocol) {
            protocol = new TCompactProtocol(transport);
          } else {
            protocol = new TBinaryProtocol(transport);
          }
          client = new ThriftHiveMetastore.Client(protocol);
          try {
            transport.open();
            isConnected = true;
          } catch (TTransportException e) {
            tte = e;
            if (LOG.isDebugEnabled()) {
              LOG.warn("Failed to connect to the MetaStore Server...", e);
            } else {
              // Don't print full exception trace if DEBUG is not on.
              LOG.warn("Failed to connect to the MetaStore Server...");
            }
          }

          if (isConnected && !useSasl && conf.getBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI)){
            // Call set_ugi, only in unsecure mode.
            try {
              UserGroupInformation ugi = Utils.getUGI();
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
          LOG.error("Unable to connect to metastore with URI " + store
                    + " in attempt " + attempt, e);
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
      throw new MetaException("Could not connect to meta store using any of the URIs provided." +
        " Most recent failure: " + StringUtils.stringifyException(tte));
    }

    snapshotActiveConf();

    LOG.info("Connected to metastore.");
  }

  private void snapshotActiveConf() {
    currentMetaVars = new HashMap<String, String>(HiveConf.metaVars.length);
    for (ConfVars oneVar : HiveConf.metaVars) {
      currentMetaVars.put(oneVar.varname, conf.get(oneVar.varname, ""));
    }
  }

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
      }
    } catch (TException e) {
      LOG.debug("Unable to shutdown metastore client. Will try closing transport directly.", e);
    }
    // Transport would have got closed via client.shutdown(), so we dont need this, but
    // just in case, we make this call.
    if ((transport != null) && transport.isOpen()) {
      transport.close();
    }
  }

  @Override
  public void setMetaConf(String key, String value) throws TException {
    client.setMetaConf(key, value);
  }

  @Override
  public String getMetaConf(String key) throws TException {
    return client.getMetaConf(key);
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
  public Partition add_partition(Partition new_part)
      throws InvalidObjectException, AlreadyExistsException, MetaException,
      TException {
    return add_partition(new_part, null);
  }

  public Partition add_partition(Partition new_part, EnvironmentContext envContext)
      throws InvalidObjectException, AlreadyExistsException, MetaException,
      TException {
    return deepCopy(client.add_partition_with_environment_context(new_part, envContext));
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
  public int add_partitions(List<Partition> new_parts)
      throws InvalidObjectException, AlreadyExistsException, MetaException,
      TException {
    return client.add_partitions(new_parts);
  }

  @Override
  public List<Partition> add_partitions(
      List<Partition> parts, boolean ifNotExists, boolean needResults)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if (parts.isEmpty()) {
      return needResults ? new ArrayList<Partition>() : null;
    }
    Partition part = parts.get(0);
    AddPartitionsRequest req = new AddPartitionsRequest(
        part.getDbName(), part.getTableName(), parts, ifNotExists);
    req.setNeedResult(needResults);
    AddPartitionsResult result = client.add_partitions_req(req);
    return needResults ? filterHook.filterPartitions(result.getPartitions()) : null;
  }

  @Override
  public int add_partitions_pspec(PartitionSpecProxy partitionSpec) throws TException {
    return client.add_partitions_pspec(partitionSpec.toPartitionSpec());
  }

  /**
   * @param table_name
   * @param db_name
   * @param part_vals
   * @return the appended partition
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#append_partition(java.lang.String,
   *      java.lang.String, java.util.List)
   */
  @Override
  public Partition appendPartition(String db_name, String table_name,
      List<String> part_vals) throws InvalidObjectException,
      AlreadyExistsException, MetaException, TException {
    return appendPartition(db_name, table_name, part_vals, null);
  }

  public Partition appendPartition(String db_name, String table_name, List<String> part_vals,
      EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    return deepCopy(client.append_partition_with_environment_context(db_name, table_name,
        part_vals, envContext));
  }

  @Override
  public Partition appendPartition(String dbName, String tableName, String partName)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return appendPartition(dbName, tableName, partName, null);
  }

  public Partition appendPartition(String dbName, String tableName, String partName,
      EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    return deepCopy(client.append_partition_by_name_with_environment_context(dbName, tableName,
        partName, envContext));
  }

  /**
   * Exchange the partition between two tables
   * @param partitionSpecs partitions specs of the parent partition to be exchanged
   * @param destDb the db of the destination table
   * @param destinationTableName the destination table name
   @ @return new partition after exchanging
   */
  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destDb,
      String destinationTableName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, TException {
    return client.exchange_partition(partitionSpecs, sourceDb, sourceTable,
        destDb, destinationTableName);
  }

  @Override
  public void validatePartitionNameCharacters(List<String> partVals)
      throws TException, MetaException {
    client.partition_name_has_valid_characters(partVals, true);
  }

  /**
   * Create a new Database
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
    client.create_database(db);
  }

  /**
   * @param tbl
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_table(org.apache.hadoop.hive.metastore.api.Table)
   */
  @Override
  public void createTable(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException {
    createTable(tbl, null);
  }

  public void createTable(Table tbl, EnvironmentContext envContext) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException {
    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preCreateTable(tbl);
    }
    boolean success = false;
    try {
      // Subclasses can override this step (for example, for temporary tables)
      create_table_with_environment_context(tbl, envContext);
      if (hook != null) {
        hook.commitCreateTable(tbl);
      }
      success = true;
    } finally {
      if (!success && (hook != null)) {
        hook.rollbackCreateTable(tbl);
      }
    }
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
    dropDatabase(name, true, false, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropDatabase(name, deleteData, ignoreUnknownDb, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try {
      getDatabase(name);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownDb) {
        throw e;
      }
      return;
    }

    if (cascade) {
       List<String> tableList = getAllTables(name);
       for (String table : tableList) {
         try {
           // Subclasses can override this step (for example, for temporary tables)
           dropTable(name, table, deleteData, true);
         } catch (UnsupportedOperationException e) {
           // Ignore Index tables, those will be dropped with parent tables
         }
        }
    }
    client.drop_database(name, deleteData, cascade);
  }

  /**
   * @param tbl_name
   * @param db_name
   * @param part_vals
   * @return true or false
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String,
   *      java.lang.String, java.util.List, boolean)
   */
  public boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals) throws NoSuchObjectException, MetaException,
      TException {
    return dropPartition(db_name, tbl_name, part_vals, true, null);
  }

  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
      EnvironmentContext env_context) throws NoSuchObjectException, MetaException, TException {
    return dropPartition(db_name, tbl_name, part_vals, true, env_context);
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, String partName, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return dropPartition(dbName, tableName, partName, deleteData, null);
  }

  private static EnvironmentContext getEnvironmentContextWithIfPurgeSet() {
    Map<String, String> warehouseOptions = new HashMap<String, String>();
    warehouseOptions.put("ifPurge", "TRUE");
    return new EnvironmentContext(warehouseOptions);
  }

  /*
  public boolean dropPartition(String dbName, String tableName, String partName, boolean deleteData, boolean ifPurge)
      throws NoSuchObjectException, MetaException, TException {

    return dropPartition(dbName, tableName, partName, deleteData,
                         ifPurge? getEnvironmentContextWithIfPurgeSet() : null);
  }
  */

  public boolean dropPartition(String dbName, String tableName, String partName, boolean deleteData,
      EnvironmentContext envContext) throws NoSuchObjectException, MetaException, TException {
    return client.drop_partition_by_name_with_environment_context(dbName, tableName, partName,
        deleteData, envContext);
  }

  /**
   * @param db_name
   * @param tbl_name
   * @param part_vals
   * @param deleteData
   *          delete the underlying data or just delete the table in metadata
   * @return true or false
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String,
   *      java.lang.String, java.util.List, boolean)
   */
  @Override
  public boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException {
    return dropPartition(db_name, tbl_name, part_vals, deleteData, null);
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, PartitionDropOptions options) throws TException {
    return dropPartition(db_name, tbl_name, part_vals, options.deleteData,
                         options.purgeData? getEnvironmentContextWithIfPurgeSet() : null);
  }

  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
      boolean deleteData, EnvironmentContext envContext) throws NoSuchObjectException,
      MetaException, TException {
    return client.drop_partition_with_environment_context(db_name, tbl_name, part_vals, deleteData,
        envContext);
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName,
                                        List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions options)
      throws TException {
    RequestPartsSpec rps = new RequestPartsSpec();
    List<DropPartitionsExpr> exprs = new ArrayList<DropPartitionsExpr>(partExprs.size());
    for (ObjectPair<Integer, byte[]> partExpr : partExprs) {
      DropPartitionsExpr dpe = new DropPartitionsExpr();
      dpe.setExpr(partExpr.getSecond());
      dpe.setPartArchiveLevel(partExpr.getFirst());
      exprs.add(dpe);
    }
    rps.setExprs(exprs);
    DropPartitionsRequest req = new DropPartitionsRequest(dbName, tblName, rps);
    req.setDeleteData(options.deleteData);
    req.setIgnoreProtection(options.ignoreProtection);
    req.setNeedResult(options.returnResults);
    req.setIfExists(options.ifExists);
    if (options.purgeData) {
      LOG.info("Dropped partitions will be purged!");
      req.setEnvironmentContext(getEnvironmentContextWithIfPurgeSet());
    }
    return client.drop_partitions_req(req).getPartitions();
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName,
      List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ignoreProtection,
      boolean ifExists, boolean needResult) throws NoSuchObjectException, MetaException, TException {

    return dropPartitions(dbName, tblName, partExprs,
                          PartitionDropOptions.instance()
                                              .deleteData(deleteData)
                                              .ignoreProtection(ignoreProtection)
                                              .ifExists(ifExists)
                                              .returnResults(needResult));

  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName,
      List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ignoreProtection,
      boolean ifExists) throws NoSuchObjectException, MetaException, TException {
    // By default, we need the results from dropPartitions();
    return dropPartitions(dbName, tblName, partExprs,
                          PartitionDropOptions.instance()
                                              .deleteData(deleteData)
                                              .ignoreProtection(ignoreProtection)
                                              .ifExists(ifExists));
  }

  /**
   * {@inheritDoc}
   * @see #dropTable(String, String, boolean, boolean, EnvironmentContext)
   */
  @Override
  public void dropTable(String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab) throws MetaException, TException,
      NoSuchObjectException, UnsupportedOperationException {
    dropTable(dbname, name, deleteData, ignoreUnknownTab, null);
  }

  /**
   * Drop the table and choose whether to save the data in the trash.
   * @param ifPurge completely purge the table (skipping trash) while removing
   *                data from warehouse
   * @see #dropTable(String, String, boolean, boolean, EnvironmentContext)
   */
  @Override
  public void dropTable(String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab, boolean ifPurge)
      throws MetaException, TException, NoSuchObjectException, UnsupportedOperationException {
    //build new environmentContext with ifPurge;
    EnvironmentContext envContext = null;
    if(ifPurge){
      Map<String, String> warehouseOptions = null;
      warehouseOptions = new HashMap<String, String>();
      warehouseOptions.put("ifPurge", "TRUE");
      envContext = new EnvironmentContext(warehouseOptions);
    }
    dropTable(dbname, name, deleteData, ignoreUnknownTab, envContext);
  }

  /** {@inheritDoc} */
  @Override
  @Deprecated
  public void dropTable(String tableName, boolean deleteData)
      throws MetaException, UnknownTableException, TException, NoSuchObjectException {
    dropTable(DEFAULT_DATABASE_NAME, tableName, deleteData, false, null);
  }

  /**
   * @see #dropTable(String, String, boolean, boolean, EnvironmentContext)
   */
  @Override
  public void dropTable(String dbname, String name)
      throws NoSuchObjectException, MetaException, TException {
    dropTable(dbname, name, true, true, null);
  }

  /**
   * Drop the table and choose whether to: delete the underlying table data;
   * throw if the table doesn't exist; save the data in the trash.
   *
   * @param dbname
   * @param name
   * @param deleteData
   *          delete the underlying data or just delete the table in metadata
   * @param ignoreUnknownTab
   *          don't throw if the requested table doesn't exist
   * @param envContext
   *          for communicating with thrift
   * @throws MetaException
   *           could not drop table properly
   * @throws NoSuchObjectException
   *           the table wasn't found
   * @throws TException
   *           a thrift communication error occurred
   * @throws UnsupportedOperationException
   *           dropping an index table is not allowed
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_table(java.lang.String,
   *      java.lang.String, boolean)
   */
  public void dropTable(String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab, EnvironmentContext envContext) throws MetaException, TException,
      NoSuchObjectException, UnsupportedOperationException {
    Table tbl;
    try {
      tbl = getTable(dbname, name);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw e;
      }
      return;
    }
    if (isIndexTable(tbl)) {
      throw new UnsupportedOperationException("Cannot drop index tables");
    }
    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preDropTable(tbl);
    }
    boolean success = false;
    try {
      drop_table_with_environment_context(dbname, name, deleteData, envContext);
      if (hook != null) {
        hook.commitDropTable(tbl, deleteData);
      }
      success=true;
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
      result = new LinkedHashMap<String, Type>();
      for (String key : fromClient.keySet()) {
        result.put(key, deepCopy(fromClient.get(key)));
      }
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getDatabases(String databasePattern)
    throws MetaException {
    try {
      return filterHook.filterDatabases(client.get_databases(databasePattern));
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getAllDatabases() throws MetaException {
    try {
      return filterHook.filterDatabases(client.get_all_databases());
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  /**
   * @param tbl_name
   * @param db_name
   * @param max_parts
   * @return list of partitions
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   */
  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name,
      short max_parts) throws NoSuchObjectException, MetaException, TException {
    return deepCopyPartitions(filterHook.filterPartitions(
        client.get_partitions(db_name, tbl_name, max_parts)));
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
    return PartitionSpecProxy.Factory.get(filterHook.filterPartitionSpecs(
        client.get_partitions_pspec(dbName, tableName, maxParts)));
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name,
      List<String> part_vals, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return deepCopyPartitions(filterHook.filterPartitions(
        client.get_partitions_ps(db_name, tbl_name, part_vals, max_parts)));
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String db_name,
      String tbl_name, short max_parts, String user_name, List<String> group_names)
       throws NoSuchObjectException, MetaException, TException {
    return deepCopyPartitions(filterHook.filterPartitions(
        client.get_partitions_with_auth(db_name, tbl_name, max_parts, user_name, group_names)));
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String db_name,
      String tbl_name, List<String> part_vals, short max_parts,
      String user_name, List<String> group_names) throws NoSuchObjectException,
      MetaException, TException {
    return deepCopyPartitions(filterHook.filterPartitions(client.get_partitions_ps_with_auth(db_name,
        tbl_name, part_vals, max_parts, user_name, group_names)));
  }

  /**
   * Get list of partitions matching specified filter
   * @param db_name the database name
   * @param tbl_name the table name
   * @param filter the filter string,
   *    for example "part1 = \"p1_abc\" and part2 <= "\p2_test\"". Filtering can
   *    be done only on string partition keys.
   * @param max_parts the maximum number of partitions to return,
   *    all partitions are returned if -1 is passed
   * @return list of partitions
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  @Override
  public List<Partition> listPartitionsByFilter(String db_name, String tbl_name,
      String filter, short max_parts) throws MetaException,
         NoSuchObjectException, TException {
    return deepCopyPartitions(filterHook.filterPartitions(
        client.get_partitions_by_filter(db_name, tbl_name, filter, max_parts)));
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name,
                                                       String filter, int max_parts) throws MetaException,
         NoSuchObjectException, TException {
    return PartitionSpecProxy.Factory.get(filterHook.filterPartitionSpecs(
        client.get_part_specs_by_filter(db_name, tbl_name, filter, max_parts)));
  }

  @Override
  public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr,
      String default_partition_name, short max_parts, List<Partition> result)
          throws TException {
    assert result != null;
    PartitionsByExprRequest req = new PartitionsByExprRequest(
        db_name, tbl_name, ByteBuffer.wrap(expr));
    if (default_partition_name != null) {
      req.setDefaultPartitionName(default_partition_name);
    }
    if (max_parts >= 0) {
      req.setMaxParts(max_parts);
    }
    PartitionsByExprResult r = null;
    try {
      r = client.get_partitions_by_expr(req);
    } catch (TApplicationException te) {
      // TODO: backward compat for Hive <= 0.12. Can be removed later.
      if (te.getType() != TApplicationException.UNKNOWN_METHOD
          && te.getType() != TApplicationException.WRONG_METHOD_NAME) {
        throw te;
      }
      throw new IncompatibleMetastoreException(
          "Metastore doesn't support listPartitionsByExpr: " + te.getMessage());
    }
    r.setPartitions(filterHook.filterPartitions(r.getPartitions()));
    // TODO: in these methods, do we really need to deepcopy?
    deepCopyPartitions(r.getPartitions(), result);
    return !r.isSetHasUnknownPartitions() || r.isHasUnknownPartitions(); // Assume the worst.
  }


  /**
   * @param name
   * @return the database
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_database(java.lang.String)
   */
  @Override
  public Database getDatabase(String name) throws NoSuchObjectException,
      MetaException, TException {
    return deepCopy(filterHook.filterDatabase(client.get_database(name)));
  }

  /**
   * @param tbl_name
   * @param db_name
   * @param part_vals
   * @return the partition
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_partition(java.lang.String,
   *      java.lang.String, java.util.List)
   */
  @Override
  public Partition getPartition(String db_name, String tbl_name,
      List<String> part_vals) throws NoSuchObjectException, MetaException, TException {
    return deepCopy(filterHook.filterPartition(
        client.get_partition(db_name, tbl_name, part_vals)));
  }

  @Override
  public List<Partition> getPartitionsByNames(String db_name, String tbl_name,
      List<String> part_names) throws NoSuchObjectException, MetaException, TException {
    return deepCopyPartitions(filterHook.filterPartitions(
        client.get_partitions_by_names(db_name, tbl_name, part_names)));
  }

  @Override
  public Partition getPartitionWithAuthInfo(String db_name, String tbl_name,
      List<String> part_vals, String user_name, List<String> group_names)
      throws MetaException, UnknownTableException, NoSuchObjectException,
      TException {
    return deepCopy(filterHook.filterPartition(client.get_partition_with_auth(db_name,
        tbl_name, part_vals, user_name, group_names)));
  }

  /**
   * @param name
   * @param dbname
   * @return the table
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @throws NoSuchObjectException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_table(java.lang.String,
   *      java.lang.String)
   */
  @Override
  public Table getTable(String dbname, String name) throws MetaException,
      TException, NoSuchObjectException {
    return deepCopy(filterHook.filterTable(client.get_table(dbname, name)));
  }

  /** {@inheritDoc} */
  @Override
  @Deprecated
  public Table getTable(String tableName) throws MetaException, TException,
      NoSuchObjectException {
    return filterHook.filterTable(getTable(DEFAULT_DATABASE_NAME, tableName));
  }

  /** {@inheritDoc} */
  @Override
  public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return deepCopyTables(filterHook.filterTables(
        client.get_table_objects_by_name(dbName, tableNames)));
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws MetaException, TException, InvalidOperationException, UnknownDBException {
    return filterHook.filterTableNames(dbName,
        client.get_table_names_by_filter(dbName, filter, maxTables));
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

  /** {@inheritDoc} */
  @Override
  public List<String> getTables(String dbname, String tablePattern) throws MetaException {
    try {
      return filterHook.filterTableNames(dbname, client.get_tables(dbname, tablePattern));
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getAllTables(String dbname) throws MetaException {
    try {
      return filterHook.filterTableNames(dbname, client.get_all_tables(dbname));
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) throws MetaException,
      TException, UnknownDBException {
    try {
      return filterHook.filterTable(client.get_table(databaseName, tableName)) != null;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  /** {@inheritDoc} */
  @Override
  @Deprecated
  public boolean tableExists(String tableName) throws MetaException,
      TException, UnknownDBException {
    return tableExists(DEFAULT_DATABASE_NAME, tableName);
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName,
      short max) throws MetaException, TException {
    return filterHook.filterPartitionNames(dbName, tblName,
        client.get_partition_names(dbName, tblName, max));
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name,
      List<String> part_vals, short max_parts)
      throws MetaException, TException, NoSuchObjectException {
    return filterHook.filterPartitionNames(db_name, tbl_name,
        client.get_partition_names_ps(db_name, tbl_name, part_vals, max_parts));
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partition(dbName, tblName, newPart);
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts)
  throws InvalidOperationException, MetaException, TException {
    client.alter_partitions(dbName, tblName, newParts);
}

  @Override
  public void alterDatabase(String dbName, Database db)
      throws MetaException, NoSuchObjectException, TException {
    client.alter_database(dbName, db);
  }
  /**
   * @param db
   * @param tableName
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_fields(java.lang.String,
   *      java.lang.String)
   */
  @Override
  public List<FieldSchema> getFields(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException {
    return deepCopyFieldSchemas(client.get_fields(db, tableName));
  }

  /**
   * create an index
   * @param index the index object
   * @param indexTable which stores the index data
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @throws AlreadyExistsException
   */
  @Override
  public void createIndex(Index index, Table indexTable) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    client.add_index(index, indexTable);
  }

  /**
   * @param dbname
   * @param base_tbl_name
   * @param idx_name
   * @param new_idx
   * @throws InvalidOperationException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#alter_index(java.lang.String,
   *      java.lang.String, java.lang.String, org.apache.hadoop.hive.metastore.api.Index)
   */
  @Override
  public void alter_index(String dbname, String base_tbl_name, String idx_name, Index new_idx)
      throws InvalidOperationException, MetaException, TException {
    client.alter_index(dbname, base_tbl_name, idx_name, new_idx);
  }

  /**
   * @param dbName
   * @param tblName
   * @param indexName
   * @return the index
   * @throws MetaException
   * @throws UnknownTableException
   * @throws NoSuchObjectException
   * @throws TException
   */
  @Override
  public Index getIndex(String dbName, String tblName, String indexName)
      throws MetaException, UnknownTableException, NoSuchObjectException,
      TException {
    return deepCopy(filterHook.filterIndex(client.get_index_by_name(dbName, tblName, indexName)));
  }

  /**
   * list indexes of the give base table
   * @param dbName
   * @param tblName
   * @param max
   * @return the list of indexes
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   */
  @Override
  public List<String> listIndexNames(String dbName, String tblName, short max)
      throws MetaException, TException {
    return filterHook.filterIndexNames(dbName, tblName, client.get_index_names(dbName, tblName, max));
  }

  /**
   * list all the index names of the give base table.
   *
   * @param dbName
   * @param tblName
   * @param max
   * @return list of indexes
   * @throws MetaException
   * @throws TException
   */
  @Override
  public List<Index> listIndexes(String dbName, String tblName, short max)
      throws NoSuchObjectException, MetaException, TException {
    return filterHook.filterIndexes(client.get_indexes(dbName, tblName, max));
  }

  /** {@inheritDoc} */
  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
    InvalidInputException{
    return client.update_table_column_statistics(statsObj);
  }

  /** {@inheritDoc} */
  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
    InvalidInputException{
    return client.update_partition_column_statistics(statsObj);
  }

  /** {@inheritDoc} */
  @Override
  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
    InvalidInputException{
    return client.set_aggr_stats_for(request);
  }

  /** {@inheritDoc} */
  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws NoSuchObjectException, MetaException, TException,
      InvalidInputException, InvalidObjectException {
    return client.get_table_statistics_req(
        new TableStatsRequest(dbName, tableName, colNames)).getTableStats();
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName, String tableName, List<String> partNames, List<String> colNames)
          throws NoSuchObjectException, MetaException, TException {
    return client.get_partitions_statistics_req(
        new PartitionsStatsRequest(dbName, tableName, colNames, partNames)).getPartStats();
  }

  /** {@inheritDoc} */
  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
    String colName) throws NoSuchObjectException, InvalidObjectException, MetaException,
    TException, InvalidInputException
  {
    return client.delete_partition_column_statistics(dbName, tableName, partName, colName);
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
    InvalidInputException
  {
    return client.delete_table_column_statistics(dbName, tableName, colName);
  }

  /**
   * @param db
   * @param tableName
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_schema(java.lang.String,
   *      java.lang.String)
   */
  @Override
  public List<FieldSchema> getSchema(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException {
      EnvironmentContext envCxt = null;
      String addedJars = conf.getVar(ConfVars.HIVEADDEDJARS);
      if(org.apache.commons.lang.StringUtils.isNotBlank(addedJars)) {
         Map<String, String> props = new HashMap<String, String>();
         props.put("hive.added.jars.path", addedJars);
         envCxt = new EnvironmentContext(props);
       }

    return deepCopyFieldSchemas(client.get_schema_with_environment_context(db, tableName, envCxt));
  }

  @Override
  public String getConfigValue(String name, String defaultValue)
      throws TException, ConfigValSecurityException {
    return client.get_config_value(name, defaultValue);
  }

  @Override
  public Partition getPartition(String db, String tableName, String partName)
      throws MetaException, TException, UnknownTableException, NoSuchObjectException {
    return deepCopy(
        filterHook.filterPartition(client.get_partition_by_name(db, tableName, partName)));
  }

  public Partition appendPartitionByName(String dbName, String tableName, String partName)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return appendPartitionByName(dbName, tableName, partName, null);
  }

  public Partition appendPartitionByName(String dbName, String tableName, String partName,
      EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    return deepCopy(client.append_partition_by_name_with_environment_context(dbName, tableName,
        partName, envContext));
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

  @Override
  public List<String> partitionNameToVals(String name) throws MetaException, TException {
    return client.partition_name_to_vals(name);
  }

  @Override
  public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException{
    return client.partition_name_to_spec(name);
  }

  /**
   * @param partition
   * @return
   */
  private Partition deepCopy(Partition partition) {
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

  private Index deepCopy(Index index) {
    Index copy = null;
    if (index != null) {
      copy = new Index(index);
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

  private List<Partition> deepCopyPartitions(List<Partition> partitions) {
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
  public boolean dropIndex(String dbName, String tblName, String name,
      boolean deleteData) throws NoSuchObjectException, MetaException,
      TException {
    return client.drop_index_by_name(dbName, tblName, name, deleteData);
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
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
      String userName, List<String> groupNames) throws MetaException,
      TException {
    return client.get_privilege_set(hiveObject, userName, groupNames);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String principalName,
      PrincipalType principalType, HiveObjectRef hiveObject)
      throws MetaException, TException {
    return client.list_privileges(principalName, principalType, hiveObject);
  }

  public String getDelegationToken(String renewerKerberosPrincipalName) throws
  MetaException, TException, IOException {
    //a convenience method that makes the intended owner for the delegation
    //token request the current user
    String owner = conf.getUser();
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
  public ValidTxnList getValidTxns() throws TException {
    return TxnHandler.createValidReadTxnList(client.get_open_txns(), 0);
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn) throws TException {
    return TxnHandler.createValidReadTxnList(client.get_open_txns(), currentTxn);
  }

  @Override
  public long openTxn(String user) throws TException {
    OpenTxnsResponse txns = openTxns(user, 1);
    return txns.getTxn_ids().get(0);
  }

  @Override
  public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    String hostname = null;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to resolve my host name " + e.getMessage());
      throw new RuntimeException(e);
    }
    return client.open_txns(new OpenTxnRequest(numTxns, user, hostname));
  }

  @Override
  public void rollbackTxn(long txnid) throws NoSuchTxnException, TException {
    client.abort_txn(new AbortTxnRequest(txnid));
  }

  @Override
  public void commitTxn(long txnid)
      throws NoSuchTxnException, TxnAbortedException, TException {
    client.commit_txn(new CommitTxnRequest(txnid));
  }

  @Override
  public GetOpenTxnsInfoResponse showTxns() throws TException {
    return client.get_open_txns_info();
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
  public ShowLocksResponse showLocks() throws TException {
    return client.show_locks(new ShowLocksRequest());
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
  public void compact(String dbname, String tableName, String partitionName,  CompactionType type)
      throws TException {
    CompactionRequest cr = new CompactionRequest();
    if (dbname == null) cr.setDbname(DEFAULT_DATABASE_NAME);
    else cr.setDbname(dbname);
    cr.setTablename(tableName);
    if (partitionName != null) cr.setPartitionname(partitionName);
    cr.setType(type);
    client.compact(cr);
  }

  @Override
  public ShowCompactResponse showCompactions() throws TException {
    return client.show_compact(new ShowCompactRequest());
  }

  @Override
  public void addDynamicPartitions(long txnId, String dbName, String tableName,
                                   List<String> partNames) throws TException {
    client.add_dynamic_partitions(new AddDynamicPartitions(txnId, dbName, tableName, partNames));
  }

  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  @Override
  public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents,
                                                       NotificationFilter filter) throws TException {
    NotificationEventRequest rqst = new NotificationEventRequest(lastEventId);
    rqst.setMaxEvents(maxEvents);
    NotificationEventResponse rsp = client.get_next_notification(rqst);
    LOG.debug("Got back " + rsp.getEventsSize() + " events");
    if (filter == null) {
      return rsp;
    } else {
      NotificationEventResponse filtered = new NotificationEventResponse();
      if (rsp != null && rsp.getEvents() != null) {
        for (NotificationEvent e : rsp.getEvents()) {
          if (filter.accept(e)) filtered.addToEvents(e);
        }
      }
      return filtered;
    }
  }

  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    return client.get_current_notificationEventId();
  }

  @InterfaceAudience.LimitedPrivate({"Apache Hive, HCatalog"})
  @Override
  public FireEventResponse fireListenerEvent(FireEventRequest rqst) throws TException {
    return client.fire_listener_event(rqst);
  }

  /**
   * Creates a synchronized wrapper for any {@link IMetaStoreClient}.
   * This may be used by multi-threaded applications until we have
   * fixed all reentrancy bugs.
   *
   * @param client unsynchronized client
   *
   * @return synchronized client
   */
  public static IMetaStoreClient newSynchronizedClient(
      IMetaStoreClient client) {
    return (IMetaStoreClient) Proxy.newProxyInstance(
      HiveMetaStoreClient.class.getClassLoader(),
      new Class [] { IMetaStoreClient.class },
      new SynchronizedHandler(client));
  }

  private static class SynchronizedHandler implements InvocationHandler {
    private final IMetaStoreClient client;
    private static final Object lock = SynchronizedHandler.class;

    SynchronizedHandler(IMetaStoreClient client) {
      this.client = client;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object [] args)
        throws Throwable {
      try {
        synchronized (lock) {
          return method.invoke(client, args);
        }
      } catch (InvocationTargetException e) {
        throw e.getTargetException();
      }
    }
  }

  @Override
  public void markPartitionForEvent(String db_name, String tbl_name, Map<String,String> partKVs, PartitionEventType eventType)
      throws MetaException, TException, NoSuchObjectException, UnknownDBException,
      UnknownTableException,
      InvalidPartitionException, UnknownPartitionException {
    assert db_name != null;
    assert tbl_name != null;
    assert partKVs != null;
    client.markPartitionForEvent(db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String,String> partKVs, PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, UnknownTableException, UnknownDBException, TException,
      InvalidPartitionException, UnknownPartitionException {
    assert db_name != null;
    assert tbl_name != null;
    assert partKVs != null;
    return client.isPartitionMarkedForEvent(db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException,
      MetaException, TException {
    client.create_function(func);
  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException, TException {
    client.alter_function(dbName, funcName, newFunction);
  }

  @Override
  public void dropFunction(String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException, TException {
    client.drop_function(dbName, funcName);
  }

  @Override
  public Function getFunction(String dbName, String funcName)
      throws MetaException, TException {
    return deepCopy(client.get_function(dbName, funcName));
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern)
      throws MetaException, TException {
    return client.get_functions(dbName, pattern);
  }

  protected void create_table_with_environment_context(Table tbl, EnvironmentContext envContext)
      throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {
    client.create_table_with_environment_context(tbl, envContext);
  }

  protected void drop_table_with_environment_context(String dbname, String name,
      boolean deleteData, EnvironmentContext envContext) throws MetaException, TException,
      NoSuchObjectException, UnsupportedOperationException {
    client.drop_table_with_environment_context(dbname, name, deleteData, envContext);
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName,
    List<String> colNames, List<String> partNames) throws NoSuchObjectException, MetaException, TException {
    if (colNames.isEmpty() || partNames.isEmpty()) {
      LOG.debug("Columns is empty or partNames is empty : Short-circuiting stats eval on client side.");
      return new AggrStats(new ArrayList<ColumnStatisticsObj>(),0); // Nothing to aggregate
    }
    PartitionsStatsRequest req = new PartitionsStatsRequest(dbName, tblName, colNames, partNames);
    return client.get_aggr_stats_for(req);
  }
}
