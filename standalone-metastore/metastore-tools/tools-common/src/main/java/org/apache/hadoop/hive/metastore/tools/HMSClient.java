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

package org.apache.hadoop.hive.metastore.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsRequest;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *  Wrapper for Thrift HMS interface.
 */
final class HMSClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(HMSClient.class);
  private static final String METASTORE_URI = "hive.metastore.uris";
  private static final String CONFIG_DIR = "/etc/hive/conf";
  private static final String HIVE_SITE = "hive-site.xml";
  private static final String CORE_SITE = "core-site.xml";
  private static final String PRINCIPAL_KEY = "hive.metastore.kerberos.principal";

  private final String confDir;
  private ThriftHiveMetastore.Iface client;
  private TTransport transport;
  private URI serverURI;
  private Configuration hadoopConf;

  public URI getServerURI() {
    return serverURI;
  }

  @Override
  public String toString() {
    return serverURI.toString();
  }

  HMSClient(@Nullable URI uri)
      throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
    this(uri, CONFIG_DIR);
  }

  HMSClient(@Nullable URI uri, @Nullable String confDir)
      throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
    this.confDir = confDir == null ? CONFIG_DIR : confDir;
    getClient(uri);
  }

  private void addResource(Configuration conf, @NotNull String r) throws MalformedURLException {
    File f = new File(confDir + "/" + r);
    if (f.exists() && !f.isDirectory()) {
      LOG.debug("Adding configuration resource {}", r);
      conf.addResource(f.toURI().toURL());
    } else {
      LOG.debug("Configuration {} does not exist", r);
    }
  }

  /**
   * Create a client to Hive Metastore.
   * If principal is specified, create kerberised client.
   *
   * @param uri server uri
   * @throws MetaException        if fails to login using kerberos credentials
   * @throws IOException          if fails connecting to metastore
   * @throws InterruptedException if interrupted during kerberos setup
   */
  private void getClient(@Nullable URI uri)
      throws TException, IOException, InterruptedException, URISyntaxException, LoginException {
    Configuration conf = new HiveConf();
    addResource(conf, HIVE_SITE);
    if (uri != null) {
      conf.set(METASTORE_URI, uri.toString());
    }

    // Pick up the first URI from the list of available URIs
    serverURI = uri != null ?
        uri :
        new URI(conf.get(METASTORE_URI).split(",")[0]);

    String principal = conf.get(PRINCIPAL_KEY);

    if (principal == null) {
      open(conf, serverURI);
      return;
    }

    LOG.debug("Opening kerberos connection to HMS");
    addResource(conf, CORE_SITE);

    this.hadoopConf = new Configuration();
    addResource(hadoopConf, HIVE_SITE);
    addResource(hadoopConf, CORE_SITE);

    // Kerberos magic
    UserGroupInformation.setConfiguration(hadoopConf);
    UserGroupInformation.getLoginUser()
        .doAs((PrivilegedExceptionAction<TTransport>)
            () -> open(conf, serverURI));
  }

  boolean dbExists(@NotNull String dbName) throws TException {
    return getAllDatabases(dbName).contains(dbName);
  }

  boolean tableExists(@NotNull String dbName, @NotNull String tableName) throws TException {
    return getAllTables(dbName, tableName).contains(tableName);
  }

  Database getDatabase(@NotNull String dbName) throws TException {
    return client.get_database(dbName);
  }

  /**
   * Return all databases with name matching the filter.
   *
   * @param filter Regexp. Can be null or empty in which case everything matches
   * @return list of database names matching the filter
   * @throws MetaException
   */
  Set<String> getAllDatabases(@Nullable String filter) throws TException {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.get_all_databases());
    }
    return client.get_all_databases()
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  Set<String> getAllTables(@NotNull String dbName, @Nullable String filter) throws TException {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.get_all_tables(dbName));
    }
    return client.get_all_tables(dbName)
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  /**
   * Create database with the given name if it doesn't exist
   *
   * @param name database name
   */
  boolean createDatabase(@NotNull String name) throws TException {
    return createDatabase(name, null, null, null);
  }

  /**
   * Create database if it doesn't exist
   *
   * @param name        Database name
   * @param description Database description
   * @param location    Database location
   * @param params      Database params
   * @throws TException if database exists
   */
  boolean createDatabase(@NotNull String name,
                         @Nullable String description,
                         @Nullable String location,
                         @Nullable Map<String, String> params)
      throws TException {
    Database db = new Database(name, description, location, params);
    client.create_database(db);
    return true;
  }

  boolean createDatabase(Database db) throws TException {
    client.create_database(db);
    return true;
  }

  boolean dropDatabase(@NotNull String dbName) throws TException {
    client.drop_database(dbName, true, true);
    return true;
  }

  boolean createTable(Table table) throws TException {
    client.create_table(table);
    return true;
  }

  boolean dropTable(@NotNull String dbName, @NotNull String tableName) throws TException {
    return dropTable(dbName, tableName, true);
  }

  boolean dropTable(@NotNull String dbName, @NotNull String tableName, boolean deleteData)
    throws TException {
    client.drop_table(dbName, tableName, deleteData);
    return true;
  }

  Table getTable(@NotNull String dbName, @NotNull String tableName) throws TException {
    return client.get_table(dbName, tableName);
  }

  Partition createPartition(@NotNull Table table, @NotNull List<String> values) throws TException {
    return client.add_partition(new Util.PartitionBuilder(table).withValues(values).build());
  }

  Partition addPartition(@NotNull Partition partition) throws TException {
    return client.add_partition(partition);
  }

  void addPartitions(List<Partition> partitions) throws TException {
    client.add_partitions(partitions);
  }


  List<Partition> listPartitions(@NotNull String dbName,
                                 @NotNull String tableName) throws TException {
    return client.get_partitions(dbName, tableName, (short) -1);
  }

  Long getCurrentNotificationId() throws TException {
    return client.get_current_notificationEventId().getEventId();
  }

  List<String> getPartitionNames(@NotNull String dbName,
                                 @NotNull String tableName) throws TException {
    return client.get_partition_names(dbName, tableName, (short) -1);
  }

  public boolean dropPartition(@NotNull String dbName, @NotNull String tableName,
                               @NotNull List<String> arguments)
      throws TException {
    return client.drop_partition(dbName, tableName, arguments, true);
  }

  public boolean dropPartition(@NotNull String dbName, @NotNull String tableName,
                               @NotNull String arguments)
          throws TException {
    List<String> partVals = Warehouse.getPartValuesFromPartName(arguments);
    return dropPartition(dbName, tableName, partVals);
  }

  List<Partition> getPartitions(@NotNull String dbName, @NotNull String tableName) throws TException {
    return client.get_partitions(dbName, tableName, (short) -1);
  }

  DropPartitionsResult dropPartitions(@NotNull String dbName, @NotNull String tableName,
                                      @Nullable List<String> partNames) throws TException {
    if (partNames == null) {
      return dropPartitions(dbName, tableName, getPartitionNames(dbName, tableName));
    }
    if (partNames.isEmpty()) {
      return null;
    }
    return client.drop_partitions_req(new DropPartitionsRequest(dbName,
        tableName, RequestPartsSpec.names(partNames)));
  }

  List<Partition> getPartitionsByNames(@NotNull String dbName, @NotNull String tableName,
                                       @Nullable List<String> names) throws TException {
    if (names == null) {
      return client.get_partitions_by_names(dbName, tableName,
          getPartitionNames(dbName, tableName));
    }
    return client.get_partitions_by_names(dbName, tableName, names);
  }

  List<Partition> getPartitionsByFilter(@NotNull String dbName, @NotNull String tableName,
                                        @NotNull String filter) throws TException {
    return client.get_partitions_by_filter(dbName, tableName, filter, (short) -1);
  }

  boolean alterTable(@NotNull String dbName, @NotNull String tableName, @NotNull Table newTable)
      throws TException {
    client.alter_table(dbName, tableName, newTable);
    return true;
  }

  void alterPartition(@NotNull String dbName, @NotNull String tableName,
                      @NotNull Partition partition) throws TException {
    client.alter_partition(dbName, tableName, partition);
  }

  void alterPartitions(@NotNull String dbName, @NotNull String tableName,
                       @NotNull List<Partition> partitions) throws TException {
    client.alter_partitions(dbName, tableName, partitions);
  }

  void appendPartition(@NotNull String dbName, @NotNull String tableName,
                       @NotNull List<String> partitionValues) throws TException {
    client.append_partition_with_environment_context(dbName, tableName, partitionValues, null);
  }

  List<Long> getOpenTxns() throws TException {
    GetOpenTxnsRequest getOpenTxnsRequest = new GetOpenTxnsRequest();
    getOpenTxnsRequest.setExcludeTxnTypes(Arrays.asList(TxnType.READ_ONLY));
    GetOpenTxnsResponse txns = client.get_open_txns_req(getOpenTxnsRequest);
    List<Long> openTxns = new ArrayList<>();
    BitSet abortedBits = BitSet.valueOf(txns.getAbortedBits());
    int i = 0;
    for(long txnId : txns.getOpen_txns()) {
      if(!abortedBits.get(i)) {
        openTxns.add(txnId);
      }
      ++i;
    }
    return openTxns;
  }

  List<TxnInfo> getOpenTxnsInfo() throws TException {
    return client.get_open_txns_info().getOpen_txns();
  }

  boolean commitTxn(long txnId) throws TException {
    client.commit_txn(new CommitTxnRequest(txnId));
    return true;
  }

  boolean abortTxns(List<Long> txnIds) throws TException {
    client.abort_txns(new AbortTxnsRequest(txnIds));
    return true;
  }

  boolean allocateTableWriteIds(String dbName, String tableName, List<Long> openTxns) throws TException {
    AllocateTableWriteIdsRequest awiRqst = new AllocateTableWriteIdsRequest(dbName, tableName);
    openTxns.forEach(t -> {
      awiRqst.addToTxnIds(t);
    });

    client.allocate_table_write_ids(awiRqst);
    return true;
  }

  boolean getValidWriteIds(List<String> fullTableNames) throws TException {
    client.get_valid_write_ids(new GetValidWriteIdsRequest(fullTableNames));
    return true;
  }

  LockResponse lock(@NotNull LockRequest rqst) throws TException {
    return client.lock(rqst);
  }

  List<Long> openTxn(int howMany) throws TException {
    OpenTxnsResponse txns = openTxnsIntr("", howMany, null);
    return txns.getTxn_ids();
  }

  private TTransport open(Configuration conf, @NotNull URI uri) throws
      TException, IOException, LoginException {
    boolean useSSL = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.USE_SSL);
    boolean useSasl = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.USE_THRIFT_SASL);
    boolean useFramedTransport = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.USE_THRIFT_FRAMED_TRANSPORT);
    boolean useCompactProtocol = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
    int clientSocketTimeout = (int) MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);
    int connectionTimeout = (int) MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.CLIENT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);

    LOG.debug("Connecting to {}, framedTransport = {}", uri, useFramedTransport);

    String host = uri.getHost();
    int port = uri.getPort();

    // Sasl/SSL code is copied from HiveMetastoreCLient
    if (!useSSL) {
      transport = new TSocket(new TConfiguration(),host, port, clientSocketTimeout, connectionTimeout);
    } else {
      String trustStorePath = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_TRUSTSTORE_PATH).trim();
      if (trustStorePath.isEmpty()) {
        throw new IllegalArgumentException(MetastoreConf.ConfVars.SSL_TRUSTSTORE_PATH.toString()
            + " Not configured for SSL connection");
      }
      String trustStorePassword =
          MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.SSL_TRUSTSTORE_PASSWORD);
      String trustStoreType =
              MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_TRUSTSTORE_TYPE).trim();
      String trustStoreAlgorithm =
              MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_TRUSTMANAGERFACTORY_ALGORITHM).trim();

      // Create an SSL socket and connect
      transport = SecurityUtils.getSSLSocket(host, port, clientSocketTimeout, connectionTimeout,
          trustStorePath, trustStorePassword, trustStoreType, trustStoreAlgorithm);
      LOG.info("Opened an SSL connection to metastore, current connections");
    }

    if (useSasl) {
      // Wrap thrift connection with SASL for secure connection.
      HadoopThriftAuthBridge.Client authBridge =
          HadoopThriftAuthBridge.getBridge().createClient();

      // check if we should use delegation tokens to authenticate
      // the call below gets hold of the tokens if they are set up by hadoop
      // this should happen on the map/reduce tasks if the client added the
      // tokens into hadoop's credential store in the front end during job
      // submission.
      String tokenSig = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.TOKEN_SIGNATURE);
      // tokenSig could be null
      String tokenStrForm = SecurityUtils.getTokenStrForm(tokenSig);

      if (tokenStrForm != null) {
        LOG.info("HMSC::open(): Found delegation token. Creating DIGEST-based thrift connection.");
        // authenticate using delegation tokens via the "DIGEST" mechanism
        transport = authBridge.createClientTransport(null, host,
            "DIGEST", tokenStrForm, transport,
            MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
      } else {
        LOG.info("HMSC::open(): Could not find delegation token. Creating KERBEROS-based thrift connection.");
        String principalConfig =
            MetastoreConf.getVar(conf, MetastoreConf.ConfVars.KERBEROS_PRINCIPAL);
        transport = authBridge.createClientTransport(
            principalConfig, host, "KERBEROS", null,
            transport, MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
      }
    } else {
      if (useFramedTransport) {
        transport = new TFramedTransport(transport);
      }
    }

    final TProtocol protocol;
    if (useCompactProtocol) {
      protocol = new TCompactProtocol(transport);
    } else {
      protocol = new TBinaryProtocol(transport);
    }
    client = new ThriftHiveMetastore.Client(protocol);
    if (!transport.isOpen()) {
      transport.open();
      LOG.info("Opened a connection to metastore, current connections");

      if (!useSasl && MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI)) {
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
    }

    LOG.debug("Connected to metastore, using compact protocol = {}", useCompactProtocol);
    return transport;
  }

  private OpenTxnsResponse openTxnsIntr(String user, int numTxns, TxnType txnType) throws TException {
    String hostname;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to resolve my host name " + e.getMessage());
      throw new RuntimeException(e);
    }
    OpenTxnRequest rqst = new OpenTxnRequest(numTxns, user, hostname);
    if (txnType != null) {
      rqst.setTxn_type(txnType);
    }
    return client.open_txns(rqst);
  }

  @Override
  public void close() throws Exception {
    if (transport != null && transport.isOpen()) {
      LOG.debug("Closing thrift transport");
      transport.close();
    }
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }
}
