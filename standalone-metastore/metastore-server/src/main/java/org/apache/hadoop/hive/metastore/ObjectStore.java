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

import static org.apache.hadoop.hive.metastore.Batchable.NO_BATCHING;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.COMPACTOR_USE_CUSTOM_POOL;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifiers;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.jdo.JDODataStoreException;
import javax.jdo.JDOException;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.identity.IntIdentity;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.DatabaseName;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.directsql.DirectSqlDeleteStats;
import org.apache.hadoop.hive.metastore.directsql.MetaStoreDirectSql;
import org.apache.hadoop.hive.metastore.directsql.MetaStoreDirectSql.SqlFilterForPushdown;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.AddPackageRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DropPackageRequest;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.GetPackageRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.ListPackageRequest;
import org.apache.hadoop.hive.metastore.api.ListStoredProcedureRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionFilterMode;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.QueryState;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.ReplicationMetricList;
import org.apache.hadoop.hive.metastore.api.GetReplicationMetricsRequest;
import org.apache.hadoop.hive.metastore.api.ReplicationMetrics;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.metastore.api.SchemaCompatibility;
import org.apache.hadoop.hive.metastore.api.SchemaType;
import org.apache.hadoop.hive.metastore.api.SchemaValidation;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SerdeType;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StoredProcedure;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableParamsUpdate;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMPoolTrigger;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlanStatus;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.directsql.DirectSqlAggrStats;
import org.apache.hadoop.hive.metastore.metastore.iface.PrivilegeStore;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.model.MCatalog;
import org.apache.hadoop.hive.metastore.model.MColumnDescriptor;
import org.apache.hadoop.hive.metastore.model.MConstraint;
import org.apache.hadoop.hive.metastore.model.MCreationMetadata;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MDataConnector;
import org.apache.hadoop.hive.metastore.model.MDCPrivilege;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MDelegationToken;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MFunction;
import org.apache.hadoop.hive.metastore.model.MISchema;
import org.apache.hadoop.hive.metastore.model.MMVSource;
import org.apache.hadoop.hive.metastore.model.MMasterKey;
import org.apache.hadoop.hive.metastore.model.MMetastoreDBProperties;
import org.apache.hadoop.hive.metastore.model.MOrder;
import org.apache.hadoop.hive.metastore.model.MPackage;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MPartitionEvent;
import org.apache.hadoop.hive.metastore.model.MResourceUri;
import org.apache.hadoop.hive.metastore.model.MRuntimeStat;
import org.apache.hadoop.hive.metastore.model.MScheduledExecution;
import org.apache.hadoop.hive.metastore.model.MScheduledQuery;
import org.apache.hadoop.hive.metastore.model.MSchemaVersion;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MStoredProc;
import org.apache.hadoop.hive.metastore.model.MStringList;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.hadoop.hive.metastore.model.MType;
import org.apache.hadoop.hive.metastore.model.MVersionTable;
import org.apache.hadoop.hive.metastore.model.MWMMapping;
import org.apache.hadoop.hive.metastore.model.MWMMapping.EntityType;
import org.apache.hadoop.hive.metastore.model.MWMPool;
import org.apache.hadoop.hive.metastore.model.MWMResourcePlan;
import org.apache.hadoop.hive.metastore.model.MWMResourcePlan.Status;
import org.apache.hadoop.hive.metastore.model.MWMTrigger;
import org.apache.hadoop.hive.metastore.model.MReplicationMetrics;
import org.apache.hadoop.hive.metastore.properties.CachingPropertyStore;
import org.apache.hadoop.hive.metastore.properties.PropertyStore;
import org.apache.hadoop.hive.metastore.metastore.PersistenceManagerProxy;
import org.apache.hadoop.hive.metastore.metastore.RawStoreAware;
import org.apache.hadoop.hive.metastore.metastore.MetaDescriptor;
import org.apache.hadoop.hive.metastore.metastore.TransactionHandler;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.RetryingExecutor;
import org.datanucleus.ExecutionContext;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.api.jdo.JDOTransaction;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/**
 * This class is the interface between the application logic and the database
 * store that contains the objects. Refrain putting any logic in mode.M* objects
 * or in this file as former could be auto generated and this class would need
 * to be made into a interface that can read both from a database and a
 * filestore.
 */
public class ObjectStore implements RawStore, Configurable {
  protected int batchSize = NO_BATCHING;

  private static final DateTimeFormatter YMDHMS_FORMAT = DateTimeFormatter.ofPattern(
      "yyyy_MM_dd_HH_mm_ss");
  /**
  * Verify the schema only once per JVM since the db connection info is static
  */
  private static final AtomicBoolean isSchemaVerified = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(ObjectStore.class);
  private int RM_PROGRESS_COL_WIDTH = 10000;
  private int RM_METADATA_COL_WIDTH = 4000;
  private int ORACLE_DB_MAX_COL_WIDTH = 4000;

  private enum TXN_STATUS {
    NO_STATE, OPEN, COMMITED, ROLLBACK
  }

  /**
   * Java system properties for configuring SSL to the database store
   */
  public static final String TRUSTSTORE_PATH_KEY = "javax.net.ssl.trustStore";
  public static final String TRUSTSTORE_PASSWORD_KEY = "javax.net.ssl.trustStorePassword";
  public static final String TRUSTSTORE_TYPE_KEY = "javax.net.ssl.trustStoreType";

  private static final String JDO_PARAM = ":param";

  /** Constant declaring a query parameter of type string and name key. */
  private static final String PTYPARAM_STR_KEY = "java.lang.String key";
  /** Constant declaring a property query predicate using equality. */
  private static final String PTYARG_EQ_KEY = "this.propertyKey == key";

  private boolean isInitialized = false;
  protected PersistenceManager pm = null;
  protected SQLGenerator sqlGenerator = null;
  private MetaStoreDirectSql directSql = null;
  private DirectSqlAggrStats directSqlAggrStats;
  protected DatabaseProduct dbType = null;
  protected Configuration conf;
  private volatile int openTrasactionCalls = 0;
  private Transaction currentTransaction = null;
  private TXN_STATUS transactionStatus = TXN_STATUS.NO_STATE;
  private Counter directSqlErrors;
  private boolean areTxnStatsSupported = false;
  private PropertyStore propertyStore;
  private Map<Class<?>, Object> cachedImpls = new HashMap<>();

  public ObjectStore() {
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Called whenever this object is instantiated using ReflectionUtils, and also
   * on connection retries. In cases of connection retries, conf will usually
   * contain modified values.
   */
  @Override
  @SuppressWarnings("nls")
  public void setConf(Configuration conf) {
    isInitialized = false;
    this.conf = conf;
    this.areTxnStatsSupported = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_TXN_STATS_ENABLED);
    configureSSL(conf);
    PersistenceManagerProvider.updatePmfProperties(conf);

    assert (!isActiveTransaction());
    shutdown();
    // Always want to re-create pm as we don't know if it were created by the
    // most recent instance of the pmf
    pm = null;
    directSql = null;
    directSqlAggrStats = null;
    openTrasactionCalls = 0;
    currentTransaction = null;
    transactionStatus = TXN_STATUS.NO_STATE;

    initialize();
    
    // Note, if metrics have not been initialized this will return null, which means we aren't
    // using metrics.  Thus we should always check whether this is non-null before using.
    MetricRegistry registry = Metrics.getRegistry();
    if (registry != null) {
      directSqlErrors = Metrics.getOrCreateCounter(MetricsConstants.DIRECTSQL_ERRORS);
    }

    this.batchSize = MetastoreConf.getIntVar(conf, ConfVars.RAWSTORE_PARTITION_BATCH_SIZE);

    if (!isInitialized) {
      throw new RuntimeException("Unable to create persistence manager. Check log for details");
    } else {
      LOG.debug("Initialized ObjectStore");
    }
  }

  @SuppressWarnings("nls")
  private void initialize() {
    LOG.debug("ObjectStore, initialize called");
    // if this method fails, PersistenceManagerProvider will retry for the configured number of times
    // before giving up
    boolean isForCompactor = MetastoreConf.getBoolVar(conf, COMPACTOR_USE_CUSTOM_POOL);
    pm = PersistenceManagerProvider.getPersistenceManager(isForCompactor);
    LOG.info("RawStore: {}, with PersistenceManager: {}" +
        " created in the thread with id: {}", this, pm, Thread.currentThread().getId());

    isInitialized = pm != null;
    if (isInitialized) {
      dbType = PersistenceManagerProvider.getDatabaseProduct();
      sqlGenerator = new SQLGenerator(dbType, conf);
      if (MetastoreConf.getBoolVar(getConf(), ConfVars.TRY_DIRECT_SQL)) {
        String schema = PersistenceManagerProvider.getProperty("javax.jdo.mapping.Schema");
        schema = org.apache.commons.lang3.StringUtils.defaultIfBlank(schema, null);
        directSql = new MetaStoreDirectSql(pm, conf, schema);
        directSqlAggrStats = new DirectSqlAggrStats(pm,conf,schema);
      }
    }
    if (propertyStore == null) {
      propertyStore = new CachingPropertyStore(new JdoPropertyStore(this), conf);
    }
  }

  /**
   * @return the property store instance
   */
  @Override
  public PropertyStore getPropertyStore() {
    return propertyStore;
  }

  /**
   * Configure SSL encryption to the database store.
   *
   * The following properties must be set correctly to enable encryption:
   *
   * 1. {@link MetastoreConf.ConfVars#DBACCESS_USE_SSL}
   * 2. {@link MetastoreConf.ConfVars#CONNECT_URL_KEY}
   * 3. {@link MetastoreConf.ConfVars#DBACCESS_SSL_TRUSTSTORE_PATH}
   * 4. {@link MetastoreConf.ConfVars#DBACCESS_SSL_TRUSTSTORE_PASSWORD}
   * 5. {@link MetastoreConf.ConfVars#DBACCESS_SSL_TRUSTSTORE_TYPE}
   *
   * The last three properties directly map to JSSE (Java) system properties. The Java layer will handle enabling
   * encryption once these properties are set.
   *
   * Additionally, {@link MetastoreConf.ConfVars#CONNECT_URL_KEY} must have the database-specific SSL flag in the connection URL.
   *
   * @param conf configuration values
   */
  private static void configureSSL(Configuration conf) {
    configureSSLDeprecated(conf); // TODO: Deprecate this method

    boolean useSSL = MetastoreConf.getBoolVar(conf, ConfVars.DBACCESS_USE_SSL);

    if (useSSL) {
      try {
        LOG.info("Setting SSL properties to connect to the database store");
        String trustStorePath = MetastoreConf.getVar(conf, ConfVars.DBACCESS_SSL_TRUSTSTORE_PATH).trim();
        // Specifying a truststore path is not necessary. If one is not provided, then the default Java truststore path will be used instead.
        if (!trustStorePath.isEmpty()) {
          System.setProperty(TRUSTSTORE_PATH_KEY, trustStorePath);
        } else {
          LOG.info(ConfVars.DBACCESS_SSL_TRUSTSTORE_PATH.toString() + " has not been set. Defaulting to jssecacerts, if it exists. Otherwise, cacerts.");
        }
        // If the truststore password has been configured and redacted properly using the Hadoop CredentialProvider API, then
        // MetastoreConf.getPassword() will securely decrypt it. Otherwise, it will default to being read in from the
        // configuration file in plain text.
        String trustStorePassword = MetastoreConf.getPassword(conf, ConfVars.DBACCESS_SSL_TRUSTSTORE_PASSWORD);
        if (!trustStorePassword.isEmpty()) {
          System.setProperty(TRUSTSTORE_PASSWORD_KEY, trustStorePassword);
        } else {
          LOG.info(ConfVars.DBACCESS_SSL_TRUSTSTORE_PASSWORD.toString() + " has not been set. Using default Java truststore password.");
        }
        // Already validated in MetaStoreConf
        String trustStoreType = MetastoreConf.getVar(conf, ConfVars.DBACCESS_SSL_TRUSTSTORE_TYPE);
        System.setProperty(TRUSTSTORE_TYPE_KEY, trustStoreType);
      } catch (IOException e) {
        throw new RuntimeException("Failed to set the SSL properties to connect to the database store.", e);
      }
    }
  }

  /**
   * Configure the SSL properties of the connection from provided config
   *
   * This method was kept for backwards compatibility purposes.
   *
   * The property {@link MetastoreConf.ConfVars#DBACCESS_SSL_PROPS} was deprecated in HIVE-20992 in favor of more
   * transparent and user-friendly properties.
   *
   * Please use the MetastoreConf.ConfVars#DBACCESS_SSL_* instead. Setting those properties will overwrite the values
   * of the deprecated property.
   *
   * The process of completely removing this property and its functionality is being tracked in HIVE-21024.
   *
   * @param conf configuration values
   */
  @Deprecated
  private static void configureSSLDeprecated(Configuration conf) {
    // SSL support
    String sslPropString = MetastoreConf.getVar(conf, ConfVars.DBACCESS_SSL_PROPS);
    if (org.apache.commons.lang3.StringUtils.isNotEmpty(sslPropString)) {
      LOG.warn("Configuring SSL using a deprecated key " + ConfVars.DBACCESS_SSL_PROPS.toString() +
              ". This may be removed in the future. See HIVE-20992 for more details.");
      LOG.info("Metastore setting SSL properties of the connection to backend DB");
      for (String sslProp : sslPropString.split(",")) {
        String[] pair = sslProp.trim().split("=");
        if (pair.length == 2) {
          System.setProperty(pair[0].trim(), pair[1].trim());
        } else {
          LOG.warn("Invalid metastore property value for {}", ConfVars.DBACCESS_SSL_PROPS);
        }
      }
    }
  }

  @InterfaceAudience.LimitedPrivate({"HCATALOG"})
  @InterfaceStability.Evolving
  public PersistenceManager getPersistenceManager() {
    return PersistenceManagerProvider.getPersistenceManager();
  }

  @Override
  public void shutdown() {
    LOG.info("RawStore: {}, with PersistenceManager: {} will be shutdown", this, pm);
    if (pm != null) {
      pm.close();
      pm = null;
    }
  }

  /**
   * Opens a new one or the one already created. Every call of this function must
   * have corresponding commit or rollback function call.
   *
   * @return an active transaction
   */
  @Override
  public boolean openTransaction() {
    openTrasactionCalls++;
    if (openTrasactionCalls == 1) {
      currentTransaction = pm.currentTransaction();
      currentTransaction.begin();
      transactionStatus = TXN_STATUS.OPEN;
    } else {
      // openTransactionCalls > 1 means this is an interior transaction
      // We should already have a transaction created that is active.
      if ((currentTransaction == null) || (!currentTransaction.isActive())){
        throw new RuntimeException("openTransaction called in an interior"
            + " transaction scope, but currentTransaction is not active.");
      }
    }

    boolean result = currentTransaction.isActive();
    debugLog("Open transaction: count = " + openTrasactionCalls + ", isActive = " + result);
    return result;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) {
    MetaDescriptor descriptor = iface.getAnnotation(MetaDescriptor.class);
    if (descriptor == null) {
      throw new IllegalArgumentException("Unable to unwrap the store as " + iface);
    }
    String implClassName = conf.get("metastore." + descriptor.alias() + ".store.impl", "");
    T impl = (T) cachedImpls.get(iface);
    if (impl != null && impl.getClass().getName().equals(implClassName)) {
      return impl;
    }

    Class<?> ifaceImpl = conf.getClass(implClassName, descriptor.defaultImpl());
    T simpl = (T) JavaUtils.newInstance(ifaceImpl);
    List<Query> trackOpenedQueries = new LinkedList<>();
    if (simpl instanceof RawStoreAware rsa) {
      rsa.setBaseStore(this);
      rsa.setPersistentManager(PersistenceManagerProxy.getProxy(pm, trackOpenedQueries));
    }
    impl = TransactionHandler.getProxy(iface, new TransactionHandler<>(this, simpl, trackOpenedQueries));
    cachedImpls.put(iface, impl);
    return impl;
  }

  @Override
  public void updateTableParams(List<TableParamsUpdate> updates) throws MetaException, NoSuchObjectException {
    if (updates == null || updates.isEmpty()) {
      return;
    }

    new GetListHelper<Void>(null, null, null, true, false) {
      @Override
      protected List<Void> getSqlResult(GetHelper<List<Void>> ctx) throws MetaException {
        boolean success = false;
        try {
          openTransaction();
          directSql.updateTableParams(updates, ObjectStore.this::getTable);
          success = commitTransaction();
        } finally {
          rollbackAndCleanup(success, null);
        }
        return null;
      }

      @Override
      protected List<Void> getJdoResult(GetHelper<List<Void>> ctx) {
        throw new UnsupportedOperationException("UnsupportedOperationException");
      }
    }.run(false);
  }

  /**
   * if this is the commit of the first open call then an actual commit is
   * called.
   *
   * @return Always returns true
   */
  @Override
  @SuppressWarnings("nls")
  public boolean commitTransaction() {
    if (TXN_STATUS.ROLLBACK == transactionStatus) {
      debugLog("Commit transaction: rollback");
      return false;
    }
    if (openTrasactionCalls <= 0) {
      RuntimeException e = new RuntimeException("commitTransaction was called but openTransactionCalls = "
          + openTrasactionCalls + ". This probably indicates that there are unbalanced " +
          "calls to openTransaction/commitTransaction");
      LOG.error("Unbalanced calls to open/commit Transaction", e);
      throw e;
    }
    if (!currentTransaction.isActive()) {
      RuntimeException e = new RuntimeException("commitTransaction was called but openTransactionCalls = "
          + openTrasactionCalls + ". This probably indicates that there are unbalanced " +
          "calls to openTransaction/commitTransaction");
      LOG.error("Unbalanced calls to open/commit Transaction", e);
      throw e;
    }
    openTrasactionCalls--;
    debugLog("Commit transaction: count = " + openTrasactionCalls + ", isactive "+ currentTransaction.isActive());

    if ((openTrasactionCalls == 0) && currentTransaction.isActive()) {
      transactionStatus = TXN_STATUS.COMMITED;
      currentTransaction.commit();
    }
    return true;
  }

  /**
   * @return true if there is an active transaction. If the current transaction
   *         is either committed or rolled back it returns false
   */
  @Override
  public boolean isActiveTransaction() {
    if (currentTransaction == null) {
      return false;
    }
    return currentTransaction.isActive();
  }

  /**
   * Rolls back the current transaction if it is active
   */
  @Override
  public void rollbackTransaction() {
    debugLog("Rollback transaction, isActive: " + isActiveTransaction());
    try {
      if (isActiveTransaction() && transactionStatus != TXN_STATUS.ROLLBACK) {
        currentTransaction.rollback();
      }
    } finally {
      openTrasactionCalls = 0;
      transactionStatus = TXN_STATUS.ROLLBACK;
      // remove all detached objects from the cache, since the transaction is
      // being rolled back they are no longer relevant, and this prevents them
      // from reattaching in future transactions
      pm.evictAll();
    }
  }

  private void setTransactionSavePoint(String savePoint) {
    if (savePoint != null) {
      ExecutionContext ec = ((JDOPersistenceManager) pm).getExecutionContext();
      ec.getStoreManager().getConnectionManager().getConnection(ec);
      ((JDOTransaction) currentTransaction).setSavepoint(savePoint);
    }
  }

  private void rollbackTransactionToSavePoint(String savePoint) {
    if (savePoint != null) {
      ((JDOTransaction) currentTransaction).rollbackToSavepoint(savePoint);
    }
  }

  @Override
  public void createCatalog(Catalog cat) throws MetaException {
    LOG.debug("Creating catalog {}", cat);
    boolean committed = false;
    MCatalog mCat = catToMCat(cat);
    try {
      openTransaction();
      pm.makePersistent(mCat);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public void alterCatalog(String catName, Catalog cat)
      throws MetaException, InvalidOperationException {
    if (!cat.getName().equals(catName)) {
      throw new InvalidOperationException("You cannot change a catalog's name");
    }
    boolean committed = false;
    try {
      MCatalog mCat = getMCatalog(catName);
      if (org.apache.commons.lang3.StringUtils.isNotBlank(cat.getLocationUri())) {
        mCat.setLocationUri(cat.getLocationUri());
      }
      if (org.apache.commons.lang3.StringUtils.isNotBlank(cat.getDescription())) {
        mCat.setDescription(cat.getDescription());
      }
      mCat.setParameters(cat.getParameters());
      openTransaction();
      pm.makePersistent(mCat);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    LOG.debug("Fetching catalog {}", catalogName);
    MCatalog mCat = getMCatalog(catalogName);
    if (mCat == null) {
      throw new NoSuchObjectException("No catalog " + catalogName);
    }
    return mCatToCat(mCat);
  }

  @Override
  public List<String> getCatalogs() {
    LOG.debug("Fetching all catalog names");
    boolean commited = false;
    List<String> catalogs = null;

    String queryStr = "select name from org.apache.hadoop.hive.metastore.model.MCatalog";
    Query query = null;

    openTransaction();
    try {
      query = pm.newQuery(queryStr);
      query.setResult("name");
      catalogs = new ArrayList<>((Collection<String>) query.execute());
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    Collections.sort(catalogs);
    return catalogs;
  }

  @Override
  public void dropCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    LOG.debug("Dropping catalog {}", catalogName);
    boolean committed = false;
    try {
      openTransaction();
      MCatalog mCat = getMCatalog(catalogName);
      pm.retrieve(mCat);
      if (mCat == null) {
        throw new NoSuchObjectException("No catalog " + catalogName);
      }
      pm.deletePersistent(mCat);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  private MCatalog getMCatalog(String catalogName) {
    boolean committed = false;
    Query query = null;
    try {
      openTransaction();
      catalogName = normalizeIdentifier(catalogName);
      query = pm.newQuery(MCatalog.class, "name == catname");
      query.declareParameters("java.lang.String catname");
      query.setUnique(true);
      MCatalog mCat = (MCatalog)query.execute(catalogName);
      pm.retrieve(mCat);
      committed = commitTransaction();
      return mCat;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  private MCatalog catToMCat(Catalog cat) {
    MCatalog mCat = new MCatalog();
    mCat.setName(normalizeIdentifier(cat.getName()));
    if (cat.isSetDescription()) {
      mCat.setDescription(cat.getDescription());
    }
    mCat.setParameters(cat.getParameters());
    mCat.setLocationUri(cat.getLocationUri());
    mCat.setCreateTime(cat.getCreateTime());
    return mCat;
  }

  private Catalog mCatToCat(MCatalog mCat) {
    Catalog cat = new Catalog(mCat.getName(), mCat.getLocationUri());
    if (mCat.getDescription() != null) {
      cat.setDescription(mCat.getDescription());
    }
    if (mCat.getParameters() != null) {
      cat.setParameters(mCat.getParameters());
    }
    cat.setCreateTime(mCat.getCreateTime());
    return cat;
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    boolean commited = false;
    MDatabase mdb = new MDatabase();
    assert db.getCatalogName() != null;
    mdb.setCatalogName(normalizeIdentifier(db.getCatalogName()));
    assert mdb.getCatalogName() != null;
    mdb.setName(db.getName().toLowerCase());
    mdb.setLocationUri(db.getLocationUri());
    mdb.setManagedLocationUri(db.getManagedLocationUri());
    mdb.setDescription(db.getDescription());
    mdb.setParameters(db.getParameters());
    mdb.setOwnerName(db.getOwnerName());
    mdb.setDataConnectorName(db.getConnector_name());
    mdb.setRemoteDatabaseName(db.getRemote_dbname());
    if (db.getType() == null) {
      mdb.setType(DatabaseType.NATIVE.name());
    } else {
      mdb.setType(db.getType().name());
    }
    PrincipalType ownerType = db.getOwnerType();
    mdb.setOwnerType((null == ownerType ? PrincipalType.USER.name() : ownerType.name()));
    mdb.setCreateTime(db.getCreateTime());
    try {
      openTransaction();
      pm.makePersistent(mdb);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, null);
    }
  }

  @SuppressWarnings("nls")
  private MDatabase getMDatabase(String catName, String name) throws NoSuchObjectException {
    MDatabase mdb = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      name = normalizeIdentifier(name);
      catName = normalizeIdentifier(catName);
      query = pm.newQuery(MDatabase.class, "name == dbname && catalogName == catname");
      query.declareParameters("java.lang.String dbname, java.lang.String catname");
      query.setUnique(true);
      mdb = (MDatabase) query.execute(name, catName);
      pm.retrieve(mdb);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    if (mdb == null) {
      throw new NoSuchObjectException("There is no database " + catName + "." + name);
    }
    return mdb;
  }

  @Override
  public Database getDatabase(String catalogName, String name) throws NoSuchObjectException {
    MetaException ex = null;
    Database db = null;
    try {
      db = getDatabaseInternal(catalogName, name);
    } catch (MetaException e) {
      // Signature restriction to NSOE, and NSOE being a flat exception prevents us from
      // setting the cause of the NSOE as the MetaException. We should not lose the info
      // we got here, but it's very likely that the MetaException is irrelevant and is
      // actually an NSOE message, so we should log it and throw an NSOE with the msg.
      ex = e;
    }
    if (db == null) {
      LOG.debug("Failed to get database {}.{}, returning NoSuchObjectException",
          catalogName, name, ex);
      final String errorMessage = (ex == null ? "" : (": " + ex.getMessage()));
      throw new NoSuchObjectException("database " + catalogName + "." + name + errorMessage);
    }
    return db;
  }

  public Database getDatabaseInternal(String catalogName, String name)
      throws MetaException, NoSuchObjectException {
    return new GetDbHelper(catalogName, name, true, true) {
      @Override
      protected Database getSqlResult(GetHelper<Database> ctx) throws MetaException {
        return directSql.getDatabase(catalogName, dbName);
      }

      @Override
      protected Database getJdoResult(GetHelper<Database> ctx) throws MetaException, NoSuchObjectException {
        return getJDODatabase(catalogName, dbName);
      }
    }.run(false);
   }

  public Database getJDODatabase(String catName, String name) throws NoSuchObjectException {
    MDatabase mdb = null;
    boolean commited = false;
    try {
      openTransaction();
      mdb = getMDatabase(catName, name);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, null);
    }
    Database db = new Database();
    db.setName(mdb.getName());
    db.setDescription(mdb.getDescription());
    db.setParameters(convertMap(mdb.getParameters(), conf));
    db.setOwnerName(mdb.getOwnerName());
    String type = org.apache.commons.lang3.StringUtils.defaultIfBlank(mdb.getOwnerType(), null);
    PrincipalType principalType = (type == null) ? null : PrincipalType.valueOf(type);
    db.setOwnerType(principalType);
    if (mdb.getType().equalsIgnoreCase(DatabaseType.NATIVE.name())) {
      db.setType(DatabaseType.NATIVE);
    } else {
      db.setType(DatabaseType.REMOTE);
      db.setConnector_name(org.apache.commons.lang3.StringUtils.defaultIfBlank(mdb.getDataConnectorName(), null));
      db.setRemote_dbname(org.apache.commons.lang3.StringUtils.defaultIfBlank(mdb.getRemoteDatabaseName(), null));
    }
    db.setLocationUri(mdb.getLocationUri());
    db.setManagedLocationUri(org.apache.commons.lang3.StringUtils.defaultIfBlank(mdb.getManagedLocationUri(), null));
    db.setCatalogName(catName);
    db.setCreateTime(mdb.getCreateTime());
    return db;
  }

  /**
   * Alter the database object in metastore. Currently only the parameters
   * of the database or the owner can be changed.
   * @param dbName the database name
   * @param db the Hive Database object
   */
  @Override
  public boolean alterDatabase(String catName, String dbName, Database db)
    throws MetaException, NoSuchObjectException {

    MDatabase mdb = null;
    boolean committed = false;
    try {
      mdb = getMDatabase(catName, dbName);
      mdb.setParameters(db.getParameters());
      mdb.setOwnerName(db.getOwnerName());
      if (db.getOwnerType() != null) {
        mdb.setOwnerType(db.getOwnerType().name());
      }
      if (org.apache.commons.lang3.StringUtils.isNotBlank(db.getDescription())) {
        mdb.setDescription(db.getDescription());
      }
      if (org.apache.commons.lang3.StringUtils.isNotBlank(db.getLocationUri())) {
        mdb.setLocationUri(db.getLocationUri());
      }
      if (org.apache.commons.lang3.StringUtils.isNotBlank(db.getManagedLocationUri())) {
        mdb.setManagedLocationUri(db.getManagedLocationUri());
      }
      openTransaction();
      pm.makePersistent(mdb);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
    return committed;
  }

  @Override
  public boolean dropDatabase(String catName, String dbname)
      throws NoSuchObjectException, MetaException {
    boolean success = false;
    LOG.info("Dropping database {}.{} along with all tables", catName, dbname);
    dbname = normalizeIdentifier(dbname);
    catName = normalizeIdentifier(catName);
    try {
      openTransaction();

      // then drop the database
      MDatabase db = getMDatabase(catName, dbname);
      pm.retrieve(db);
      List<MDBPrivilege> dbGrants = unwrap(PrivilegeStore.class).listDatabaseGrants(catName, dbname, null);
      if (CollectionUtils.isNotEmpty(dbGrants)) {
        pm.deletePersistentAll(dbGrants);
      }
      pm.deletePersistent(db);
      success = commitTransaction();
    } catch (Exception e) {
      LOG.error("Failed to drop database", e);
      throw new MetaException(e.getMessage());
    } finally {
      rollbackAndCleanup(success, null);
    }
    return success;
  }

  @Override
  public List<String> getDatabases(String catName, String pattern) throws MetaException {
    if (pattern == null || pattern.equals("*")) {
      return getAllDatabases(catName);
    }
    boolean commited = false;
    List<String> databases = null;
    Query query = null;
    try {
      openTransaction();
      // Take the pattern and split it on the | to get all the composing
      // patterns
      StringBuilder filterBuilder = new StringBuilder();
      List<String> parameterVals = new ArrayList<>();
      appendSimpleCondition(filterBuilder, "catalogName", new String[] {catName}, parameterVals);
      appendPatternCondition(filterBuilder, "name", pattern, parameterVals);
      query = pm.newQuery(MDatabase.class, filterBuilder.toString());
      query.setResult("name");
      query.setOrdering("name ascending");
      Collection<String> names = (Collection<String>) query.executeWithArray(parameterVals.toArray(new String[0]));
      databases = new ArrayList<>(names);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return databases;
  }

  @Override
  public List<String> getAllDatabases(String catName) throws MetaException {
    boolean commited = false;
    List<String> databases = null;

    Query query = null;
    catName = normalizeIdentifier(catName);

    openTransaction();
    try {
      query = pm.newQuery("select name from org.apache.hadoop.hive.metastore.model.MDatabase " +
          "where catalogName == catname");
      query.declareParameters("java.lang.String catname");
      query.setResult("name");
      databases = new ArrayList<>((Collection<String>) query.execute(catName));
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    Collections.sort(databases);
    return databases;
  }

  @Override
  public List<Database> getDatabaseObjects(String catName, String pattern) throws MetaException {
    boolean commited = false;
    List<Database> databases = new ArrayList<>();
    Query query = null;
    catName = normalizeIdentifier(catName);
    try {
      openTransaction();
      StringBuilder filterBuilder = new StringBuilder();
      List<String> parameterVals = new ArrayList<>();
      appendSimpleCondition(filterBuilder, "catalogName", new String[]{catName}, parameterVals);
      if (!(pattern == null || pattern.equals("*"))) {
        appendPatternCondition(filterBuilder, "name", pattern, parameterVals);
      }
      query = pm.newQuery(MDatabase.class, filterBuilder.toString());
      query.setOrdering("name ascending");
      Collection<MDatabase> mDBs = (Collection<MDatabase>) query.executeWithArray(parameterVals.toArray(new String[0]));
      for (MDatabase mdb : mDBs) {
        databases.add(convertToDatabase(mdb));
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return databases;
  }

  private Database convertToDatabase(MDatabase mdb) {
    Database db = new Database();
    db.setName(mdb.getName());
    db.setDescription(mdb.getDescription());
    db.setParameters(convertMap(mdb.getParameters(), conf));
    db.setOwnerName(mdb.getOwnerName());
    String type = org.apache.commons.lang3.StringUtils.defaultIfBlank(mdb.getOwnerType(), null);
    PrincipalType principalType = (type == null) ? null : PrincipalType.valueOf(type);
    db.setOwnerType(principalType);
    if (mdb.getType().equalsIgnoreCase(DatabaseType.NATIVE.name())) {
      db.setType(DatabaseType.NATIVE);
      db.setLocationUri(mdb.getLocationUri());
      db.setManagedLocationUri(org.apache.commons.lang3.StringUtils.defaultIfBlank(mdb.getManagedLocationUri(), null));
    } else {
      db.setType(DatabaseType.REMOTE);
      db.setConnector_name(org.apache.commons.lang3.StringUtils.defaultIfBlank(mdb.getDataConnectorName(), null));
      db.setRemote_dbname(org.apache.commons.lang3.StringUtils.defaultIfBlank(mdb.getRemoteDatabaseName(), null));
    }
    db.setCatalogName(mdb.getCatalogName());
    db.setCreateTime(mdb.getCreateTime());
    return db;
  }

  @Override
  public void createDataConnector(DataConnector connector) throws InvalidObjectException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      pm.makePersistent(convert(connector));
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, null);
    }
  }

  public static MDataConnector convert(DataConnector connector) {
    MDataConnector mDataConnector = new MDataConnector();
    mDataConnector.setName(connector.getName().toLowerCase());
    mDataConnector.setType(connector.getType());
    mDataConnector.setUrl(connector.getUrl());
    mDataConnector.setDescription(connector.getDescription());
    mDataConnector.setParameters(connector.getParameters());
    mDataConnector.setOwnerName(connector.getOwnerName());
    PrincipalType ownerType = connector.getOwnerType();
    mDataConnector.setOwnerType((null == ownerType ? PrincipalType.USER.name() : ownerType.name()));
    mDataConnector.setCreateTime(connector.getCreateTime());
    return mDataConnector;
  }

  @SuppressWarnings("nls")
  private MDataConnector getMDataConnector(String name) throws NoSuchObjectException {
    MDataConnector mdc = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      name = normalizeIdentifier(name);
      query = pm.newQuery(MDataConnector.class, "name == dcname");
      query.declareParameters("java.lang.String dcname");
      query.setUnique(true);
      mdc = (MDataConnector) query.execute(name);
      pm.retrieve(mdc);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    if (mdc == null) {
      throw new NoSuchObjectException("There is no dataconnector " + name);
    }
    return mdc;
  }

  @Override
  public DataConnector getDataConnector(String name) throws NoSuchObjectException {
    MDataConnector mdc = null;
    boolean commited = false;
    try {
      openTransaction();
      mdc = getMDataConnector(name);
      commited = commitTransaction();
    } catch (NoSuchObjectException no) {
      throw new NoSuchObjectException("Dataconnector named " + name + " does not exist:" + no.getCause());
    } finally {
      rollbackAndCleanup(commited, null);
    }
    DataConnector connector = new DataConnector();
    connector.setName(mdc.getName());
    connector.setType(mdc.getType());
    connector.setUrl(mdc.getUrl());
    connector.setDescription(mdc.getDescription());
    connector.setParameters(convertMap(mdc.getParameters(), conf));
    connector.setOwnerName(mdc.getOwnerName());
    String type = org.apache.commons.lang3.StringUtils.defaultIfBlank(mdc.getOwnerType(), null);
    PrincipalType principalType = (type == null) ? null : PrincipalType.valueOf(type);
    connector.setOwnerType(principalType);
    connector.setCreateTime(mdc.getCreateTime());
    return connector;
  }

  @Override
  public List<String> getAllDataConnectorNames() throws MetaException {
    boolean commited = false;
    List<String> connectors = null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MDataConnector.class);
      query.setResult("name");
      query.setOrdering("name ascending");
      Collection<String> names = (Collection<String>) query.executeWithArray();
      connectors = new ArrayList<>(names);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return connectors;
  }

  /**
   * Alter the dataconnector object in metastore. Currently only the parameters
   * of the dataconnector or the owner can be changed.
   * @param dcName the dataconnector name
   * @param connector the Hive DataConnector object
   */
  @Override
  public boolean alterDataConnector(String dcName, DataConnector connector)
      throws MetaException, NoSuchObjectException {

    MDataConnector mdc = null;
    boolean committed = false;
    try {
      mdc = getMDataConnector(dcName);
      mdc.setUrl(connector.getUrl());
      mdc.setParameters(connector.getParameters());
      mdc.setOwnerName(connector.getOwnerName());
      if (connector.getOwnerType() != null) {
        mdc.setOwnerType(connector.getOwnerType().name());
      }
      if (org.apache.commons.lang3.StringUtils.isNotBlank(connector.getDescription())) {
        mdc.setDescription(connector.getDescription());
      }
      openTransaction();
      pm.makePersistent(mdc);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
    return committed;
  }

  @Override
  public boolean dropDataConnector(String dcname)
      throws NoSuchObjectException, MetaException {
    boolean success = false;
    LOG.info("Dropping dataconnector {} ", dcname);
    dcname = normalizeIdentifier(dcname);
    try {
      openTransaction();

      // then drop the dataconnector
      MDataConnector mdb = getMDataConnector(dcname);
      pm.retrieve(mdb);
      List<MDCPrivilege> dcGrants = unwrap(PrivilegeStore.class).listDataConnectorGrants(dcname, null);
      if (CollectionUtils.isNotEmpty(dcGrants)) {
        pm.deletePersistentAll(dcGrants);
      }
      pm.deletePersistent(mdb);
      success = commitTransaction();
    } catch (Exception e) {
      LOG.error("Failed to drop data connector", e);
      throw new MetaException(e.getMessage());
    } finally {
      rollbackAndCleanup(success, null);
    }
    return success;
  }

  private MType getMType(Type type) {
    List<MFieldSchema> fields = new ArrayList<>();
    if (type.getFields() != null) {
      for (FieldSchema field : type.getFields()) {
        fields.add(new MFieldSchema(field.getName(), field.getType(), field
            .getComment()));
      }
    }
    return new MType(type.getName(), type.getType1(), type.getType2(), fields);
  }

  private Type getType(MType mtype) {
    List<FieldSchema> fields = new ArrayList<>();
    if (mtype.getFields() != null) {
      for (MFieldSchema field : mtype.getFields()) {
        fields.add(new FieldSchema(field.getName(), field.getType(), field
            .getComment()));
      }
    }
    Type ret = new Type();
    ret.setName(mtype.getName());
    ret.setType1(mtype.getType1());
    ret.setType2(mtype.getType2());
    ret.setFields(fields);
    return ret;
  }

  @Override
  public boolean createType(Type type) {
    boolean success = false;
    MType mtype = getMType(type);
    boolean commited = false;
    try {
      openTransaction();
      pm.makePersistent(mtype);
      commited = commitTransaction();
      success = true;
    } finally {
      rollbackAndCleanup(commited, null);
    }
    return success;
  }

  @Override
  public Type getType(String typeName) {
    Type type = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MType.class, "name == typeName");
      query.declareParameters("java.lang.String typeName");
      query.setUnique(true);
      MType mtype = (MType) query.execute(typeName.trim());
      if (mtype != null) {
        type = getType(mtype);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return type;
  }

  @Override
  public boolean dropType(String typeName) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MType.class, "name == typeName");
      query.declareParameters("java.lang.String typeName");
      query.setUnique(true);
      MType type = (MType) query.execute(typeName.trim());
      pm.retrieve(type);
      if (type != null) {
        pm.deletePersistent(type);
      }
      success = commitTransaction();
    } catch (JDOObjectNotFoundException e) {
      success = commitTransaction();
      LOG.debug("type not found {}", typeName, e);
    } finally {
      rollbackAndCleanup(success, query);
    }
    return success;
  }

  @Override
  public SQLAllTableConstraints createTableWithConstraints(Table tbl, SQLAllTableConstraints constraints)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      createTable(tbl);
      // Add constraints.
      // We need not do a deep retrieval of the Table Column Descriptor while persisting the
      // constraints since this transaction involving create table is not yet committed.
      if (CollectionUtils.isNotEmpty(constraints.getForeignKeys())) {
        constraints.setForeignKeys(addForeignKeys(constraints.getForeignKeys(), false, constraints.getPrimaryKeys(),
            constraints.getUniqueConstraints()));
      }
      if (CollectionUtils.isNotEmpty(constraints.getPrimaryKeys())) {
        constraints.setPrimaryKeys(addPrimaryKeys(constraints.getPrimaryKeys(), false));
      }
      if (CollectionUtils.isNotEmpty(constraints.getUniqueConstraints())) {
        constraints.setUniqueConstraints(addUniqueConstraints(constraints.getUniqueConstraints(), false));
      }
      if (CollectionUtils.isNotEmpty(constraints.getNotNullConstraints())) {
        constraints.setNotNullConstraints(addNotNullConstraints(constraints.getNotNullConstraints(), false));
      }
      if (CollectionUtils.isNotEmpty(constraints.getDefaultConstraints())) {
        constraints.setDefaultConstraints(addDefaultConstraints(constraints.getDefaultConstraints(), false));
      }
      if (CollectionUtils.isNotEmpty(constraints.getCheckConstraints())) {
        constraints.setCheckConstraints(addCheckConstraints(constraints.getCheckConstraints(), false));
      }
      success = commitTransaction();
      return constraints;
    } finally {
      rollbackAndCleanup(success, null);
    }
  }

  /**
   * Convert PrivilegeGrantInfo from privMap to MTablePrivilege, and add all of
   * them to the toPersistPrivObjs. These privilege objects will be persisted as
   * part of createTable.
   */
  public static void putPersistentPrivObjects(MTable mtbl, List<Object> toPersistPrivObjs,
      int now, Map<String, List<PrivilegeGrantInfo>> privMap, PrincipalType type, String authorizer) {
    if (privMap != null) {
      for (Map.Entry<String, List<PrivilegeGrantInfo>> entry : privMap
          .entrySet()) {
        String principalName = entry.getKey();
        List<PrivilegeGrantInfo> privs = entry.getValue();
        for (int i = 0; i < privs.size(); i++) {
          PrivilegeGrantInfo priv = privs.get(i);
          if (priv == null) {
            continue;
          }
          MTablePrivilege mTblSec = new MTablePrivilege(
              principalName, type.toString(), mtbl, priv.getPrivilege(),
              now, priv.getGrantor(), priv.getGrantorType().toString(), priv
                  .isGrantOption(), authorizer);
          toPersistPrivObjs.add(mTblSec);
        }
      }
    }
  }

  private List<MConstraint> listAllTableConstraintsWithOptionalConstraintName(
      String catName, String dbName, String tableName, String constraintname) {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    constraintname = constraintname!=null?normalizeIdentifier(constraintname):null;
    List<MConstraint> mConstraints = null;
    List<String> constraintNames = new ArrayList<>();

    try (QueryWrapper queryForConstraintName = new QueryWrapper(pm.newQuery("select constraintName from org.apache.hadoop.hive.metastore.model.MConstraint  where "
        + "((parentTable.tableName == ptblname && parentTable.database.name == pdbname && " +
        "parentTable.database.catalogName == pcatname) || "
        + "(childTable != null && childTable.tableName == ctblname &&" +
        "childTable.database.name == cdbname && childTable.database.catalogName == ccatname)) " +
        (constraintname != null ? " && constraintName == constraintname" : "")));
        QueryWrapper queryForMConstraint = new QueryWrapper(pm.newQuery(MConstraint.class))) {

      queryForConstraintName.declareParameters("java.lang.String ptblname, java.lang.String pdbname,"
          + "java.lang.String pcatname, java.lang.String ctblname, java.lang.String cdbname," +
          "java.lang.String ccatname" +
        (constraintname != null ? ", java.lang.String constraintname" : ""));
      Collection<?> constraintNamesColl =
        constraintname != null ?
          ((Collection<?>) queryForConstraintName.
            executeWithArray(tableName, dbName, catName, tableName, dbName, catName, constraintname)):
          ((Collection<?>) queryForConstraintName.
            executeWithArray(tableName, dbName, catName, tableName, dbName, catName));
      for (Iterator<?> i = constraintNamesColl.iterator(); i.hasNext();) {
        String currName = (String) i.next();
        constraintNames.add(currName);
      }

      queryForMConstraint.setFilter("param.contains(constraintName)");
      queryForMConstraint.declareParameters("java.util.Collection param");
      Collection<?> constraints = (Collection<?>)queryForMConstraint.execute(constraintNames);
      mConstraints = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currConstraint = (MConstraint) i.next();
        mConstraints.add(currConstraint);
      }
    }
    return mConstraints;
  }

  @Override
  public List<TableName> getTableNamesWithStats() throws MetaException, NoSuchObjectException {
    return new GetListHelper<TableName>(null, null, null, true, false) {
      @Override
      protected List<TableName> getSqlResult(
          GetHelper<List<TableName>> ctx) throws MetaException {
        return directSql.getTableNamesWithStats();
      }

      @Override
      protected List<TableName> getJdoResult(
          GetHelper<List<TableName>> ctx) throws MetaException {
        throw new UnsupportedOperationException("UnsupportedOperationException"); // TODO: implement?
      }
    }.run(false);
  }

  @Override
  public Map<String, List<String>> getPartitionColsWithStats(String catName, String dbName, String tableName)
      throws MetaException, NoSuchObjectException {
    return new GetHelper<Map<String, List<String>>>(catName, dbName, null, true, false) {
      @Override
      protected Map<String, List<String>> getSqlResult(
          GetHelper<Map<String, List<String>>> ctx) throws MetaException {
        try {
          return directSql.getColAndPartNamesWithStats(catName, dbName, tableName);
        } catch (Throwable ex) {
          LOG.error("DirectSQL failed", ex);
          throw new MetaException(ex.getMessage());
        }
      }

      @Override
      protected Map<String, List<String>> getJdoResult(
          GetHelper<Map<String, List<String>>> ctx) throws MetaException {
        throw new UnsupportedOperationException("UnsupportedOperationException"); // TODO: implement?
      }

      @Override
      protected String describeResult() {
        return results.size() + " partitions";
      }
    }.run(false);
  }

  @Override
  public List<TableName> getAllTableNamesForStats() throws MetaException, NoSuchObjectException {
    return new GetListHelper<TableName>(null, null, null, true, false) {
      @Override
      protected List<TableName> getSqlResult(
          GetHelper<List<TableName>> ctx) throws MetaException {
        return directSql.getAllTableNamesForStats();
      }

      @Override
      protected List<TableName> getJdoResult(
          GetHelper<List<TableName>> ctx) throws MetaException {
        boolean commited = false;
        Query query = null;
        List<TableName> result = new ArrayList<>();
        openTransaction();
        try {
          String paramStr = "", whereStr = "";
          for (int i = 0; i < MetaStoreDirectSql.STATS_TABLE_TYPES.length; ++i) {
            if (i != 0) {
              paramStr += ", ";
              whereStr += "||";
            }
            paramStr += "java.lang.String tt" + i;
            whereStr += " tableType == tt" + i;
          }
          query = pm.newQuery(MTable.class, whereStr);
          query.declareParameters(paramStr);
          Collection<MTable> tbls = (Collection<MTable>) query.executeWithArray(
              query, MetaStoreDirectSql.STATS_TABLE_TYPES);
          pm.retrieveAll(tbls);
          for (MTable tbl : tbls) {
            result.add(new TableName(
                tbl.getDatabase().getCatalogName(), tbl.getDatabase().getName(), tbl.getTableName()));
          }
          commited = commitTransaction();
        } finally {
          rollbackAndCleanup(commited, query);
        }
        return result;
      }
    }.run(false);
  }

  public static StringBuilder appendPatternCondition(StringBuilder builder,
      String fieldName, String elements, List<String> parameters) {
      elements = normalizeIdentifier(elements);
    return appendCondition(builder, fieldName, elements.split("\\|"), true, parameters);
  }

  public static StringBuilder appendSimpleCondition(StringBuilder builder,
      String fieldName, String[] elements, List<String> parameters) {
    return appendCondition(builder, fieldName, elements, false, parameters);
  }

  private static StringBuilder appendCondition(StringBuilder builder,
      String fieldName, String[] elements, boolean pattern, List<String> parameters) {
    if (builder.length() > 0) {
      builder.append(" && ");
    }
    builder.append(" (");
    int length = builder.length();
    for (String element : elements) {
      if (pattern) {
        element = element.replaceAll("\\*", ".*");
      }
      parameters.add(element);
      if (builder.length() > length) {
        builder.append(" || ");
      }
      builder.append(fieldName);
      if (pattern) {
        builder.append(".matches(").append(JDO_PARAM).append(parameters.size()).append(")");
      } else {
        builder.append(" == ").append(JDO_PARAM).append(parameters.size());
      }
    }
    builder.append(" )");
    return builder;
  }

  class AttachedMTableInfo {
    MTable mtbl;
    MColumnDescriptor mcd;

    public AttachedMTableInfo() {}

    public AttachedMTableInfo(MTable mtbl, MColumnDescriptor mcd) {
      this.mtbl = mtbl;
      this.mcd = mcd;
    }
  }

  private AttachedMTableInfo getMTable(String catName, String db, String table,
                                       boolean retrieveCD) {
    AttachedMTableInfo nmtbl = new AttachedMTableInfo();
    MTable mtbl = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      catName = normalizeIdentifier(Optional.ofNullable(catName).orElse(getDefaultCatalog(conf)));
      db = normalizeIdentifier(db);
      table = normalizeIdentifier(table);
      query = pm.newQuery(MTable.class,
          "tableName == table && database.name == db && database.catalogName == catname");
      query.declareParameters(
          "java.lang.String table, java.lang.String db, java.lang.String catname");
      query.setUnique(true);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Executing getMTable for {}",
            TableName.getQualified(catName, db, table));
      }
      mtbl = (MTable) query.execute(table, db, catName);
      pm.retrieve(mtbl);
      // Retrieving CD can be expensive and unnecessary, so do it only when required.
      if (mtbl != null && retrieveCD) {
        pm.retrieve(mtbl.getSd());
        pm.retrieveAll(mtbl.getSd().getCD());
        nmtbl.mcd = mtbl.getSd().getCD();
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    nmtbl.mtbl = mtbl;
    return nmtbl;
  }

  private MTable getMTable(String catName, String db, String table) {
    AttachedMTableInfo nmtbl = getMTable(catName, db, table, false);
    return nmtbl.mtbl;
  }

  /** Makes shallow copy of a list to avoid DataNucleus mucking with our objects. */
  private static <T> List<T> convertList(List<T> dnList) {
    return (dnList == null) ? null : Lists.newArrayList(dnList);
  }

  /** Makes shallow copy of a map to avoid DataNucleus mucking with our objects. */
  private static Map<String, String> convertMap(Map<String, String> dnMap, Configuration conf, GetPartitionsArgs... args) {
    Map<String, String> parameters = MetaStoreServerUtils.trimMapNulls(dnMap,
        MetastoreConf.getBoolVar(conf, ConfVars.ORM_RETRIEVE_MAPNULLS_AS_EMPTY_STRINGS));
    if (parameters != null && args != null && args.length == 1) {
      // Pattern matching in Java might be different from the one used by the metastore backends,
      // An underscore (_) in pattern stands for (matches) any single character;
      // a percent sign (%) matches any sequence of zero or more characters.
      // See TestGetPartitionsUsingProjectionAndFilterSpecs#testPartitionProjectionEmptySpec.
      Pattern includePatt = null;
      if (StringUtils.isNotBlank(args[0].getIncludeParamKeyPattern())) {
        includePatt = Optional.of(args[0].getIncludeParamKeyPattern()).map(regex ->
            Pattern.compile(regex.replaceAll("%", ".*").replaceAll("_", "."))).get();
      }
      Pattern excludePatt = null;
      if (StringUtils.isNotBlank(args[0].getExcludeParamKeyPattern())) {
        excludePatt = Optional.of(args[0].getExcludeParamKeyPattern()).map(regex ->
            Pattern.compile(regex.replaceAll("%", ".*").replaceAll("_", "."))).get();;
      }
      final Pattern includePattern = includePatt;
      final Pattern excludePattern = excludePatt;
      return parameters.entrySet().stream().filter(entry -> {
        boolean matches = true;
        if (includePattern != null) {
          matches &= includePattern.matcher(entry.getKey()).matches();
        }
        if (excludePattern != null) {
          matches &= !excludePattern.matcher(entry.getKey()).matches();
        }
        return matches;
      }).collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
    }
    return parameters;
  }

  public static Table convertToTable(MTable mtbl, Configuration conf) throws MetaException {
    if (mtbl == null) {
      return null;
    }
    String tableType = mtbl.getTableType();
    String viewOriginalText = null;
    String viewExpandedText = null;
    if (tableType == null) {
      // for backwards compatibility with old metastore persistence
      if (mtbl.getViewOriginalText() != null) {
        tableType = TableType.VIRTUAL_VIEW.toString();
      } else if (mtbl.getParameters() != null && Boolean.parseBoolean(mtbl.getParameters().get("EXTERNAL"))) {
        tableType = TableType.EXTERNAL_TABLE.toString();
      } else {
        tableType = TableType.MANAGED_TABLE.toString();
      }
    } else {
      if (tableType.equals(TableType.VIRTUAL_VIEW.toString()) || tableType.equals(TableType.MATERIALIZED_VIEW.toString())) {
        viewOriginalText = mtbl.getViewOriginalText();
        viewExpandedText = mtbl.getViewExpandedText();
      }
    }
    Map<String, String> parameters = convertMap(mtbl.getParameters(), conf);
    boolean isAcidTable = TxnUtils.isAcidTable(parameters);
    final Table t = new Table(mtbl.getTableName(), mtbl.getDatabase() != null ? mtbl.getDatabase().getName() : null,
        mtbl.getOwner(), mtbl.getCreateTime(), mtbl.getLastAccessTime(), mtbl.getRetention(),
        convertToStorageDescriptor(mtbl.getSd(), false, isAcidTable, conf),
        convertToFieldSchemas(mtbl.getPartitionKeys()), parameters, viewOriginalText,
        viewExpandedText, tableType);

    if (Strings.isNullOrEmpty(mtbl.getOwnerType())) {
      // Before the ownerType exists in an old Hive schema, USER was the default type for owner.
      // Let's set the default to USER to keep backward compatibility.
      t.setOwnerType(PrincipalType.USER);
    } else {
      t.setOwnerType(PrincipalType.valueOf(mtbl.getOwnerType()));
    }

    t.setId(mtbl.getId());
    t.setRewriteEnabled(mtbl.isRewriteEnabled());
    t.setCatName(mtbl.getDatabase() != null ? mtbl.getDatabase().getCatalogName() : null);
    t.setWriteId(mtbl.getWriteId());
    return t;
  }

  public static MTable convertToMTable(Table tbl, RawStore base) throws InvalidObjectException,
      MetaException {
    // NOTE: we don't set writeId in this method. Write ID is only set after validating the
    //       existing write ID against the caller's valid list.
    if (tbl == null) {
      return null;
    }
    MDatabase mdb = null;
    String catName = tbl.isSetCatName() ? tbl.getCatName() : getDefaultCatalog(base.getConf());
    try {
      mdb = base.ensureGetMDatabase(catName, tbl.getDbName());
    } catch (NoSuchObjectException e) {
      LOG.error("Could not convert to MTable", e);
      throw new InvalidObjectException("Database " +
          DatabaseName.getQualified(catName, tbl.getDbName()) + " doesn't exist.");
    }

    // If the table has property EXTERNAL set, update table type
    // accordingly
    String tableType = tbl.getTableType();
    boolean isExternal = Boolean.parseBoolean(tbl.getParameters().get("EXTERNAL"));
    if (TableType.MANAGED_TABLE.toString().equals(tableType)) {
      if (isExternal) {
        tableType = TableType.EXTERNAL_TABLE.toString();
      }
    }
    if (TableType.EXTERNAL_TABLE.toString().equals(tableType)) {
      if (!isExternal) {
        tableType = TableType.MANAGED_TABLE.toString();
      }
    }

    PrincipalType ownerPrincipalType = tbl.getOwnerType();
    String ownerType = (ownerPrincipalType == null) ? PrincipalType.USER.name() : ownerPrincipalType.name();

    // A new table is always created with a new column descriptor
    return new MTable(normalizeIdentifier(tbl.getTableName()), mdb,
        convertToMStorageDescriptor(tbl.getSd()), tbl.getOwner(), ownerType, tbl
        .getCreateTime(), tbl.getLastAccessTime(), tbl.getRetention(),
        convertToMFieldSchemas(tbl.getPartitionKeys()), tbl.getParameters(),
        tbl.getViewOriginalText(), tbl.getViewExpandedText(), tbl.isRewriteEnabled(),
        tableType);
  }

  private static List<MFieldSchema> convertToMFieldSchemas(List<FieldSchema> keys) {
    List<MFieldSchema> mkeys = null;
    if (keys != null) {
      mkeys = new ArrayList<>(keys.size());
      for (FieldSchema part : keys) {
        mkeys.add(new MFieldSchema(part.getName().toLowerCase(),
            part.getType(), part.getComment()));
      }
    }
    return mkeys;
  }

  public static List<FieldSchema> convertToFieldSchemas(List<MFieldSchema> mkeys) {
    List<FieldSchema> keys = null;
    if (mkeys != null) {
      keys = new ArrayList<>();
      for (MFieldSchema part : mkeys) {
        keys.add(new FieldSchema(part.getName(), part.getType(), part
            .getComment()));
      }
    }
    return keys;
  }

  private static List<MOrder> convertToMOrders(List<Order> keys) {
    List<MOrder> mkeys = null;
    if (keys != null) {
      mkeys = new ArrayList<>();
      for (Order part : keys) {
        mkeys.add(new MOrder(normalizeIdentifier(part.getCol()), part.getOrder()));
      }
    }
    return mkeys;
  }

  private static List<Order> convertToOrders(List<MOrder> mkeys) {
    List<Order> keys = null;
    if (mkeys != null) {
      keys = new ArrayList<>();
      for (MOrder part : mkeys) {
        keys.add(new Order(part.getCol(), part.getOrder()));
      }
    }
    return keys;
  }

  private static SerDeInfo convertToSerDeInfo(MSerDeInfo ms, Configuration conf, boolean allowNull)
      throws MetaException {
    if (ms == null) {
      if (allowNull) {
        return null;
      }
      throw new MetaException("Invalid SerDeInfo object");
    }
    SerDeInfo serde =
        new SerDeInfo(ms.getName(), ms.getSerializationLib(), convertMap(ms.getParameters(), conf));
    if (ms.getDescription() != null) {
      serde.setDescription(ms.getDescription());
    }
    if (ms.getSerializerClass() != null) {
      serde.setSerializerClass(ms.getSerializerClass());
    }
    if (ms.getDeserializerClass() != null) {
      serde.setDeserializerClass(ms.getDeserializerClass());
    }
    if (ms.getSerdeType() > 0) {
      serde.setSerdeType(SerdeType.findByValue(ms.getSerdeType()));
    }
    return serde;
  }

  private static MSerDeInfo convertToMSerDeInfo(SerDeInfo ms) throws MetaException {
    if (ms == null) {
      throw new MetaException("Invalid SerDeInfo object");
    }
    return new MSerDeInfo(ms.getName(), ms.getSerializationLib(), ms.getParameters(),
        ms.getDescription(), ms.getSerializerClass(), ms.getDeserializerClass(),
        ms.getSerdeType() == null ? 0 : ms.getSerdeType().getValue());
  }

  /**
   * Given a list of model field schemas, create a new model column descriptor.
   * @param cols the columns the column descriptor contains
   * @return a new column descriptor db-backed object
   */
  private static MColumnDescriptor createNewMColumnDescriptor(List<MFieldSchema> cols) {
    if (cols == null) {
      return null;
    }
    return new MColumnDescriptor(cols);
  }

  private static StorageDescriptor convertToStorageDescriptor(
      MStorageDescriptor msd, boolean noFS, boolean isAcidTable, Configuration conf) throws MetaException {
    if (msd == null) {
      return null;
    }
    List<MFieldSchema> mFieldSchemas;
    if (noFS) {
      mFieldSchemas = Collections.emptyList();
    } else {
      mFieldSchemas = msd.getCD() == null ? null : msd.getCD().getCols();
    }
    List<Order> orderList = (isAcidTable) ? Collections.emptyList() : convertToOrders(msd.getSortCols());
    List<String> bucList = convertList(msd.getBucketCols());
    SkewedInfo skewedInfo = null;

    Map<String, String> sdParams = isAcidTable ? Collections.emptyMap() : convertMap(msd.getParameters(), conf);
    StorageDescriptor sd = new StorageDescriptor(convertToFieldSchemas(mFieldSchemas),
        msd.getLocation(), msd.getInputFormat(), msd.getOutputFormat(), msd
        .isCompressed(), msd.getNumBuckets(),
        (!isAcidTable) ? convertToSerDeInfo(msd.getSerDeInfo(), conf, true)
            : new SerDeInfo(msd.getSerDeInfo().getName(), msd.getSerDeInfo().getSerializationLib(), Collections.emptyMap()),
        bucList , orderList, sdParams);
    if (!isAcidTable) {
      skewedInfo = new SkewedInfo(convertList(msd.getSkewedColNames()),
          convertToSkewedValues(msd.getSkewedColValues()),
          covertToSkewedMap(msd.getSkewedColValueLocationMaps()));
    } else {
      skewedInfo = new SkewedInfo(Collections.emptyList(), Collections.emptyList(),
          Collections.emptyMap());
    }
    sd.setSkewedInfo(skewedInfo);
    sd.setStoredAsSubDirectories(msd.isStoredAsSubDirectories());
    return sd;
  }

  /**
   * Convert a list of MStringList to a list of list string
   */
  private static List<List<String>> convertToSkewedValues(List<MStringList> mLists) {
    List<List<String>> lists = null;
    if (mLists != null) {
      lists = new ArrayList<>();
      for (MStringList element : mLists) {
        lists.add(new ArrayList<>(element.getInternalList()));
      }
    }
    return lists;
  }

  private static List<MStringList> convertToMStringLists(List<List<String>> mLists) {
    List<MStringList> lists = null ;
    if (null != mLists) {
      lists = new ArrayList<>();
      for (List<String> mList : mLists) {
        lists.add(new MStringList(mList));
      }
    }
    return lists;
  }

  /**
   * Convert a MStringList Map to a Map
   */
  private static Map<List<String>, String> covertToSkewedMap(Map<MStringList, String> mMap) {
    Map<List<String>, String> map = null;
    if (mMap != null) {
      map = new HashMap<>();
      Set<MStringList> keys = mMap.keySet();
      for (MStringList key : keys) {
        map.put(new ArrayList<>(key.getInternalList()), mMap.get(key));
      }
    }
    return map;
  }

  /**
   * Covert a Map to a MStringList Map
   */
  private static Map<MStringList, String> covertToMapMStringList(Map<List<String>, String> mMap) {
    Map<MStringList, String> map = null;
    if (mMap != null) {
      map = new HashMap<>();
      Set<List<String>> keys = mMap.keySet();
      for (List<String> key : keys) {
        map.put(new MStringList(key), mMap.get(key));
      }
    }
    return map;
  }

  /**
   * Converts a storage descriptor to a db-backed storage descriptor.  Creates a
   *   new db-backed column descriptor object for this SD.
   * @param sd the storage descriptor to wrap in a db-backed object
   * @return the storage descriptor db-backed object
   */
  private static MStorageDescriptor convertToMStorageDescriptor(StorageDescriptor sd)
      throws MetaException {
    if (sd == null) {
      return null;
    }
    MColumnDescriptor mcd = createNewMColumnDescriptor(convertToMFieldSchemas(sd.getCols()));
    return convertToMStorageDescriptor(sd, mcd);
  }

  /**
   * Converts a storage descriptor to a db-backed storage descriptor.  It points the
   * storage descriptor's column descriptor to the one passed as an argument,
   * so it does not create a new mcolumn descriptor object.
   * @param sd the storage descriptor to wrap in a db-backed object
   * @param mcd the db-backed column descriptor
   * @return the db-backed storage descriptor object
   */
  private static MStorageDescriptor convertToMStorageDescriptor(StorageDescriptor sd,
      MColumnDescriptor mcd) throws MetaException {
    if (sd == null) {
      return null;
    }
    return new MStorageDescriptor(mcd, sd
        .getLocation(), sd.getInputFormat(), sd.getOutputFormat(), sd
        .isCompressed(), sd.getNumBuckets(), convertToMSerDeInfo(sd
        .getSerdeInfo()), sd.getBucketCols(),
        convertToMOrders(sd.getSortCols()), sd.getParameters(),
        (null == sd.getSkewedInfo()) ? null
            : sd.getSkewedInfo().getSkewedColNames(),
        convertToMStringLists((null == sd.getSkewedInfo()) ? null : sd.getSkewedInfo()
            .getSkewedColValues()),
        covertToMapMStringList((null == sd.getSkewedInfo()) ? null : sd.getSkewedInfo()
            .getSkewedColValueLocationMaps()), sd.isStoredAsSubDirectories());
  }

  public static MCreationMetadata convertToMCreationMetadata(CreationMetadata m, RawStore base)
      throws MetaException {
    if (m == null) {
      return null;
    }
    assert !m.isSetMaterializationTime();
    try {
      Set<MMVSource> tablesUsed = new HashSet<>();
      if (m.isSetSourceTables()) {
        for (SourceTable sourceTable : m.getSourceTables()) {
          tablesUsed.add(convertToSourceTable(m.getCatName(), sourceTable, base));
        }
      } else {
        for (String fullyQualifiedName : m.getTablesUsed()) {
          tablesUsed.add(convertToSourceTable(m.getCatName(), fullyQualifiedName, base));
        }
      }
      return new MCreationMetadata(normalizeIdentifier(m.getCatName()), normalizeIdentifier(m.getDbName()),
          normalizeIdentifier(m.getTblName()), tablesUsed, m.getValidTxnList(), System.currentTimeMillis());
    } catch (NoSuchObjectException nse) {
      throw new MetaException(nse.getMessage());
    }
  }

  public static MMVSource convertToSourceTable(String catalog, SourceTable sourceTable, RawStore base)
      throws NoSuchObjectException {
    Table table = sourceTable.getTable();
    MTable mtbl = base.ensureGetMTable(catalog, table.getDbName(), table.getTableName());
    MMVSource source = new MMVSource();
    source.setTable(mtbl);
    source.setInsertedCount(sourceTable.getInsertedCount());
    source.setUpdatedCount(sourceTable.getUpdatedCount());
    source.setDeletedCount(sourceTable.getDeletedCount());
    return source;
  }

  /**
   * This method resets the stats to 0 and supports only backward compatibility with clients does not
   * send {@link SourceTable} instances.
   *
   * Use {@link ObjectStore#convertToSourceTable(String, SourceTable, RawStore)} instead.
   *
   * @param catalog Catalog name where source table is located
   * @param fullyQualifiedTableName fully qualified name of source table
   * @return {@link MMVSource} instance represents this source table.
   */
  @Deprecated
  private static MMVSource convertToSourceTable(String catalog, String fullyQualifiedTableName, RawStore base)
      throws NoSuchObjectException {
    String[] names = fullyQualifiedTableName.split("\\.");
    MTable mtbl = base.ensureGetMTable(catalog, names[0], names[1]);
    MMVSource source = new MMVSource();
    source.setTable(mtbl);
    source.setInsertedCount(0L);
    source.setUpdatedCount(0L);
    source.setDeletedCount(0L);
    return source;
  }

  public static CreationMetadata convertToCreationMetadata(MCreationMetadata s, RawStore base)
      throws MetaException {
    if (s == null) {
      return null;
    }
    try {
      Set<String> tablesUsed = new HashSet<>();
      List<SourceTable> sourceTables = new ArrayList<>(s.getTables().size());
      for (MMVSource mtbl : s.getTables()) {
        tablesUsed.add(
            Warehouse.getQualifiedName(mtbl.getTable().getDatabase().getName(), mtbl.getTable().getTableName()));
        sourceTables.add(convertToSourceTable(mtbl, s.getCatalogName(), base));
      }
      CreationMetadata r = new CreationMetadata(s.getCatalogName(), s.getDbName(), s.getTblName(), tablesUsed);
      r.setMaterializationTime(s.getMaterializationTime());
      if (s.getTxnList() != null) {
        r.setValidTxnList(s.getTxnList());
      }
      r.setSourceTables(sourceTables);
      return r;
    } catch (NoSuchObjectException nse) {
      throw new MetaException(nse.getMessage());
    }
  }

  private static SourceTable convertToSourceTable(MMVSource mmvSource, String catalogName, RawStore base)
      throws MetaException, NoSuchObjectException {
    SourceTable sourceTable = new SourceTable();
    MTable mTable = mmvSource.getTable();
    Table table =
        convertToTable(base.ensureGetMTable(catalogName, mTable.getDatabase().getName(), mTable.getTableName()),
            base.getConf());
    sourceTable.setTable(table);
    sourceTable.setInsertedCount(mmvSource.getInsertedCount());
    sourceTable.setUpdatedCount(mmvSource.getUpdatedCount());
    sourceTable.setDeletedCount(mmvSource.getDeletedCount());
    return sourceTable;
  }

  /**
   * Convert a Partition object into an MPartition, which is an object backed by the db
   * If the Partition's set of columns is the same as the parent table's AND useTableCD
   * is true, then this partition's storage descriptor's column descriptor will point
   * to the same one as the table's storage descriptor.
   * @param part the partition to convert
   * @param mt the parent table object
   * @return the model partition object, and null if the input partition is null.
   */
  public static MPartition convertToMPart(Partition part, MTable mt)
      throws InvalidObjectException, MetaException {
    // NOTE: we don't set writeId in this method. Write ID is only set after validating the
    //       existing write ID against the caller's valid list.
    if (part == null) {
      return null;
    }
    if (mt == null) {
      throw new InvalidObjectException(
          "Partition doesn't have a valid table or database name");
    }

    // If this partition's set of columns is the same as the parent table's,
    // use the parent table's, so we do not create a duplicate column descriptor,
    // thereby saving space
    MStorageDescriptor msd;
    if (mt.getSd() != null && mt.getSd().getCD() != null &&
        mt.getSd().getCD().getCols() != null &&
        part.getSd() != null &&
        convertToFieldSchemas(mt.getSd().getCD().getCols()).
        equals(part.getSd().getCols())) {
      msd = convertToMStorageDescriptor(part.getSd(), mt.getSd().getCD());
    } else {
      msd = convertToMStorageDescriptor(part.getSd());
    }

    return new MPartition(Warehouse.makePartName(convertToFieldSchemas(mt
        .getPartitionKeys()), part.getValues()), mt, part.getValues(), part
        .getCreateTime(), part.getLastAccessTime(),
        msd, part.getParameters());
  }

  public static Partition convertToPart(String catName, String dbName, String tblName,
      MPartition mpart, boolean isAcidTable, Configuration conf, GetPartitionsArgs... args)
      throws MetaException {
    if (mpart == null) {
      return null;
    }
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    Map<String,String> params = convertMap(mpart.getParameters(), conf, args);
    boolean noFS = args != null && args.length == 1 && args[0].isSkipColumnSchemaForPartition();
    Partition p = new Partition(convertList(mpart.getValues()), dbName, tblName,
        mpart.getCreateTime(), mpart.getLastAccessTime(),
        convertToStorageDescriptor(mpart.getSd(), noFS, isAcidTable, conf), params);
    p.setCatName(catName);
    if(mpart.getWriteId()>0) {
      p.setWriteId(mpart.getWriteId());
    }else {
      p.setWriteId(-1L);
    }
    return p;
  }

  public static List<Partition> convertToParts(String catName, String dbName, String tblName,
      List<MPartition> mparts, boolean isAcidTable, Configuration conf, GetPartitionsArgs args)
      throws MetaException {
    List<Partition> parts = new ArrayList<>(mparts.size());
    for (MPartition mp : mparts) {
      parts.add(convertToPart(catName, dbName, tblName, mp, isAcidTable, conf, args));
      Deadline.checkTimeout();
    }
    return parts;
  }

  public static Pair<Query, Map<String, String>> getPartQueryWithParams(
      PersistenceManager pm,
      String catName, String dbName, String tblName,
      List<String> partNames) {
    Query query = pm.newQuery();
    Map<String, String> params = new HashMap<>();
    String filterStr = getJDOFilterStrForPartitionNames(catName, dbName, tblName, partNames, params);
    query.setFilter(filterStr);
    LOG.debug(" JDOQL filter is {}", filterStr);
    query.declareParameters(makeParameterDeclarationString(params));
    return Pair.of(query, params);
  }

  public static String getJDOFilterStrForPartitionNames(String catName, String dbName, String tblName,
      List<String> partNames, Map params) {
    StringBuilder sb = new StringBuilder(
        "table.tableName == t1 && table.database.name == t2 &&" + " table.database.catalogName == t3 && (");
    params.put("t1", normalizeIdentifier(tblName));
    params.put("t2", normalizeIdentifier(dbName));
    params.put("t3", normalizeIdentifier(catName));
    int n = 0;
    for (Iterator<String> itr = partNames.iterator(); itr.hasNext(); ) {
      String pn = "p" + n;
      n++;
      String part = itr.next();
      params.put(pn, part);
      sb.append("partitionName == ").append(pn);
      sb.append(" || ");
    }
    sb.setLength(sb.length() - 4); // remove the last " || "
    sb.append(')');
    return sb.toString();
  }

  public static String makeParameterDeclarationString(Map<String, String> params) {
    //Create the parameter declaration string
    StringBuilder paramDecl = new StringBuilder();
    for (String key : params.keySet()) {
      paramDecl.append(", java.lang.String ").append(key);
    }
    return paramDecl.toString();
  }

  /** Helper class for getting stuff w/transaction, direct SQL, perf logging, etc. */
  @VisibleForTesting
  public abstract class GetHelper<T> {
    private final boolean isInTxn, doTrace, allowJdo;
    private boolean doUseDirectSql;
    private long start;
    private Table table;
    protected final List<String> partitionFields;
    protected final String catName, dbName, tblName;
    private boolean success = false;
    protected T results = null;

    public GetHelper(String catalogName, String dbName, String tblName,
        boolean allowSql, boolean allowJdo) throws MetaException {
      this(catalogName, dbName, tblName, null, allowSql, allowJdo);
    }

    public GetHelper(String catalogName, String dbName, String tblName,
        List<String> fields, boolean allowSql, boolean allowJdo) throws MetaException {
      assert allowSql || allowJdo;
      this.allowJdo = allowJdo;
      this.catName = (catalogName != null) ? normalizeIdentifier(catalogName) : null;
      this.dbName = (dbName != null) ? normalizeIdentifier(dbName) : null;
      this.partitionFields = fields;
      if (tblName != null) {
        this.tblName = normalizeIdentifier(tblName);
      } else {
        // tblName can be null in cases of Helper being used at a higher
        // abstraction level, such as with datbases
        this.tblName = null;
        this.table = null;
      }
      this.doTrace = LOG.isDebugEnabled();
      this.isInTxn = isActiveTransaction();

      boolean isConfigEnabled = MetastoreConf.getBoolVar(getConf(), ConfVars.TRY_DIRECT_SQL);
      if (isConfigEnabled && directSql == null) {
        directSql = new MetaStoreDirectSql(pm, getConf(), "");
      }

      if (!allowJdo && isConfigEnabled && !directSql.isCompatibleDatastore()) {
        throw new MetaException("SQL is not operational"); // test path; SQL is enabled and broken.
      }
      this.doUseDirectSql = allowSql && isConfigEnabled && directSql.isCompatibleDatastore();
    }

    protected boolean canUseDirectSql(GetHelper<T> ctx) throws MetaException {
      return true; // By default, assume we can user directSQL - that's kind of the point.
    }
    protected abstract String describeResult();
    protected abstract T getSqlResult(GetHelper<T> ctx) throws MetaException;
    protected abstract T getJdoResult(
        GetHelper<T> ctx) throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

    public T run(boolean initTable) throws MetaException, NoSuchObjectException {
      try {
        start(initTable);
        String savePoint = isInTxn && allowJdo ? "rollback_" + System.nanoTime() : null;
        if (doUseDirectSql) {
          try {
            directSql.prepareTxn();
            setTransactionSavePoint(savePoint);
            this.results = getSqlResult(this);
            LOG.debug("Using direct SQL optimization.");
          } catch (Exception ex) {
            handleDirectSqlError(ex, savePoint);
          }
        }
        // Note that this will be invoked in 2 cases:
        //    1) DirectSQL was disabled to start with;
        //    2) DirectSQL threw and was disabled in handleDirectSqlError.
        if (!doUseDirectSql) {
          this.results = getJdoResult(this);
          LOG.debug("Not using direct SQL optimization.");
        }
        return commit();
      } catch (NoSuchObjectException | MetaException ex) {
        throw ex;
      } catch (Exception ex) {
        LOG.error("", ex);
        throw new MetaException(ex.getMessage());
      } finally {
        close();
      }
    }

    private void start(boolean initTable) throws MetaException, NoSuchObjectException {
      start = doTrace ? System.nanoTime() : 0;
      openTransaction();
      if (initTable && (tblName != null)) {
        table = ensureGetTable(catName, dbName, tblName);
      }
      doUseDirectSql = doUseDirectSql && canUseDirectSql(this);
    }

    private void handleDirectSqlError(Exception ex, String savePoint) throws MetaException, NoSuchObjectException {
      String message = null;
      try {
        message = generateShorterMessage(ex);
      } catch (Throwable t) {
        message = ex.toString() + "; error building a better message: " + t.getMessage();
      }
      LOG.warn(message); // Don't log the exception, people just get confused.
      LOG.debug("Full DirectSQL callstack for debugging (not an error)", ex);

      if (!allowJdo || !DatabaseProduct.isRecoverableException(ex)) {
        throw ExceptionHandler.newMetaException(ex);
      }
      
      if (!isInTxn) {
        JDOException rollbackEx = null;
        try {
          rollbackTransaction();
        } catch (JDOException jex) {
          rollbackEx = jex;
        }
        if (rollbackEx != null) {
          // Datanucleus propagates some pointless exceptions and rolls back in the finally.
          if (currentTransaction != null && currentTransaction.isActive()) {
            throw rollbackEx; // Throw if the tx wasn't rolled back.
          }
          LOG.info("Ignoring exception, rollback succeeded: " + rollbackEx.getMessage());
        }

        start = doTrace ? System.nanoTime() : 0;
        openTransaction();
        if (table != null) {
          table = ensureGetTable(catName, dbName, tblName);
        }
      } else {
        rollbackTransactionToSavePoint(savePoint);
        start = doTrace ? System.nanoTime() : 0;
      }

      if (directSqlErrors != null) {
        directSqlErrors.inc();
      }

      doUseDirectSql = false;
    }

    private String generateShorterMessage(Exception ex) {
      StringBuilder message = new StringBuilder(
          "Falling back to ORM path due to direct SQL failure (this is not an error): ");
      Throwable t = ex;
      StackTraceElement[] prevStack = null;
      while (t != null) {
        message.append(t.getMessage());
        StackTraceElement[] stack = t.getStackTrace();
        int uniqueFrames = stack.length - 1;
        if (prevStack != null) {
          int n = prevStack.length - 1;
          while (uniqueFrames >= 0 && n >= 0 && stack[uniqueFrames].equals(prevStack[n])) {
            uniqueFrames--; n--;
          }
        }
        for (int i = 0; i <= uniqueFrames; ++i) {
          StackTraceElement ste = stack[i];
          message.append(" at ").append(ste);
          if (ste.getMethodName().contains("getSqlResult")
              && (ste.getFileName() == null || ste.getFileName().contains("ObjectStore"))) {
            break;
          }
        }
        prevStack = stack;
        t = t.getCause();
        if (t != null) {
          message.append(";\n Caused by: ");
        }
      }
      return message.toString();
    }

    private T commit() {
      success = commitTransaction();
      if (doTrace) {
        double time = ((System.nanoTime() - start) / 1000000.0);
        String result = describeResult();
        String retrieveType = doUseDirectSql ? "SQL" : "ORM";

        LOG.debug("{} retrieved using {} in {}ms", result, retrieveType, time);
      }
      return results;
    }

    private void close() {
      if (!success) {
        rollbackTransaction();
      }
    }

    public Table getTable() {
      return table;
    }
  }

  private abstract class GetListHelper<T> extends GetHelper<List<T>> {
    public GetListHelper(String catName, String dbName, String tblName, boolean allowSql,
                         boolean allowJdo) throws MetaException {
      super(catName, dbName, tblName, null, allowSql, allowJdo);
    }

    public GetListHelper(String catName, String dbName, String tblName, List<String> fields,
        boolean allowSql, boolean allowJdo) throws MetaException {
      super(catName, dbName, tblName, fields, allowSql, allowJdo);
    }

    @Override
    protected String describeResult() {
      return results.size() + " entries";
    }
  }

  @VisibleForTesting
  public abstract class GetDbHelper extends GetHelper<Database> {
    /**
     * GetHelper for returning db info using directSql/JDO.
     * @param dbName The Database Name
     * @param allowSql Whether or not we allow DirectSQL to perform this query.
     * @param allowJdo Whether or not we allow ORM to perform this query.
     */
    public GetDbHelper(String catalogName, String dbName,boolean allowSql, boolean allowJdo)
        throws MetaException {
      super(catalogName, dbName,null,allowSql,allowJdo);
    }

    @Override
    protected String describeResult() {
      return "db details for db ".concat(dbName);
    }
  }

  private abstract class GetStatHelper extends GetHelper<ColumnStatistics> {
    public GetStatHelper(String catalogName, String dbName, String tblName, boolean allowSql,
                         boolean allowJdo, String writeIdList) throws MetaException {
      super(catalogName, dbName, tblName, allowSql, allowJdo);
    }

    @Override
    protected String describeResult() {
      return "statistics for " + (results == null ? 0 : results.getStatsObjSize()) + " columns";
    }
  }

  private Table ensureGetTable(String catName, String dbName, String tblName)
      throws NoSuchObjectException, MetaException {
    return convertToTable(ensureGetMTable(catName, dbName, tblName), conf);
  }

  @Override
  public MDatabase ensureGetMDatabase(String catName, String dbName)
      throws NoSuchObjectException {
    return getMDatabase(catName, dbName);
  }

  /**
   * Verifies that the stats JSON string is unchanged for alter table (txn stats).
   * @return Error message with the details of the change, or null if the value has not changed.
   */
  public static String verifyStatsChangeCtx(String fullTableName, Map<String, String> oldP, Map<String, String> newP,
                                            long writeId, String validWriteIds, boolean isColStatsChange) {
    if (validWriteIds != null && writeId > 0) {
      return null; // We have txn context.
    }

    if (!StatsSetupConst.areBasicStatsUptoDate(newP)) {
      // The validWriteIds can be absent, for example, in case of Impala alter.
      // If the new value is invalid, then we don't care, let the alter operation go ahead.
      return null;
    }

    String oldVal = oldP == null ? null : oldP.get(StatsSetupConst.COLUMN_STATS_ACCURATE);
    String newVal = newP == null ? null : newP.get(StatsSetupConst.COLUMN_STATS_ACCURATE);
    if (StringUtils.equalsIgnoreCase(oldVal, newVal)) {
      if (!isColStatsChange) {
        return null; // No change in col stats or parameters => assume no change.
      }
    }

    // Some change to the stats state is being made; it can only be made with a write ID.
    return "Cannot change stats state for a transactional table " + fullTableName + " without " +
            "providing the transactional write state for verification (new write ID " +
            writeId + ", valid write IDs " + validWriteIds + "; current state " + oldVal + "; new" +
            " state " + newVal;
  }

  private static MFieldSchema getColumnFromTableColumns(List<MFieldSchema> cols, String col) {
    if (cols == null) {
      return null;
    }
    for (MFieldSchema mfs : cols) {
      if (mfs.getName().equalsIgnoreCase(col)) {
        return mfs;
      }
    }
    return null;
  }

  private static int getColumnIndexFromTableColumns(List<MFieldSchema> cols, String col) {
    if (cols == null) {
      return -1;
    }
    for (int i = 0; i < cols.size(); i++) {
      MFieldSchema mfs = cols.get(i);
      if (mfs.getName().equalsIgnoreCase(col)) {
        return i;
      }
    }
    return -1;
  }

  private  boolean constraintNameAlreadyExists(MTable table, String constraintName) {
    boolean commited = false;
    Query<MConstraint> constraintExistsQuery = null;
    String constraintNameIfExists = null;
    try {
      openTransaction();
      constraintName = normalizeIdentifier(constraintName);
      constraintExistsQuery = pm.newQuery(MConstraint.class,
          "parentTable == parentTableP && constraintName == constraintNameP");
      constraintExistsQuery.declareParameters("MTable parentTableP, java.lang.String constraintNameP");
      constraintExistsQuery.setUnique(true);
      constraintExistsQuery.setResult("constraintName");
      constraintNameIfExists = (String) constraintExistsQuery.executeWithArray(table, constraintName);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, constraintExistsQuery);
    }
    return constraintNameIfExists != null && !constraintNameIfExists.isEmpty();
  }

  private String generateConstraintName(MTable table, String... parameters) throws MetaException {
    int hashcode = ArrayUtils.toString(parameters).hashCode() & 0xfffffff;
    int counter = 0;
    final int MAX_RETRIES = 10;
    while (counter < MAX_RETRIES) {
      String currName = (parameters.length == 0 ? "constraint_" : parameters[parameters.length-1]) +
        "_" + hashcode + "_" + System.currentTimeMillis() + "_" + (counter++);
      if (!constraintNameAlreadyExists(table, currName)) {
        return currName;
      }
    }
    throw new MetaException("Error while trying to generate the constraint name for " + ArrayUtils.toString(parameters));
  }

  @Override
  public List<SQLForeignKey> addForeignKeys(
    List<SQLForeignKey> fks) throws InvalidObjectException, MetaException {
   return addForeignKeys(fks, true, null, null);
  }

  @Override
  public String getMetastoreDbUuid() throws MetaException {
    String ret = getGuidFromDB();
    if(ret != null) {
      return ret;
    }
    return createDbGuidAndPersist();
  }

  private String createDbGuidAndPersist() throws MetaException {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      MMetastoreDBProperties prop = new MMetastoreDBProperties();
      prop.setPropertykey("guid");
      final String guid = UUID.randomUUID().toString();
      LOG.debug("Attempting to add a guid {} for the metastore db", guid);
      prop.setPropertyValue(guid);
      prop.setDescription("Metastore DB GUID generated on "
          + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
      pm.makePersistent(prop);
      success = commitTransaction();
      if (success) {
        LOG.info("Metastore db guid {} created successfully", guid);
        return guid;
      }
    } catch (Exception e) {
      LOG.warn("Metastore db guid creation failed", e);
    } finally {
      rollbackAndCleanup(success, query);
    }
    // it possible that some other HMS instance could have created the guid
    // at the same time due which this instance could not create a guid above
    // in such case return the guid already generated
    final String guid = getGuidFromDB();
    if (guid == null) {
      throw new MetaException("Unable to create or fetch the metastore database uuid");
    }
    return guid;
  }

  private String getGuidFromDB() throws MetaException {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MMetastoreDBProperties.class, PTYARG_EQ_KEY);
      query.declareParameters(PTYPARAM_STR_KEY);
      Collection<MMetastoreDBProperties> names = (Collection<MMetastoreDBProperties>) query.execute("guid");
      List<String> uuids = new ArrayList<>();
      for (Iterator<MMetastoreDBProperties> i = names.iterator(); i.hasNext();) {
        String uuid = i.next().getPropertyValue();
        LOG.debug("Found guid {}", uuid);
        uuids.add(uuid);
      }
      success = commitTransaction();
      if(uuids.size() > 1) {
        throw new MetaException("Multiple uuids found");
      }
      if(!uuids.isEmpty()) {
        LOG.debug("Returning guid of metastore db : {}", uuids.get(0));
        return uuids.get(0);
      }
    } finally {
      rollbackAndCleanup(success, query);
    }
    LOG.warn("Guid for metastore db not found");
    return null;
  }

  public boolean runInTransaction(Runnable exec) {
    boolean success = false;
    try {
      if (openTransaction()) {
        exec.run();
        success = commitTransaction();
      }
    } catch (Exception e) {
      LOG.warn("Metastore operation failed", e);
    } finally {
      rollbackAndCleanup(success, null);
    }
    return success;
  }

  public boolean dropProperties(String key) {
    boolean success = false;
    Query<MMetastoreDBProperties> query = null;
    try {
      if (openTransaction()) {
        query = pm.newQuery(MMetastoreDBProperties.class, PTYARG_EQ_KEY);
        query.declareParameters(PTYPARAM_STR_KEY);
        @SuppressWarnings("unchecked")
        Collection<MMetastoreDBProperties> properties = (Collection<MMetastoreDBProperties>) query.execute(key);
        if (!properties.isEmpty()) {
          pm.deletePersistentAll(properties);
        }
        success = commitTransaction();
      }
    } catch (Exception e) {
      LOG.warn("Metastore property drop failed", e);
    } finally {
      rollbackAndCleanup(success, query);
    }
    return success;
  }


  public MMetastoreDBProperties putProperties(String key, String value, String description,  byte[] content) {
    boolean success = false;
    try {
      if (openTransaction()) {
        //pm.currentTransaction().setOptimistic(false);
        // fetch first to determine new vs update
        MMetastoreDBProperties properties = doFetchProperties(key, null);
        final boolean newInstance;
        if (properties == null) {
          newInstance = true;
          properties = new MMetastoreDBProperties();
          properties.setPropertykey(key);
        } else {
          newInstance = false;
        }
        properties.setDescription(description);
        properties.setPropertyValue(value);
        properties.setPropertyContent(content);
        LOG.debug("Attempting to add property {} for the metastore db", key);
        properties.setDescription("Metastore property "
            + (newInstance ? "created" : "updated")
            + " " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        if (newInstance) {
          pm.makePersistent(properties);
        }
        success = commitTransaction();
        if (success) {
          LOG.info("Metastore property {} created successfully", key);
          return properties;
        }
      }
    } finally {
      rollbackAndCleanup(success, null);
    }
    return null;
  }


  public boolean renameProperties(String mapKey, String newKey) {
    boolean success = false;
    Query<MMetastoreDBProperties> query = null;
    try {
      LOG.debug("Attempting to rename property {} to {} for the metastore db", mapKey, newKey);
      if (openTransaction()) {
        // ensure the target is clear;
        // query is cleaned up in finally block
        query = pm.newQuery(MMetastoreDBProperties.class, PTYARG_EQ_KEY);
        query.declareParameters(PTYPARAM_STR_KEY);
        query.setUnique(true);
        MMetastoreDBProperties properties = (MMetastoreDBProperties) query.execute(newKey);
        if (properties != null) {
          return false;
        }
        // ensure we got a source
        properties = (MMetastoreDBProperties) query.execute(mapKey);
        if (properties == null) {
          return false;
        }
        byte[] content = properties.getPropertyContent();
        String value = properties.getPropertyValue();
        // remove source from persistent storage
        pm.deletePersistent(properties);
        // make it persist with new key
        MMetastoreDBProperties newProperties = new MMetastoreDBProperties();
        // update description
        newProperties.setDescription("Metastore property renamed from " + mapKey + " to " + newKey
            + " " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        // change key
        newProperties.setPropertykey(newKey);
        newProperties.setPropertyValue(value);
        newProperties.setPropertyContent(content);
        pm.makePersistent(newProperties);
        // commit
        success = commitTransaction();
        if (success) {
          LOG.info("Metastore property {} renamed {} successfully", mapKey, newKey);
          return true;
        }
      }
    } finally {
      rollbackAndCleanup(success, query);
    }
    return false;
  }

  private <T> T doFetchProperties(String key, java.util.function.Function<MMetastoreDBProperties, T> transform) {
    try(QueryWrapper query = new QueryWrapper(pm.newQuery(MMetastoreDBProperties.class, PTYARG_EQ_KEY))) {
      query.declareParameters(PTYPARAM_STR_KEY);
      query.setUnique(true);
      MMetastoreDBProperties properties = (MMetastoreDBProperties) query.execute(key);
      if (properties != null) {
        return (T) (transform != null? transform.apply(properties) : properties);
      }
    }
    return null;
  }

  public <T> T fetchProperties(String key, java.util.function.Function<MMetastoreDBProperties, T> transform) {
    boolean success = false;
    T properties = null;
    try {
      if (openTransaction()) {
        properties = doFetchProperties(key, transform);
        success = commitTransaction();
      }
    } finally {
      rollbackAndCleanup(success, null);
    }
    return properties;
  }

  public <T> Map<String, T> selectProperties(String key, java.util.function.Function<MMetastoreDBProperties, T> transform) {
    boolean success = false;
    Query<MMetastoreDBProperties> query = null;
    Map<String, T> results = null;
    try {
      if (openTransaction()) {
        Collection<MMetastoreDBProperties> properties;
        if (key == null || key.isEmpty()) {
          query = pm.newQuery(MMetastoreDBProperties.class);
          properties = (Collection<MMetastoreDBProperties>) query.execute();
        } else {
          query = pm.newQuery(MMetastoreDBProperties.class, "this.propertyKey.startsWith(key)");
          query.declareParameters(PTYPARAM_STR_KEY);
          properties = (Collection<MMetastoreDBProperties>) query.execute(key);
        }
        pm.retrieveAll(properties);
        if (!properties.isEmpty()) {
          results = new TreeMap<String, T>();
          for(MMetastoreDBProperties ptys : properties) {
            T t = (T) (transform != null? transform.apply(ptys) : ptys);
            if (t != null) {
              results.put(ptys.getPropertykey(), t);
            }
          }
        }
        success = commitTransaction();
      }
    } finally {
      rollbackAndCleanup(success, query);
    }
    return results;
  }

  //TODO: clean up this method
  private List<SQLForeignKey> addForeignKeys(List<SQLForeignKey> foreignKeys, boolean retrieveCD,
      List<SQLPrimaryKey> primaryKeys, List<SQLUniqueConstraint> uniqueConstraints)
          throws InvalidObjectException, MetaException {
    if (CollectionUtils.isNotEmpty(foreignKeys)) {
      List<MConstraint> mpkfks = new ArrayList<>();
      String currentConstraintName = null;
      String catName = null;
      // We start iterating through the foreign keys. This list might contain more than a single
      // foreign key, and each foreign key might contain multiple columns. The outer loop retrieves
      // the information that is common for a single key (table information) while the inner loop
      // checks / adds information about each column.
      for (int i = 0; i < foreignKeys.size(); i++) {
        if (catName == null) {
          catName = normalizeIdentifier(foreignKeys.get(i).isSetCatName() ? foreignKeys.get(i).getCatName() :
              getDefaultCatalog(conf));
        } else {
          String tmpCatName = normalizeIdentifier(foreignKeys.get(i).isSetCatName() ?
              foreignKeys.get(i).getCatName() : getDefaultCatalog(conf));
          if (!catName.equals(tmpCatName)) {
            throw new InvalidObjectException("Foreign keys cannot span catalogs");
          }
        }
        final String fkTableDB = normalizeIdentifier(foreignKeys.get(i).getFktable_db());
        final String fkTableName = normalizeIdentifier(foreignKeys.get(i).getFktable_name());
        // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
        // For instance, this is the case when we are creating the table.
        final AttachedMTableInfo nChildTable = getMTable(catName, fkTableDB, fkTableName, retrieveCD);
        final MTable childTable = nChildTable.mtbl;
        if (childTable == null) {
          throw new InvalidObjectException("Child table not found: " + fkTableName);
        }
        MColumnDescriptor childCD = retrieveCD ? nChildTable.mcd : childTable.getSd().getCD();
        final List<MFieldSchema> childCols = childCD == null || childCD.getCols() == null ?
            new ArrayList<>() : new ArrayList<>(childCD.getCols());
        if (childTable.getPartitionKeys() != null) {
          childCols.addAll(childTable.getPartitionKeys());
        }

        final String pkTableDB = normalizeIdentifier(foreignKeys.get(i).getPktable_db());
        final String pkTableName = normalizeIdentifier(foreignKeys.get(i).getPktable_name());
        // For primary keys, we retrieve the column descriptors if retrieveCD is true (which means
        // it is an alter table statement) or if it is a create table statement but we are
        // referencing another table instead of self for the primary key.
        final AttachedMTableInfo nParentTable;
        final MTable parentTable;
        MColumnDescriptor parentCD;
        final List<MFieldSchema> parentCols;
        final List<SQLPrimaryKey> existingTablePrimaryKeys;
        final List<SQLUniqueConstraint> existingTableUniqueConstraints;
        final boolean sameTable = fkTableDB.equals(pkTableDB) && fkTableName.equals(pkTableName);
        if (sameTable) {
          nParentTable = nChildTable;
          parentTable = childTable;
          parentCD = childCD;
          parentCols = childCols;
          existingTablePrimaryKeys = primaryKeys;
          existingTableUniqueConstraints = uniqueConstraints;
        } else {
          nParentTable = getMTable(catName, pkTableDB, pkTableName, true);
          parentTable = nParentTable.mtbl;
          if (parentTable == null) {
            throw new InvalidObjectException("Parent table not found: " + pkTableName);
          }
          parentCD = nParentTable.mcd;
          parentCols = parentCD == null || parentCD.getCols() == null ?
              new ArrayList<>() : new ArrayList<>(parentCD.getCols());
          if (parentTable.getPartitionKeys() != null) {
            parentCols.addAll(parentTable.getPartitionKeys());
          }
          PrimaryKeysRequest primaryKeysRequest = new PrimaryKeysRequest(pkTableDB, pkTableName);
          primaryKeysRequest.setCatName(catName);
          existingTablePrimaryKeys = getPrimaryKeys(primaryKeysRequest);
          existingTableUniqueConstraints =
              getUniqueConstraints(new UniqueConstraintsRequest(catName, pkTableDB, pkTableName));
        }

        // Here we build an aux structure that is used to verify that the foreign key that is declared
        // is actually referencing a valid primary key or unique key. We also check that the types of
        // the columns correspond.
        if (existingTablePrimaryKeys.isEmpty() && existingTableUniqueConstraints.isEmpty()) {
          throw new MetaException(
              "Trying to define foreign key but there are no primary keys or unique keys for referenced table");
        }
        final Set<String> validPKsOrUnique = generateValidPKsOrUniqueSignatures(parentCols,
            existingTablePrimaryKeys, existingTableUniqueConstraints);

        StringBuilder fkSignature = new StringBuilder();
        StringBuilder referencedKSignature = new StringBuilder();
        for (; i < foreignKeys.size(); i++) {
          SQLForeignKey foreignKey = foreignKeys.get(i);
          final String fkColumnName = normalizeIdentifier(foreignKey.getFkcolumn_name());
          int childIntegerIndex = getColumnIndexFromTableColumns(childCD.getCols(), fkColumnName);
          if (childIntegerIndex == -1) {
            if (childTable.getPartitionKeys() != null) {
              childCD = null;
              childIntegerIndex = getColumnIndexFromTableColumns(childTable.getPartitionKeys(), fkColumnName);
            }
            if (childIntegerIndex == -1) {
              throw new InvalidObjectException("Child column not found: " + fkColumnName);
            }
          }

          final String pkColumnName = normalizeIdentifier(foreignKey.getPkcolumn_name());
          int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD.getCols(), pkColumnName);
          if (parentIntegerIndex == -1) {
            if (parentTable.getPartitionKeys() != null) {
              parentCD = null;
              parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), pkColumnName);
            }
            if (parentIntegerIndex == -1) {
              throw new InvalidObjectException("Parent column not found: " + pkColumnName);
            }
          }

          if (foreignKey.getFk_name() == null) {
            // When there is no explicit foreign key name associated with the constraint and the key is composite,
            // we expect the foreign keys to be send in order in the input list.
            // Otherwise, the below code will break.
            // If this is the first column of the FK constraint, generate the foreign key name
            // NB: The below code can result in race condition where duplicate names can be generated (in theory).
            // However, this scenario can be ignored for practical purposes because of
            // the uniqueness of the generated constraint name.
            if (foreignKey.getKey_seq() == 1) {
              currentConstraintName = generateConstraintName(parentTable, fkTableDB, fkTableName, pkTableDB,
                  pkTableName, pkColumnName, fkColumnName, "fk");
            }
          } else {
            currentConstraintName = normalizeIdentifier(foreignKey.getFk_name());
            if (constraintNameAlreadyExists(parentTable, currentConstraintName)) {
              String fqConstraintName = String.format("%s.%s.%s", parentTable.getDatabase().getName(),
                  parentTable.getTableName(), currentConstraintName);
              throw new InvalidObjectException("Constraint name already exists: " + fqConstraintName);
            }
          }
          // Update Column, keys, table, database, catalog name
          foreignKey.setFk_name(currentConstraintName);
          foreignKey.setCatName(catName);
          foreignKey.setFktable_db(fkTableDB);
          foreignKey.setFktable_name(fkTableName);
          foreignKey.setPktable_db(pkTableDB);
          foreignKey.setPktable_name(pkTableName);
          foreignKey.setFkcolumn_name(fkColumnName);
          foreignKey.setPkcolumn_name(pkColumnName);

          Integer updateRule = foreignKey.getUpdate_rule();
          Integer deleteRule = foreignKey.getDelete_rule();
          int enableValidateRely = (foreignKey.isEnable_cstr() ? 4 : 0) +
                  (foreignKey.isValidate_cstr() ? 2 : 0) + (foreignKey.isRely_cstr() ? 1 : 0);

          MConstraint mpkfk = new MConstraint(
              currentConstraintName,
              foreignKey.getKey_seq(),
              MConstraint.FOREIGN_KEY_CONSTRAINT,
              deleteRule,
              updateRule,
              enableValidateRely,
              parentTable,
              childTable,
              parentCD,
              childCD,
              childIntegerIndex,
              parentIntegerIndex
          );
          mpkfks.add(mpkfk);

          final String fkColType = getColumnFromTableColumns(childCols, fkColumnName).getType();
          fkSignature.append(
              generateColNameTypeSignature(fkColumnName, fkColType));
          referencedKSignature.append(
              generateColNameTypeSignature(pkColumnName, fkColType));

          if (i + 1 < foreignKeys.size() && foreignKeys.get(i + 1).getKey_seq() == 1) {
            // Next one is a new key, we bail out from the inner loop
            break;
          }
        }
        String referenced = referencedKSignature.toString();
        if (!validPKsOrUnique.contains(referenced)) {
          throw new MetaException(
              "Foreign key references " + referenced + " but no corresponding "
              + "primary key or unique key exists. Possible keys: " + validPKsOrUnique);
        }
        if (sameTable && fkSignature.toString().equals(referenced)) {
          throw new MetaException(
              "Cannot be both foreign key and primary/unique key on same table: " + referenced);
        }
        fkSignature = new StringBuilder();
        referencedKSignature = new StringBuilder();
      }
      pm.makePersistentAll(mpkfks);

    }
    return foreignKeys;
  }

  private static Set<String> generateValidPKsOrUniqueSignatures(List<MFieldSchema> tableCols,
      List<SQLPrimaryKey> refTablePrimaryKeys, List<SQLUniqueConstraint> refTableUniqueConstraints) {
    final Set<String> validPKsOrUnique = new HashSet<>();
    if (!refTablePrimaryKeys.isEmpty()) {
      refTablePrimaryKeys.sort((o1, o2) -> {
        int keyNameComp = o1.getPk_name().compareTo(o2.getPk_name());
        if (keyNameComp == 0) {
          return Integer.compare(o1.getKey_seq(), o2.getKey_seq());
        }
        return keyNameComp;
      });
      StringBuilder pkSignature = new StringBuilder();
      for (SQLPrimaryKey pk : refTablePrimaryKeys) {
        pkSignature.append(
            generateColNameTypeSignature(
                pk.getColumn_name(), getColumnFromTableColumns(tableCols, pk.getColumn_name()).getType()));
      }
      validPKsOrUnique.add(pkSignature.toString());
    }
    if (!refTableUniqueConstraints.isEmpty()) {
      refTableUniqueConstraints.sort((o1, o2) -> {
        int keyNameComp = o1.getUk_name().compareTo(o2.getUk_name());
        if (keyNameComp == 0) {
          return Integer.compare(o1.getKey_seq(), o2.getKey_seq());
        }
        return keyNameComp;
      });
      StringBuilder ukSignature = new StringBuilder();
      for (int j = 0; j < refTableUniqueConstraints.size(); j++) {
        SQLUniqueConstraint uk = refTableUniqueConstraints.get(j);
        ukSignature.append(
            generateColNameTypeSignature(
                uk.getColumn_name(), getColumnFromTableColumns(tableCols, uk.getColumn_name()).getType()));
        if (j + 1 < refTableUniqueConstraints.size()) {
          if (!refTableUniqueConstraints.get(j + 1).getUk_name().equals(
                  refTableUniqueConstraints.get(j).getUk_name())) {
            validPKsOrUnique.add(ukSignature.toString());
            ukSignature = new StringBuilder();
          }
        } else {
          validPKsOrUnique.add(ukSignature.toString());
        }
      }
    }
    return validPKsOrUnique;
  }

  private static String generateColNameTypeSignature(String colName, String colType) {
    return colName + ":" + colType + ";";
  }

  @Override
  public List<SQLPrimaryKey> addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException,
    MetaException {
    return addPrimaryKeys(pks, true);
  }

  private List<SQLPrimaryKey> addPrimaryKeys(List<SQLPrimaryKey> pks, boolean retrieveCD) throws InvalidObjectException,
    MetaException {
    List<MConstraint> mpks = new ArrayList<>();
    String constraintName = null;

    for (SQLPrimaryKey pk : pks) {
      final String catName = normalizeIdentifier(pk.getCatName());
      final String tableDB = normalizeIdentifier(pk.getTable_db());
      final String tableName = normalizeIdentifier(pk.getTable_name());
      final String columnName = normalizeIdentifier(pk.getColumn_name());

      // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
      // For instance, this is the case when we are creating the table.
      AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
      MTable parentTable = nParentTable.mtbl;
      if (parentTable == null) {
        throw new InvalidObjectException("Parent table not found: " + tableName);
      }

      MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
      int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
      if (parentIntegerIndex == -1) {
        if (parentTable.getPartitionKeys() != null) {
          parentCD = null;
          parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
        }
        if (parentIntegerIndex == -1) {
          throw new InvalidObjectException("Parent column not found: " + columnName);
        }
      }
      if (getPrimaryKeyConstraintName(parentTable.getDatabase().getCatalogName(),
          parentTable.getDatabase().getName(), parentTable.getTableName()) != null) {
        throw new MetaException(" Primary key already exists for: " +
            TableName.getQualified(catName, tableDB, tableName));
      }
      if (pk.getPk_name() == null) {
        if (pk.getKey_seq() == 1) {
          constraintName = generateConstraintName(parentTable, tableDB, tableName, columnName, "pk");
        }
      } else {
        constraintName = normalizeIdentifier(pk.getPk_name());
        if (constraintNameAlreadyExists(parentTable, constraintName)) {
          String fqConstraintName = String.format("%s.%s.%s", parentTable.getDatabase().getName(),
              parentTable.getTableName(), constraintName);
          throw new InvalidObjectException("Constraint name already exists: " + fqConstraintName);
        }
      }

      int enableValidateRely = (pk.isEnable_cstr() ? 4 : 0) +
      (pk.isValidate_cstr() ? 2 : 0) + (pk.isRely_cstr() ? 1 : 0);
      MConstraint mpk = new MConstraint(
          constraintName,
          pk.getKey_seq(),
          MConstraint.PRIMARY_KEY_CONSTRAINT,
          null,
          null,
          enableValidateRely,
          parentTable,
          null,
          parentCD,
          null,
          null,
        parentIntegerIndex);
      mpks.add(mpk);

      // Add normalized identifier back to result
      pk.setCatName(catName);
      pk.setTable_db(tableDB);
      pk.setTable_name(tableName);
      pk.setColumn_name(columnName);
      pk.setPk_name(constraintName);
    }
    pm.makePersistentAll(mpks);
    return pks;
  }

  @Override
  public List<SQLUniqueConstraint> addUniqueConstraints(List<SQLUniqueConstraint> uks)
          throws InvalidObjectException, MetaException {
    return addUniqueConstraints(uks, true);
  }

  private List<SQLUniqueConstraint> addUniqueConstraints(List<SQLUniqueConstraint> uks, boolean retrieveCD)
          throws InvalidObjectException, MetaException {

    List<MConstraint> cstrs = new ArrayList<>();
    String constraintName = null;

    for (SQLUniqueConstraint uk : uks) {
      final String catName = normalizeIdentifier(uk.getCatName());
      final String tableDB = normalizeIdentifier(uk.getTable_db());
      final String tableName = normalizeIdentifier(uk.getTable_name());
      final String columnName = normalizeIdentifier(uk.getColumn_name());

      // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
      // For instance, this is the case when we are creating the table.
      AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
      MTable parentTable = nParentTable.mtbl;
      if (parentTable == null) {
        throw new InvalidObjectException("Parent table not found: " + tableName);
      }

      MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
      int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
      if (parentIntegerIndex == -1) {
        if (parentTable.getPartitionKeys() != null) {
          parentCD = null;
          parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
        }
        if (parentIntegerIndex == -1) {
          throw new InvalidObjectException("Parent column not found: " + columnName);
        }
      }
      if (uk.getUk_name() == null) {
        if (uk.getKey_seq() == 1) {
          constraintName = generateConstraintName(parentTable, tableDB, tableName, columnName, "uk");
        }
      } else {
        constraintName = normalizeIdentifier(uk.getUk_name());
        if (constraintNameAlreadyExists(parentTable, constraintName)) {
          String fqConstraintName = String.format("%s.%s.%s", parentTable.getDatabase().getName(),
              parentTable.getTableName(), constraintName);
          throw new InvalidObjectException("Constraint name already exists: " + fqConstraintName);
        }
      }


      int enableValidateRely = (uk.isEnable_cstr() ? 4 : 0) +
          (uk.isValidate_cstr() ? 2 : 0) + (uk.isRely_cstr() ? 1 : 0);
      MConstraint muk = new MConstraint(
          constraintName,
          uk.getKey_seq(),
          MConstraint.UNIQUE_CONSTRAINT,
          null,
          null,
          enableValidateRely,
          parentTable,
          null,
          parentCD,
          null,
          null,
          parentIntegerIndex);
      cstrs.add(muk);

      // Add normalized identifier back to result
      uk.setCatName(catName);
      uk.setTable_db(tableDB);
      uk.setTable_name(tableName);
      uk.setColumn_name(columnName);
      uk.setUk_name(constraintName);

    }
    pm.makePersistentAll(cstrs);
    return uks;
  }

  @Override
  public List<SQLNotNullConstraint> addNotNullConstraints(List<SQLNotNullConstraint> nns)
          throws InvalidObjectException, MetaException {
    return addNotNullConstraints(nns, true);
  }

  @Override
  public List<SQLDefaultConstraint> addDefaultConstraints(List<SQLDefaultConstraint> nns)
      throws InvalidObjectException, MetaException {
    return addDefaultConstraints(nns, true);
  }

  @Override
  public List<SQLCheckConstraint> addCheckConstraints(List<SQLCheckConstraint> nns)
      throws InvalidObjectException, MetaException {
    return addCheckConstraints(nns, true);
  }

  private List<SQLCheckConstraint> addCheckConstraints(List<SQLCheckConstraint> ccs, boolean retrieveCD)
      throws InvalidObjectException, MetaException {
    List<MConstraint> cstrs = new ArrayList<>();

    for (SQLCheckConstraint cc: ccs) {
      final String catName = normalizeIdentifier(cc.getCatName());
      final String tableDB = normalizeIdentifier(cc.getTable_db());
      final String tableName = normalizeIdentifier(cc.getTable_name());
      final String columnName = cc.getColumn_name() == null? null
          : normalizeIdentifier(cc.getColumn_name());
      final String ccName = cc.getDc_name();
      boolean isEnable = cc.isEnable_cstr();
      boolean isValidate = cc.isValidate_cstr();
      boolean isRely = cc.isRely_cstr();
      String constraintValue = cc.getCheck_expression();
      MConstraint muk = addConstraint(catName, tableDB, tableName, columnName, ccName, isEnable, isRely, isValidate,
                    MConstraint.CHECK_CONSTRAINT, constraintValue, retrieveCD);
      cstrs.add(muk);

      // Add normalized identifier back to result
      cc.setCatName(catName);
      cc.setTable_db(tableDB);
      cc.setTable_name(tableName);
      cc.setColumn_name(columnName);
      cc.setDc_name(muk.getConstraintName());
    }
    pm.makePersistentAll(cstrs);
    return ccs;
  }

  private MConstraint addConstraint(String catName, String tableDB, String tableName, String columnName, String ccName,
                               boolean isEnable, boolean isRely, boolean isValidate, int constraintType,
                               String constraintValue, boolean retrieveCD)
      throws InvalidObjectException, MetaException {
    String constraintName = null;
    // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
    // For instance, this is the case when we are creating the table.
    AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
    MTable parentTable = nParentTable.mtbl;
    if (parentTable == null) {
      throw new InvalidObjectException("Parent table not found: " + tableName);
    }

    MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
    int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
    if (parentIntegerIndex == -1) {
      if (parentTable.getPartitionKeys() != null) {
        parentCD = null;
        parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
      }
    }
    if (ccName == null) {
      constraintName = generateConstraintName(parentTable, tableDB, tableName, columnName, "dc");
    } else {
      constraintName = normalizeIdentifier(ccName);
      if (constraintNameAlreadyExists(parentTable, constraintName)) {
        String fqConstraintName = String.format("%s.%s.%s", parentTable.getDatabase().getName(),
            parentTable.getTableName(), constraintName);
        throw new InvalidObjectException("Constraint name already exists: " + fqConstraintName);
      }
    }

    int enableValidateRely = (isEnable ? 4 : 0) +
        (isValidate ? 2 : 0) + (isRely ? 1 : 0);
    MConstraint muk = new MConstraint(
        constraintName,
        1,
        constraintType, // Not null constraint should reference a single column
        null,
        null,
        enableValidateRely,
        parentTable,
        null,
        parentCD,
        null,
        null,
        parentIntegerIndex,
        constraintValue);

    return muk;
  }

  private List<SQLDefaultConstraint> addDefaultConstraints(List<SQLDefaultConstraint> dcs, boolean retrieveCD)
      throws InvalidObjectException, MetaException {

    List<MConstraint> cstrs = new ArrayList<>();
    for (SQLDefaultConstraint dc : dcs) {
      final String catName = normalizeIdentifier(dc.getCatName());
      final String tableDB = normalizeIdentifier(dc.getTable_db());
      final String tableName = normalizeIdentifier(dc.getTable_name());
      final String columnName = normalizeIdentifier(dc.getColumn_name());
      final String dcName = dc.getDc_name();
      boolean isEnable = dc.isEnable_cstr();
      boolean isValidate = dc.isValidate_cstr();
      boolean isRely = dc.isRely_cstr();
      String constraintValue = dc.getDefault_value();
      MConstraint muk = addConstraint(catName, tableDB, tableName, columnName, dcName, isEnable, isRely, isValidate,
      MConstraint.DEFAULT_CONSTRAINT, constraintValue, retrieveCD);
      cstrs.add(muk);

      // Add normalized identifier back to result
      dc.setCatName(catName);
      dc.setTable_db(tableDB);
      dc.setTable_name(tableName);
      dc.setColumn_name(columnName);
      dc.setDc_name(muk.getConstraintName());
    }
    pm.makePersistentAll(cstrs);
    return dcs;
  }

  private List<SQLNotNullConstraint> addNotNullConstraints(List<SQLNotNullConstraint> nns, boolean retrieveCD)
          throws InvalidObjectException, MetaException {

    List<MConstraint> cstrs = new ArrayList<>();
    String constraintName;

    for (SQLNotNullConstraint nn : nns) {
      final String catName = normalizeIdentifier(nn.getCatName());
      final String tableDB = normalizeIdentifier(nn.getTable_db());
      final String tableName = normalizeIdentifier(nn.getTable_name());
      final String columnName = normalizeIdentifier(nn.getColumn_name());

      // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
      // For instance, this is the case when we are creating the table.
      AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
      MTable parentTable = nParentTable.mtbl;
      if (parentTable == null) {
        throw new InvalidObjectException("Parent table not found: " + tableName);
      }

      MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
      int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
      if (parentIntegerIndex == -1) {
        if (parentTable.getPartitionKeys() != null) {
          parentCD = null;
          parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
        }
        if (parentIntegerIndex == -1) {
          throw new InvalidObjectException("Parent column not found: " + columnName);
        }
      }
      if (nn.getNn_name() == null) {
        constraintName = generateConstraintName(parentTable, tableDB, tableName, columnName, "nn");
      } else {
        constraintName = normalizeIdentifier(nn.getNn_name());
        if (constraintNameAlreadyExists(parentTable, constraintName)) {
          String fqConstraintName = String.format("%s.%s.%s", parentTable.getDatabase().getName(),
              parentTable.getTableName(), constraintName);
          throw new InvalidObjectException("Constraint name already exists: " + fqConstraintName);
        }
      }

      int enableValidateRely = (nn.isEnable_cstr() ? 4 : 0) +
          (nn.isValidate_cstr() ? 2 : 0) + (nn.isRely_cstr() ? 1 : 0);
      MConstraint muk = new MConstraint(
          constraintName,
          1,
          MConstraint.NOT_NULL_CONSTRAINT, // Not null constraint should reference a single column
          null,
          null,
          enableValidateRely,
          parentTable,
          null,
          parentCD,
          null,
          null,
          parentIntegerIndex);
      cstrs.add(muk);
      // Add normalized identifier back to result
      nn.setCatName(catName);
      nn.setTable_db(tableDB);
      nn.setTable_name(tableName);
      nn.setColumn_name(columnName);
      nn.setNn_name(constraintName);
    }
    pm.makePersistentAll(cstrs);
    return nns;
  }

  private void writeMTableColumnStatistics(Table table, MTableColumnStatistics mStatsObj,
      MTableColumnStatistics oldStats) throws MetaException {

    Preconditions.checkState(this.currentTransaction.isActive());

    String colName = mStatsObj.getColName();

    LOG.info("Updating table level column statistics for table={} colName={}",
        Warehouse.getCatalogQualifiedTableName(table), colName);
    validateTableCols(table, Lists.newArrayList(colName));

    if (oldStats != null) {
      StatObjectConverter.setFieldsIntoOldStats(mStatsObj, oldStats);
    } else {
      pm.makePersistent(mStatsObj);
    }
  }

  private void writeMPartitionColumnStatistics(Table table, Partition partition,
      MPartitionColumnStatistics mStatsObj, MPartitionColumnStatistics oldStats) {
    String catName = mStatsObj.getPartition().getTable().getDatabase().getCatalogName();
    String dbName = mStatsObj.getPartition().getTable().getDatabase().getName();
    String tableName = mStatsObj.getPartition().getTable().getTableName();
    String partName = mStatsObj.getPartition().getPartitionName();
    String colName = mStatsObj.getColName();

    Preconditions.checkState(this.currentTransaction.isActive());

    LOG.info("Updating partition level column statistics for table=" +
        TableName.getQualified(catName, dbName, tableName) +
        " partName=" + partName + " colName=" + colName);

    boolean foundCol = false;
    List<FieldSchema> colList = partition.getSd().getCols();
    for (FieldSchema col : colList) {
      if (col.getName().equals(mStatsObj.getColName())) {
        foundCol = true;
        break;
      }
    }

    if (!foundCol) {
      LOG.warn("Column " + colName + " for which stats gathering is requested doesn't exist.");
    }

    if (oldStats != null) {
      StatObjectConverter.setFieldsIntoOldStats(mStatsObj, oldStats);
    } else {
      pm.makePersistent(mStatsObj);
    }
  }

  @Override
  public Map<String, String> updateTableColumnStatistics(ColumnStatistics colStats, String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean committed = false;
    List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
    ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
    long start = System.currentTimeMillis();
    String catName = statsDesc.isSetCatName() ? statsDesc.getCatName() : getDefaultCatalog(conf);
    try {
      openTransaction();
      // DataNucleus objects get detached all over the place for no (real) reason.
      // So let's not use them anywhere unless absolutely necessary.
      MTable mTable = ensureGetMTable(catName, statsDesc.getDbName(), statsDesc.getTableName());
      int maxRetries = MetastoreConf.getIntVar(conf, ConfVars.METASTORE_S4U_NOWAIT_MAX_RETRIES);
      long sleepInterval = MetastoreConf.getTimeVar(conf,
          ConfVars.METASTORE_S4U_NOWAIT_RETRY_SLEEP_INTERVAL, TimeUnit.MILLISECONDS);
      Map<String, String> result = new RetryingExecutor<>(maxRetries, () -> {
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        String savePoint = "uts_" + ThreadLocalRandom.current().nextInt(10000) + "_" + System.nanoTime();
        setTransactionSavePoint(savePoint);
        executePlainSQL(
            sqlGenerator.addForUpdateNoWait("SELECT \"TBL_ID\" FROM \"TBLS\" WHERE \"TBL_ID\" = " + mTable.getId()),
            true,
            exception -> {
              rollbackTransactionToSavePoint(savePoint);
              exceptionRef.set(exception);
            });
        if (exceptionRef.get() != null) {
          throw new RetryingExecutor.RetryException(exceptionRef.get());
        }
        pm.refresh(mTable);
        Table table = convertToTable(mTable, conf);
        List<String> colNames = new ArrayList<>();
        for (ColumnStatisticsObj statsObj : statsObjs) {
          colNames.add(statsObj.getColName());
        }

        Map<String, MTableColumnStatistics> oldStats = Maps.newHashMap();
        List<MTableColumnStatistics> stats = getMTableColumnStatistics(table, colNames, colStats.getEngine());
        for (MTableColumnStatistics cStat : stats) {
          oldStats.put(cStat.getColName(), cStat);
        }

        for (ColumnStatisticsObj statsObj : statsObjs) {
          MTableColumnStatistics mStatsObj = StatObjectConverter.convertToMTableColumnStatistics(mTable, statsDesc,
              statsObj, colStats.getEngine());
          writeMTableColumnStatistics(table, mStatsObj, oldStats.get(statsObj.getColName()));
          // There is no need to add colname again, otherwise we will get duplicate colNames.
        }

        // Set the table properties
        // No need to check again if it exists.
        String dbname = table.getDbName();
        String name = table.getTableName();
        MTable oldt = mTable;
        Map<String, String> newParams = new HashMap<>(table.getParameters());
        StatsSetupConst.setColumnStatsState(newParams, colNames);
        boolean isTxn = TxnUtils.isTransactionalTable(oldt.getParameters());
        if (isTxn) {
          if (!areTxnStatsSupported) {
            StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
          } else {
            String errorMsg = verifyStatsChangeCtx(TableName.getDbTable(dbname, name), oldt.getParameters(), newParams,
                writeId, validWriteIds, true);
            if (errorMsg != null) {
              throw new MetaException(errorMsg);
            }
            if (!isCurrentStatsValidForTheQuery(oldt, validWriteIds, true)) {
              // Make sure we set the flag to invalid regardless of the current value.
              StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
              LOG.info("Removed COLUMN_STATS_ACCURATE from the parameters of the table " + dbname + "." + name);
            }
            oldt.setWriteId(writeId);
          }
        }
        oldt.setParameters(newParams);
        return newParams;
      }).onRetry(e -> e instanceof RetryingExecutor.RetryException)
        .commandName("updateTableColumnStatistics").sleepInterval(sleepInterval, interval ->
              ThreadLocalRandom.current().nextLong(sleepInterval) + 30).run();
      committed = commitTransaction();
      return committed ? result : null;
    } finally {
      LOG.debug("{} updateTableColumnStatistics took {}ms, success: {}",
          new TableName(catName, statsDesc.getDbName(), statsDesc.getTableName()),
          System.currentTimeMillis() - start, committed);
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public Map<String, String> updatePartitionColumnStatistics(Table table, MTable mTable, ColumnStatistics colStats,
      List<String> partVals, String validWriteIds, long writeId)
          throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean committed = false;
    long start = System.currentTimeMillis();
    List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
    ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
    String catName = statsDesc.isSetCatName() ? statsDesc.getCatName() : getDefaultCatalog(conf);
    try {
      openTransaction();
      MPartition mPartition =
          ensureGetMPartition(new TableName(catName, statsDesc.getDbName(), statsDesc.getTableName()), partVals);
      if (mPartition == null) {
        throw new NoSuchObjectException("Partition for which stats is gathered doesn't exist.");
      }

      List<String> colNames = new ArrayList<>();
      for(ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }
      int maxRetries = MetastoreConf.getIntVar(conf, ConfVars.METASTORE_S4U_NOWAIT_MAX_RETRIES);
      long sleepInterval = MetastoreConf.getTimeVar(conf,
          ConfVars.METASTORE_S4U_NOWAIT_RETRY_SLEEP_INTERVAL, TimeUnit.MILLISECONDS);
      Map<String, String> result = new RetryingExecutor<>(maxRetries, () -> {
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        String savePoint = "ups_" + ThreadLocalRandom.current().nextInt(10000) + "_" + System.nanoTime();
        setTransactionSavePoint(savePoint);
        executePlainSQL(sqlGenerator.addForUpdateNoWait(
            "SELECT \"PART_ID\" FROM \"PARTITIONS\" WHERE \"PART_ID\" = " + mPartition.getId()),
            true,
            exception -> {
              rollbackTransactionToSavePoint(savePoint);
              exceptionRef.set(exception);
            });
        if (exceptionRef.get() != null) {
          throw new RetryingExecutor.RetryException(exceptionRef.get());
        }
        pm.refresh(mPartition);
        Partition partition = convertToPart(catName, statsDesc.getDbName(), statsDesc.getTableName(),
            mPartition, TxnUtils.isAcidTable(table), conf);
        Map<String, MPartitionColumnStatistics> oldStats = Maps.newHashMap();
        List<MPartitionColumnStatistics> stats =
            getMPartitionColumnStatistics(table, Lists.newArrayList(statsDesc.getPartName()), colNames, colStats.getEngine());
        for (MPartitionColumnStatistics cStat : stats) {
          oldStats.put(cStat.getColName(), cStat);
        }

        for (ColumnStatisticsObj statsObj : statsObjs) {
          MPartitionColumnStatistics mStatsObj = StatObjectConverter.convertToMPartitionColumnStatistics(mPartition,
              statsDesc, statsObj, colStats.getEngine());
          writeMPartitionColumnStatistics(table, partition, mStatsObj, oldStats.get(statsObj.getColName()));
        }

        Map<String, String> newParams = new HashMap<>(mPartition.getParameters());
        StatsSetupConst.setColumnStatsState(newParams, colNames);
        boolean isTxn = TxnUtils.isTransactionalTable(table);
        if (isTxn) {
          if (!areTxnStatsSupported) {
            StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
          } else {
            String errorMsg = verifyStatsChangeCtx(
                TableName.getDbTable(statsDesc.getDbName(), statsDesc.getTableName()), mPartition.getParameters(),
                newParams, writeId, validWriteIds, true);
            if (errorMsg != null) {
              throw new MetaException(errorMsg);
            }
            if (!isCurrentStatsValidForTheQuery(mPartition.getParameters(), mPartition.getWriteId(), validWriteIds, true)) {
              // Make sure we set the flag to invalid regardless of the current value.
              StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
              LOG.info("Removed COLUMN_STATS_ACCURATE from the parameters of the partition: {}, {} ",
                  new TableName(catName, statsDesc.getDbName(), statsDesc.getTableName()), statsDesc.getPartName());
            }
            mPartition.setWriteId(writeId);
          }
        }
        mPartition.setParameters(newParams);
        return newParams;
      }).onRetry(e -> e instanceof RetryingExecutor.RetryException)
          .commandName("updatePartitionColumnStatistics").sleepInterval(sleepInterval, interval ->
              ThreadLocalRandom.current().nextLong(sleepInterval) + 30).run();
      committed = commitTransaction();
      return committed ? result : null;
    } finally {
      LOG.debug("{} updatePartitionColumnStatistics took {}ms, success: {}",
          new TableName(catName, statsDesc.getDbName(), statsDesc.getTableName()),
          System.currentTimeMillis() - start, committed);
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public Map<String, String> updatePartitionColumnStatistics(ColumnStatistics colStats,
      List<String> partVals, String validWriteIds, long writeId)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
    Table table = getTable(statsDesc.getCatName(), statsDesc.getDbName(), statsDesc.getTableName());
    MTable mTable = ensureGetMTable(statsDesc.getCatName(), statsDesc.getDbName(), statsDesc.getTableName());
    return updatePartitionColumnStatistics(table, mTable, colStats, partVals, validWriteIds, writeId);
  }

  @Override
  public Map<String, Map<String, String>> updatePartitionColumnStatisticsInBatch(
                                                      Map<String, ColumnStatistics> partColStatsMap,
                                                      Table tbl,
                                                      List<TransactionalMetaStoreEventListener> listeners,
                                                      String validWriteIds, long writeId)
          throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {

    return new GetHelper<Map<String, Map<String, String>>>(tbl.getCatName(),
        tbl.getDbName(), tbl.getTableName(), true, false) {
      @Override
      protected String describeResult() {
        return "Map of partition key to column stats if successful";
      }
      @Override
      protected Map<String, Map<String, String>> getSqlResult(GetHelper<Map<String, Map<String, String>>> ctx)
          throws MetaException {
        return directSql.updatePartitionColumnStatisticsBatch(partColStatsMap, tbl,
            listeners, validWriteIds, writeId);
      }
      @Override
      protected Map<String, Map<String, String>> getJdoResult(GetHelper<Map<String, Map<String, String>>> ctx)
          throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        throw new UnsupportedOperationException("Cannot update partition column statistics with JDO, make sure direct SQL is enabled");
      }
    }.run(false);
  }

  private List<MTableColumnStatistics> getMTableColumnStatistics(Table table, List<String> colNames, String engine)
      throws MetaException {

    Preconditions.checkState(this.currentTransaction.isActive());

    if (colNames.isEmpty()) {
      return Collections.emptyList();
    }

    boolean committed = false;
    try {
      openTransaction();

      validateTableCols(table, colNames);

      List<MTableColumnStatistics> result = Collections.emptyList();
      try (Query query = pm.newQuery(MTableColumnStatistics.class)) {
      result =
          Batchable.runBatched(batchSize, colNames, new Batchable<String, MTableColumnStatistics>() {
            @Override
            public List<MTableColumnStatistics> run(List<String> input)
                throws MetaException {
              StringBuilder filter =
                  new StringBuilder("table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3 && engine == t4 && (");
              StringBuilder paramStr = new StringBuilder(
                  "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4");
              Object[] params = new Object[input.size() + 4];
              params[0] = table.getTableName();
              params[1] = table.getDbName();
              params[2] = table.getCatName();
              params[3] = engine;
              for (int i = 0; i < input.size(); ++i) {
                filter.append((i == 0) ? "" : " || ").append("colName == c").append(i);
                paramStr.append(", java.lang.String c").append(i);
                params[i + 4] = input.get(i);
              }
              filter.append(")");
              query.setFilter(filter.toString());
              query.declareParameters(paramStr.toString());
              List<MTableColumnStatistics> paritial = (List<MTableColumnStatistics>) query.executeWithArray(params);
              pm.retrieveAll(paritial);
              return paritial;
            }
          });

      if (result.size() > colNames.size()) {
        throw new MetaException("Unexpected " + result.size() + " statistics for "
            + colNames.size() + " columns");
      }
      result = new ArrayList<>(result);
      }
      committed = commitTransaction();
      return result;
    } catch (Exception ex) {
      LOG.error("Error retrieving statistics via jdo", ex);
      if (ex instanceof MetaException) {
        throw (MetaException) ex;
      }
      throw new MetaException(ex.getMessage());
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @VisibleForTesting
  public void validateTableCols(Table table, List<String> colNames) throws MetaException {
    List<FieldSchema> colList = table.getSd().getCols();
    for (String colName : colNames) {
      boolean foundCol = false;
      for (FieldSchema mCol : colList) {
        if (mCol.getName().equals(colName)) {
          foundCol = true;
          break;
        }
      }
      if (!foundCol) {
        throw new MetaException("Column " + colName + " doesn't exist in table "
            + table.getTableName() + " in database " + table.getDbName());
      }
    }
  }

  @Override
  public List<ColumnStatistics> getTableColumnStatistics(
      String catName,
      String dbName,
      String tableName,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    // Note: this will get stats without verifying ACID.
    boolean committed = false;
    Query query = null;
    List<ColumnStatistics> result = new ArrayList<>();

    try {
      openTransaction();
      query = pm.newQuery(MTableColumnStatistics.class);
      query.setFilter("table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      query.setResult("DISTINCT engine");
      Collection names = (Collection) query.execute(tableName, dbName, catName);
      List<String> engines = new ArrayList<>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        engines.add((String) i.next());
      }
      for (String e : engines) {
        ColumnStatistics cs = getTableColumnStatisticsInternal(
            catName, dbName, tableName, colNames, e, true, true);
        if (cs != null) {
          result.add(cs);
        }
      }
      committed = commitTransaction();
      return result;
    } finally {
      LOG.debug("Done executing getTableColumnStatistics with status : {}",
          committed);
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(
      String catName,
      String dbName,
      String tableName,
      List<String> colNames,
      String engine) throws MetaException, NoSuchObjectException {
    // Note: this will get stats without verifying ACID.
    return getTableColumnStatisticsInternal(
        catName, dbName, tableName, colNames, engine, true, true);
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(
      String catName,
      String dbName,
      String tableName,
      List<String> colNames,
      String engine,
      String writeIdList) throws MetaException, NoSuchObjectException {
    // If the current stats in the metastore doesn't comply with
    // the isolation level of the query, set No to the compliance flag.
    Boolean isCompliant = null;
    if (writeIdList != null) {
      MTable table = this.getMTable(catName, dbName, tableName);
      if (table == null) {
        throw new NoSuchObjectException(TableName.getQualified(catName, dbName, tableName) + " table not found");
      }
      isCompliant = !TxnUtils.isTransactionalTable(table.getParameters())
        || (areTxnStatsSupported && isCurrentStatsValidForTheQuery(table, writeIdList, false));
    }
    ColumnStatistics stats = getTableColumnStatisticsInternal(
        catName, dbName, tableName, colNames, engine, true, true);
    if (stats != null && isCompliant != null) {
      stats.setIsStatsCompliant(isCompliant);
    }
    return stats;
  }

  protected ColumnStatistics getTableColumnStatisticsInternal(
      String catName, String dbName, String tableName, final List<String> colNames, String engine,
      boolean allowSql, boolean allowJdo) throws MetaException, NoSuchObjectException {
    final boolean enableBitVector = MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_BITVECTOR);
    final boolean enableKll = MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_KLL);
    return new GetStatHelper(normalizeIdentifier(catName), normalizeIdentifier(dbName),
        normalizeIdentifier(tableName), allowSql, allowJdo, null) {
      @Override
      protected ColumnStatistics getSqlResult(GetHelper<ColumnStatistics> ctx) throws MetaException {
        return directSqlAggrStats.getTableStats(catName, dbName, tblName, colNames, engine, enableBitVector, enableKll);
      }

      @Override
      protected ColumnStatistics getJdoResult(GetHelper<ColumnStatistics> ctx) throws MetaException {

        List<MTableColumnStatistics> mStats = getMTableColumnStatistics(getTable(), colNames, engine);
        if (mStats.isEmpty()) {
          return null;
        }
        // LastAnalyzed is stored per column, but thrift object has it per
        // multiple columns. Luckily, nobody actually uses it, so we will set to
        // lowest value of all columns for now.
        ColumnStatisticsDesc desc = StatObjectConverter.getTableColumnStatisticsDesc(mStats.get(0));
        List<ColumnStatisticsObj> statObjs = new ArrayList<>(mStats.size());
        for (MTableColumnStatistics mStat : mStats) {
          if (desc.getLastAnalyzed() > mStat.getLastAnalyzed()) {
            desc.setLastAnalyzed(mStat.getLastAnalyzed());
          }
          statObjs.add(StatObjectConverter.getColumnStatisticsObj(mStat, enableBitVector, enableKll));
          Deadline.checkTimeout();
        }
        ColumnStatistics colStat = new ColumnStatistics(desc, statObjs);
        colStat.setEngine(engine);
        return colStat;
      }
    }.run(true);
  }

  @Override
  public List<List<ColumnStatistics>> getPartitionColumnStatistics(String catName, String dbName, String tableName,
      List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
    // Note: this will get stats without verifying ACID.
    boolean committed = false;
    Query query = null;
    List<List<ColumnStatistics>> result = new ArrayList<>();

    try {
      openTransaction();
      query = pm.newQuery(MPartitionColumnStatistics.class);
      query.setFilter("partition.table.tableName == t1 && partition.table.database.name == t2 && partition.table.database.catalogName == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      query.setResult("DISTINCT engine");
      Collection names = (Collection) query.execute(tableName, dbName, catName);
      List<String> engines = new ArrayList<>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        engines.add((String) i.next());
      }
      for (String e : engines) {
        List<ColumnStatistics> cs = getPartitionColumnStatisticsInternal(
            catName, dbName, tableName, partNames, colNames, e, true, true);
        if (cs != null) {
          result.add(cs);
        }
      }
      committed = commitTransaction();
      return result;
    } finally {
      LOG.debug("Done executing getTableColumnStatistics with status : {}",
          committed);
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String catName, String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine) throws MetaException, NoSuchObjectException {
    // Note: this will get stats without verifying ACID.
    if (CollectionUtils.isEmpty(partNames) || CollectionUtils.isEmpty(colNames)) {
      LOG.debug("PartNames and/or ColNames are empty");
      return Collections.emptyList();
    }
    return getPartitionColumnStatisticsInternal(
        catName, dbName, tableName, partNames, colNames, engine, true, true);
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(
      String catName, String dbName, String tableName,
      List<String> partNames, List<String> colNames,
      String engine, String writeIdList)
      throws MetaException, NoSuchObjectException {
    if (CollectionUtils.isEmpty(partNames) || CollectionUtils.isEmpty(colNames)) {
      LOG.debug("PartNames and/or ColNames are empty");
      return Collections.emptyList();
    }
    List<ColumnStatistics> allStats = getPartitionColumnStatisticsInternal(
        catName, dbName, tableName, partNames, colNames, engine, true, true);
    if (writeIdList != null) {
      if (!areTxnStatsSupported) {
        for (ColumnStatistics cs : allStats) {
          cs.setIsStatsCompliant(false);
        }
      } else {
        // TODO: this could be improved to get partitions in bulk
        for (ColumnStatistics cs : allStats) {
          MPartition mpart = ensureGetMPartition(new TableName(catName, dbName, tableName),
              Warehouse.getPartValuesFromPartName(cs.getStatsDesc().getPartName()));
          if (mpart == null
              || !isCurrentStatsValidForTheQuery(mpart.getParameters(), mpart.getWriteId(), writeIdList, false)) {
            if (mpart != null) {
              LOG.debug("The current metastore transactional partition column statistics for {}.{}.{} "
                + "(write ID {}) are not valid for current query ({} {})", dbName, tableName,
                mpart.getPartitionName(), mpart.getWriteId(), writeIdList);
            }
            cs.setIsStatsCompliant(false);
          } else {
            cs.setIsStatsCompliant(true);
          }
        }
      }
    }
    return allStats;
  }

  protected List<ColumnStatistics> getPartitionColumnStatisticsInternal(
      String catName, String dbName, String tableName, final List<String> partNames, final List<String> colNames,
      String engine, boolean allowSql, boolean allowJdo) throws MetaException, NoSuchObjectException {
    final boolean enableBitVector = MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_BITVECTOR);
    final boolean enableKll = MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_KLL);
    return new GetListHelper<ColumnStatistics>(catName, dbName, tableName, allowSql, allowJdo) {
      @Override
      protected List<ColumnStatistics> getSqlResult(
          GetHelper<List<ColumnStatistics>> ctx) throws MetaException {
        return directSqlAggrStats.getPartitionStats(
            catName, dbName, tblName, partNames, colNames, engine, enableBitVector, enableKll);
      }
      @Override
      protected List<ColumnStatistics> getJdoResult(GetHelper<List<ColumnStatistics>> ctx)
          throws MetaException, NoSuchObjectException {
        List<MPartitionColumnStatistics> mStats =
            getMPartitionColumnStatistics(getTable(), partNames, colNames, engine);
        List<ColumnStatistics> result = new ArrayList<>(Math.min(mStats.size(), partNames.size()));
        String lastPartName = null;
        List<ColumnStatisticsObj> curList = null;
        ColumnStatisticsDesc csd = null;
        for (int i = 0; i <= mStats.size(); ++i) {
          boolean isLast = i == mStats.size();
          MPartitionColumnStatistics mStatsObj = isLast ? null : mStats.get(i);
          String partName = isLast ? null : mStatsObj.getPartition().getPartitionName();
          if (isLast || !partName.equals(lastPartName)) {
            if (i != 0) {
              ColumnStatistics colStat = new ColumnStatistics(csd, curList);
              colStat.setEngine(engine);
              result.add(colStat);
            }
            if (isLast) {
              continue;
            }
            csd = StatObjectConverter.getPartitionColumnStatisticsDesc(mStatsObj);
            curList = new ArrayList<>(colNames.size());
          }
          curList.add(StatObjectConverter.getColumnStatisticsObj(mStatsObj, enableBitVector, enableKll));
          lastPartName = partName;
          Deadline.checkTimeout();
        }
        return result;
      }
    }.run(true);
  }

  @Override
  public AggrStats get_aggr_stats_for(String catName, String dbName, String tblName,
      final List<String> partNames, final List<String> colNames,
      String engine, String writeIdList) throws MetaException, NoSuchObjectException {
    // If the current stats in the metastore doesn't comply with
    // the isolation level of the query, return null.
    if (writeIdList != null) {
      if (partNames == null || partNames.isEmpty()) {
        return null;
      }

      Table table = getTable(catName, dbName, tblName);
      boolean isTxn = TxnUtils.isTransactionalTable(table.getParameters());
      if (isTxn && !areTxnStatsSupported) {
        return null;
      }
      GetPartitionsFilterSpec fs = new GetPartitionsFilterSpec();
      fs.setFilterMode(PartitionFilterMode.BY_NAMES);
      fs.setFilters(partNames);
      GetProjectionsSpec ps = new GetProjectionsSpec();
      ps.setIncludeParamKeyPattern(StatsSetupConst.COLUMN_STATS_ACCURATE + '%');
      ps.setFieldList(Lists.newArrayList("writeId", "parameters", "values"));
      List<Partition> parts = getPartitionSpecsByFilterAndProjection(table, ps, fs);

      // Loop through the given "partNames" list
      // checking isolation-level-compliance of each partition column stats.
      for (Partition part : parts) {

        if (!isCurrentStatsValidForTheQuery(part.getParameters(), part.getWriteId(), writeIdList, false)) {
          String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
          LOG.debug("The current metastore transactional partition column "
              + "statistics for {}.{}.{} is not valid for the current query",
              dbName, tblName, partName);
          return null;
        }
      }
    }
    return get_aggr_stats_for(catName, dbName, tblName, partNames, colNames, engine);
  }

  @Override
  public AggrStats get_aggr_stats_for(String catName, String dbName, String tblName,
      final List<String> partNames, final List<String> colNames, String engine)
      throws MetaException, NoSuchObjectException {
    final boolean useDensityFunctionForNDVEstimation = MetastoreConf.getBoolVar(getConf(),
        ConfVars.STATS_NDV_DENSITY_FUNCTION);
    final double ndvTuner = MetastoreConf.getDoubleVar(getConf(), ConfVars.STATS_NDV_TUNER);
    final boolean enableBitVector = MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_BITVECTOR);
    final boolean enableKll = MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_KLL);
    return new GetHelper<AggrStats>(catName, dbName, tblName, true, false) {
      @Override
      protected AggrStats getSqlResult(GetHelper<AggrStats> ctx)
          throws MetaException {
        return directSql.aggrColStatsForPartitions(catName, dbName, tblName, partNames,
            colNames, engine, useDensityFunctionForNDVEstimation, ndvTuner, enableBitVector, enableKll);
      }
      @Override
      protected AggrStats getJdoResult(GetHelper<AggrStats> ctx)
          throws MetaException, NoSuchObjectException {
        // This is fast path for query optimizations, if we can find this info
        // quickly using
        // directSql, do it. No point in failing back to slow path here.
        throw new MetaException("Jdo path is not implemented for stats aggr.");
      }
      @Override
      protected String describeResult() {
        return null;
      }
    }.run(true);
  }

  @Override
  public List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    final boolean enableBitVector = MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_BITVECTOR);
    final boolean enableKll = MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_KLL);
    return new GetHelper<List<MetaStoreServerUtils.ColStatsObjWithSourceInfo>>(
        catName, dbName, null, true, false) {
      @Override
      protected List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> getSqlResult(
          GetHelper<List<MetaStoreServerUtils.ColStatsObjWithSourceInfo>> ctx) throws MetaException {
        return directSqlAggrStats.getColStatsForAllTablePartitions(catName, dbName, enableBitVector, enableKll);
      }

      @Override
      protected List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> getJdoResult(
          GetHelper<List<MetaStoreServerUtils.ColStatsObjWithSourceInfo>> ctx)
          throws MetaException, NoSuchObjectException {
        // This is fast path for query optimizations, if we can find this info
        // quickly using directSql, do it. No point in failing back to slow path
        // here.
        throw new MetaException("Jdo path is not implemented for getPartitionColStatsForDatabase.");
      }

      @Override
      protected String describeResult() {
        return null;
      }
    }.run(true);
  }

  private List<MPartitionColumnStatistics> getMPartitionColumnStatistics(Table table, List<String> partNames,
      List<String> colNames, String engine) throws MetaException {
    boolean committed = false;

    try {
      openTransaction();
      // We are not going to verify SD for each partition. Just verify for the
      // table. TODO: we need verify the partition column instead
      try {
        validateTableCols(table, colNames);
      } catch (MetaException me) {
        LOG.warn("The table does not have the same column definition as its partition.");
      }
      List<MPartitionColumnStatistics> result = Collections.emptyList();
      try (Query query = pm.newQuery(MPartitionColumnStatistics.class)) {
        String paramStr = "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4";
        String filter = "partition.table.tableName == t1 && partition.table.database.name == t2 && partition.table.database.catalogName == t3 && engine == t4 && (";
        Object[] params = new Object[colNames.size() + partNames.size() + 4];
        int i = 0;
        params[i++] = table.getTableName();
        params[i++] = table.getDbName();
        params[i++] = table.isSetCatName() ? table.getCatName() : getDefaultCatalog(conf);
        params[i++] = engine;
        int firstI = i;
        for (String s : partNames) {
          filter += ((i == firstI) ? "" : " || ") + "partition.partitionName == p" + i;
          paramStr += ", java.lang.String p" + i;
          params[i++] = s;
        }
        filter += ") && (";
        firstI = i;
        for (String s : colNames) {
          filter += ((i == firstI) ? "" : " || ") + "colName == c" + i;
          paramStr += ", java.lang.String c" + i;
          params[i++] = s;
        }
        filter += ")";
        query.setFilter(filter);
        query.declareParameters(paramStr);
        query.setOrdering("partition.partitionName ascending");
        result = (List<MPartitionColumnStatistics>) query.executeWithArray(params);
        pm.retrieveAll(result);
        result = new ArrayList<>(result);
      } catch (Exception ex) {
        LOG.error("Error retrieving statistics via jdo", ex);
        throw new MetaException(ex.getMessage());
      }
      committed = commitTransaction();
      return result;
    } finally {
      if (!committed) {
        rollbackTransaction();
        return Collections.emptyList();
      }
    }
  }

  @Override
  public void deleteAllPartitionColumnStatistics(TableName tn, String writeIdList) {

    String catName = tn.getCat();
    String dbName = tn.getDb();
    String tableName = tn.getTable();

    Query query = null;
    dbName = org.apache.commons.lang3.StringUtils.defaultString(dbName, Warehouse.DEFAULT_DATABASE_NAME);
    catName = normalizeIdentifier(catName);
    if (tableName == null) {
      throw new RuntimeException("Table name is null.");
    }
    boolean ret = false;
    try {
      openTransaction();
      MTable mTable = getMTable(catName, dbName, tableName);

      query = pm.newQuery(MPartitionColumnStatistics.class);

      String filter = "partition.table.database.name == t2 && partition.table.tableName == t3 && partition.table.database.catalogName == t4";
      String parameters = "java.lang.String t2, java.lang.String t3, java.lang.String t4";

      query.setFilter(filter);
      query.declareParameters(parameters);

      Long number = query.deletePersistentAll(normalizeIdentifier(dbName), normalizeIdentifier(tableName),
          normalizeIdentifier(catName));

      new GetHelper<Integer>(catName, dbName, tableName, true, true) {
        private final SqlFilterForPushdown filter = new SqlFilterForPushdown();

        @Override
        protected String describeResult() {
          return "Partition count";
        }

        @Override
        protected boolean canUseDirectSql(GetHelper<Integer> ctx) throws MetaException {
          return true;
        }

        @Override
        protected Integer getSqlResult(GetHelper<Integer> ctx) throws MetaException {
          directSql.deleteColumnStatsState(getTable().getId());
          return 0;
        }

        @Override
        protected Integer getJdoResult(GetHelper<Integer> ctx) throws MetaException, NoSuchObjectException {
          try {
            List<Partition> parts = getPartitions(catName, dbName, tableName,
                GetPartitionsArgs.getAllPartitions());
            for (Partition part : parts) {
              Partition newPart = new Partition(part);
              StatsSetupConst.clearColumnStatsState(newPart.getParameters());
              alterPartition(catName, dbName, tableName, part.getValues(), newPart, writeIdList);
            }
            return parts.size();
          } catch (InvalidObjectException e) {
            LOG.error("error updating parts", e);
            return -1;
          }
        }
      }.run(true);

      ret = commitTransaction();
    } catch (Exception e) {
      LOG.error("Couldn't clear stats for table", e);
    } finally {
      rollbackAndCleanup(ret, query);
    }
  }

  @Override
  public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    if (partNames == null || partNames.isEmpty()) {
      throw new InvalidInputException("No partition specified for dropping the statistics");
    }
    dbName = org.apache.commons.lang3.StringUtils.defaultString(dbName, Warehouse.DEFAULT_DATABASE_NAME);
    catName = normalizeIdentifier(catName);
    List<String> cols = normalizeIdentifiers(colNames);
    return new GetHelper<Boolean>(catName, dbName, tableName, true, true) {
      @Override
      protected String describeResult() {
        return "delete partition column stats";
      }
      @Override
      protected Boolean getSqlResult(GetHelper<Boolean> ctx) throws MetaException {
        DirectSqlDeleteStats deleteStats = new DirectSqlDeleteStats(directSql, pm);
        return deleteStats.deletePartitionColumnStats(catName, dbName, tableName, partNames, cols, engine);
      }
      @Override
      protected Boolean getJdoResult(GetHelper<Boolean> ctx)
              throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return deletePartitionColumnStatisticsViaJdo(catName, dbName, tableName, partNames, cols, engine);
      }
    }.run(false);
  }

  private boolean deletePartitionColumnStatisticsViaJdo(String catName, String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean ret = false;
    String database = org.apache.commons.lang3.StringUtils.defaultString(dbName,
      Warehouse.DEFAULT_DATABASE_NAME);
    String catalog = normalizeIdentifier(catName);
    try {
      openTransaction();
      Batchable<String, Void> b = new Batchable<String, Void>() {
        @Override
        public List<Void> run(List<String> input) throws Exception {
          Query query = pm.newQuery(MPartitionColumnStatistics.class);
          addQueryAfterUse(query);
          String filter;
          String parameters;
          if (colNames != null && !colNames.isEmpty()) {
            filter = "t1.contains(partition.partitionName) && partition.table.database.name == t2 && partition.table.tableName == t3 && "
                + "t4.contains(colName) && partition.table.database.catalogName == t5" + (engine != null ? " && engine == t6" : "");
            parameters = "java.util.Collection t1, java.lang.String t2, java.lang.String t3, "
                + "java.util.Collection t4, java.lang.String t5" + (engine != null ? ", java.lang.String t6" : "");
          } else {
            filter = "t1.contains(partition.partitionName) && partition.table.database.name == t2 && partition.table.tableName == t3 && " +
                "partition.table.database.catalogName == t4" + (engine != null ? " && engine == t5" : "");
            parameters = "java.util.Collection t1, java.lang.String t2, java.lang.String t3, java.lang.String t4" + (engine != null ? ", java.lang.String t5" : "");
          }
          query.setFilter(filter);
          query.declareParameters(parameters);
          List<Object> params = new ArrayList<>();
          params.add(input);
          params.add(normalizeIdentifier(database));
          params.add(normalizeIdentifier(tableName));
          if (colNames != null && !colNames.isEmpty()) {
            params.add(colNames);
          }
          params.add(catalog);
          if (engine != null) {
            params.add(engine);
          }
          List<MPartitionColumnStatistics> mStatsObjColl =
              (List<MPartitionColumnStatistics>) query.executeWithArray(params.toArray());
          pm.retrieveAll(mStatsObjColl);
          if (mStatsObjColl != null) {
            pm.deletePersistentAll(mStatsObjColl);
          }
          return null;
        }
      };
      try {
        Batchable.runBatched(batchSize, partNames, b);
      } finally {
        b.closeAllQueries();
      }

      Batchable.runBatched(batchSize, partNames, new Batchable<String, Void>() {
        @Override
        public List<Void> run(List<String> input) throws MetaException {
          Pair<Query, Map<String, String>> queryWithParams = getPartQueryWithParams(pm, catalog, database, tableName,
              input);
          try (QueryWrapper qw = new QueryWrapper(queryWithParams.getLeft())) {
            qw.setResultClass(MPartition.class);
            qw.setClass(MPartition.class);
            List<MPartition> mparts = (List<MPartition>) qw.executeWithMap(queryWithParams.getRight());
            for (MPartition mPart : mparts) {
              Map<String, String> params = mPart.getParameters();
              if (params != null && params.containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
                if (colNames == null || colNames.isEmpty()) {
                  StatsSetupConst.clearColumnStatsState(params);
                } else {
                  StatsSetupConst.removeColumnStatsState(params, colNames);
                }
                mPart.setParameters(params);
              }
            }
          }
          return Collections.emptyList();
        }
      });
      ret = commitTransaction();
    } finally {
      rollbackAndCleanup(ret, null);
    }
    return ret;
  }

  @Override
  public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    dbName = org.apache.commons.lang3.StringUtils.defaultString(dbName, Warehouse.DEFAULT_DATABASE_NAME);
    if (tableName == null) {
      throw new InvalidInputException("Table name is null.");
    }
    List<String> cols = normalizeIdentifiers(colNames);
    return new GetHelper<Boolean>(catName, dbName, tableName, true, true) {
      @Override
      protected String describeResult() {
        return "delete table column stats";
      }
      @Override
      protected Boolean getSqlResult(GetHelper<Boolean> ctx) throws MetaException {
        DirectSqlDeleteStats deleteStats = new DirectSqlDeleteStats(directSql, pm);
        return deleteStats.deleteTableColumnStatistics(getTable(), cols, engine);
      }
      @Override
      protected Boolean getJdoResult(GetHelper<Boolean> ctx)
              throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return deleteTableColumnStatisticsViaJdo(catName, dbName, tableName, cols, engine);
      }
    }.run(true);
  }

  private boolean deleteTableColumnStatisticsViaJdo(String catName, String dbName, String tableName,
      List<String> colNames, String engine) throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean ret = false;
    Query query = null;
    try {
      openTransaction();
      List<MTableColumnStatistics> mStatsObjColl;
      // Note: this does not verify ACID state; called internally when removing cols/etc.
      //       Also called via an unused metastore API that checks for ACID tables.
      query = pm.newQuery(MTableColumnStatistics.class);
      String filter;
      String parameters;
      if (colNames != null && !colNames.isEmpty()) {
        filter = "table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3 && t4.contains(colName)" + (engine != null ? " && engine == t5" : "");
        parameters = "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.util.Collection t4" + (engine != null ? ", java.lang.String t5" : "");
      } else {
        filter = "table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3" + (engine != null ? " && engine == t4" : "");
        parameters = "java.lang.String t1, java.lang.String t2, java.lang.String t3" + (engine != null ? ", java.lang.String t4" : "");
      }

      query.setFilter(filter);
      query.declareParameters(parameters);
      List<Object> params = new ArrayList<>();
      params.add(normalizeIdentifier(tableName));
      params.add(normalizeIdentifier(dbName));
      params.add(catName == null ? null : normalizeIdentifier(catName));
      if (colNames != null && !colNames.isEmpty()) {
        params.add(colNames);
      }
      if (engine != null) {
        params.add(engine);
      }
      mStatsObjColl = (List<MTableColumnStatistics>) query.executeWithArray(params.toArray());
      pm.retrieveAll(mStatsObjColl);
      if (mStatsObjColl != null) {
        pm.deletePersistentAll(mStatsObjColl);
      }

      MTable mTable = getMTable(catName, dbName, tableName);
      if (mTable != null) {
        Map<String, String> tableParams = mTable.getParameters();
        if (tableParams != null && tableParams.containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
          if (colNames == null || colNames.isEmpty()) {
            StatsSetupConst.clearColumnStatsState(tableParams);
          } else {
            StatsSetupConst.removeColumnStatsState(tableParams, colNames);
          }
          mTable.setParameters(tableParams);
        }
      }
      ret = commitTransaction();
    } finally {
      rollbackAndCleanup(ret, query);
    }
    return ret;
  }

  @Override
  public long cleanupEvents() {
    boolean commited = false;
    Query query = null;
    long delCnt;
    LOG.debug("Begin executing cleanupEvents");
    Long expiryTime =
        MetastoreConf.getTimeVar(getConf(), ConfVars.EVENT_EXPIRY_DURATION, TimeUnit.MILLISECONDS);
    Long curTime = System.currentTimeMillis();
    try {
      openTransaction();
      query = pm.newQuery(MPartitionEvent.class, "curTime - eventTime > expiryTime");
      query.declareParameters("java.lang.Long curTime, java.lang.Long expiryTime");
      delCnt = query.deletePersistentAll(curTime, expiryTime);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
      LOG.debug("Done executing cleanupEvents");
    }
    return delCnt;
  }

  private MDelegationToken getTokenFrom(String tokenId) {
    try (QueryWrapper query = new QueryWrapper(pm.newQuery(MDelegationToken.class, "tokenIdentifier == tokenId"))) {
      query.declareParameters("java.lang.String tokenId");
      query.setUnique(true);
      MDelegationToken delegationToken = (MDelegationToken) query.execute(tokenId);
      return delegationToken;
    }
  }

  @Override
  public boolean addToken(String tokenId, String delegationToken) {

    LOG.debug("Begin executing addToken");
    boolean committed = false;
    MDelegationToken token;
    try{
      openTransaction();
      token = getTokenFrom(tokenId);
      if (token == null) {
        // add Token, only if it already doesn't exist
        pm.makePersistent(new MDelegationToken(tokenId, delegationToken));
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
    LOG.debug("Done executing addToken with status : {}", committed);
    return committed && (token == null);
  }

  @Override
  public boolean removeToken(String tokenId) {

    LOG.debug("Begin executing removeToken");
    boolean committed = false;
    MDelegationToken token;
    try{
      openTransaction();
      token = getTokenFrom(tokenId);
      if (null != token) {
        pm.deletePersistent(token);
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
    LOG.debug("Done executing removeToken with status : {}", committed);
    return committed && (token != null);
  }

  @Override
  public String getToken(String tokenId) {

    LOG.debug("Begin executing getToken");
    boolean committed = false;
    MDelegationToken token;
    try{
      openTransaction();
      token = getTokenFrom(tokenId);
      if (null != token) {
        pm.retrieve(token);
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
    LOG.debug("Done executing getToken with status : {}", committed);
    return (null == token) ? null : token.getTokenStr();
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    LOG.debug("Begin executing getAllTokenIdentifiers");
    boolean committed = false;
    Query query = null;
    List<String> tokenIdents = new ArrayList<>();

    try {
      openTransaction();
      query = pm.newQuery(MDelegationToken.class);
      List<MDelegationToken> tokens = (List<MDelegationToken>) query.execute();
      pm.retrieveAll(tokens);
      committed = commitTransaction();

      for (MDelegationToken token : tokens) {
        tokenIdents.add(token.getTokenIdentifier());
      }
      return tokenIdents;
    } finally {
      LOG.debug("Done executing getAllTokenIdentifers with status : {}", committed);
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public int addMasterKey(String key) throws MetaException{
    LOG.debug("Begin executing addMasterKey");
    boolean committed = false;
    MMasterKey masterKey = new MMasterKey(key);
    try{
      openTransaction();
      pm.makePersistent(masterKey);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
    LOG.debug("Done executing addMasterKey with status : {}", committed);
    if (committed) {
      return ((IntIdentity)pm.getObjectId(masterKey)).getKey();
    } else {
      throw new MetaException("Failed to add master key.");
    }
  }

  @Override
  public void updateMasterKey(Integer id, String key) throws NoSuchObjectException, MetaException {
    LOG.debug("Begin executing updateMasterKey");
    boolean committed = false;
    Query query = null;
    MMasterKey masterKey;
    try {
      openTransaction();
      query = pm.newQuery(MMasterKey.class, "keyId == id");
      query.declareParameters("java.lang.Integer id");
      query.setUnique(true);
      masterKey = (MMasterKey) query.execute(id);
      if (null != masterKey) {
        masterKey.setMasterKey(key);
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, query);
    }
    LOG.debug("Done executing updateMasterKey with status : {}", committed);
    if (null == masterKey) {
      throw new NoSuchObjectException("No key found with keyId: " + id);
    }
    if (!committed) {
      throw new MetaException("Though key is found, failed to update it. " + id);
    }
  }

  @Override
  public boolean removeMasterKey(Integer id) {
    LOG.debug("Begin executing removeMasterKey");
    boolean success = false;
    Query query = null;
    MMasterKey masterKey;
    try {
      openTransaction();
      query = pm.newQuery(MMasterKey.class, "keyId == id");
      query.declareParameters("java.lang.Integer id");
      query.setUnique(true);
      masterKey = (MMasterKey) query.execute(id);
      if (null != masterKey) {
        pm.deletePersistent(masterKey);
      }
      success = commitTransaction();
    } finally {
      rollbackAndCleanup(success, query);
    }
    LOG.debug("Done executing removeMasterKey with status : {}", success);
    return (null != masterKey) && success;
  }

  @Override
  public String[] getMasterKeys() {
    LOG.debug("Begin executing getMasterKeys");
    boolean committed = false;
    Query query = null;
    List<MMasterKey> keys;
    try {
      openTransaction();
      query = pm.newQuery(MMasterKey.class);
      keys = (List<MMasterKey>) query.execute();
      pm.retrieveAll(keys);
      committed = commitTransaction();

      String[] masterKeys = new String[keys.size()];
      for (int i = 0; i < keys.size(); i++) {
        masterKeys[i] = keys.get(i).getMasterKey();
      }
      return masterKeys;
    } finally {
      LOG.debug("Done executing getMasterKeys with status : {}", committed);
      rollbackAndCleanup(committed, query);
    }
  }

  // compare hive version and metastore version
  @Override
  public void verifySchema() throws MetaException {
    // If the schema version is already checked, then go ahead and use this metastore
    if (isSchemaVerified.get()) {
      return;
    }
    checkSchema();
  }

  public static void setSchemaVerified(boolean val) {
    isSchemaVerified.set(val);
  }

  private synchronized void checkSchema() throws MetaException {
    // recheck if it got verified by another thread while we were waiting
    if (isSchemaVerified.get()) {
      return;
    }

    boolean strictValidation = MetastoreConf.getBoolVar(getConf(), ConfVars.SCHEMA_VERIFICATION);
    // read the schema version stored in metastore db
    String dbSchemaVer = getMetaStoreSchemaVersion();
    // version of schema for this version of hive
    IMetaStoreSchemaInfo metastoreSchemaInfo = MetaStoreSchemaInfoFactory.get(getConf());
    String hiveSchemaVer = metastoreSchemaInfo.getHiveSchemaVersion();

    String user = StringUtils.defaultString(System.getenv("USER"), "UNKNOWN");
    String hostName = "UNKNOWN";
    try {
      hostName = InetAddress.getLocalHost().getHostAddress();
    } catch (IOException e) {
      LOG.debug("Fail to get the address of the local host", e);
    }
    if (dbSchemaVer == null) {
      if (strictValidation) {
        throw new MetaException("Version information not found in metastore.");
      } else {
        LOG.warn("Version information not found in metastore. {} is not " +
          "enabled so recording the schema version {}", ConfVars.SCHEMA_VERIFICATION,
            hiveSchemaVer);
        setMetaStoreSchemaVersion(hiveSchemaVer, "Set by MetaStore " + user + "@" + hostName);
      }
    } else {
      if (metastoreSchemaInfo.isVersionCompatible(hiveSchemaVer, dbSchemaVer)) {
        LOG.debug("Found expected HMS version of {}", dbSchemaVer);
      } else {
        // metastore schema version is different than Hive distribution needs
        if (strictValidation) {
          throw new MetaException("Hive Schema version " + hiveSchemaVer +
              " does not match metastore's schema version " + dbSchemaVer +
              " Metastore is not upgraded or corrupt");
        } else {
          LOG.error("Version information found in metastore differs {} " +
              "from expected schema version {}. Schema verification is disabled {}",
              dbSchemaVer, hiveSchemaVer, ConfVars.SCHEMA_VERIFICATION);
          setMetaStoreSchemaVersion(hiveSchemaVer, "Set by MetaStore " + user + "@" + hostName);
        }
      }
    }
    isSchemaVerified.set(true);
  }

  // load the schema version stored in metastore db
  @Override
  public String getMetaStoreSchemaVersion() throws MetaException {

    MVersionTable mSchemaVer;
    try {
      mSchemaVer = getMSchemaVersion();
    } catch (NoSuchObjectException e) {
      return null;
    }
    return mSchemaVer.getSchemaVersion();
  }

  private MVersionTable getMSchemaVersion() throws NoSuchObjectException, MetaException {
    boolean committed = false;
    Query query = null;
    List<MVersionTable> mVerTables = new ArrayList<>();
    try {
      openTransaction();
      query = pm.newQuery(MVersionTable.class);
      try {
        mVerTables = (List<MVersionTable>) query.execute();
        pm.retrieveAll(mVerTables);
      } catch (JDODataStoreException e) {
        if (e.getCause() instanceof MissingTableException) {
          throw new MetaException("Version table not found. " + "The metastore is not upgraded to "
              + MetaStoreSchemaInfoFactory.get(getConf()).getHiveSchemaVersion());
        } else {
          throw e;
        }
      }
      committed = commitTransaction();
      if (mVerTables.isEmpty()) {
        throw new NoSuchObjectException("No matching version found");
      }
      if (mVerTables.size() > 1) {
        String msg = "Metastore contains multiple versions (" + mVerTables.size() + ") ";
        for (MVersionTable version : mVerTables) {
          msg +=
              "[ version = " + version.getSchemaVersion() + ", comment = "
                  + version.getVersionComment() + " ] ";
        }
        throw new MetaException(msg.trim());
      }
      return mVerTables.get(0);
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public void setMetaStoreSchemaVersion(String schemaVersion, String comment) throws MetaException {
    MVersionTable mSchemaVer;
    boolean commited = false;
    boolean recordVersion =
      MetastoreConf.getBoolVar(getConf(), ConfVars.SCHEMA_VERIFICATION_RECORD_VERSION);
    if (!recordVersion) {
      LOG.warn("setMetaStoreSchemaVersion called but recording version is disabled: " +
        "version = {}, comment = {}", schemaVersion, comment);
      return;
    }
    LOG.warn("Setting metastore schema version in db to {}", schemaVersion);

    try {
      mSchemaVer = getMSchemaVersion();
    } catch (NoSuchObjectException e) {
      // if the version doesn't exist, then create it
      mSchemaVer = new MVersionTable();
    }

    mSchemaVer.setSchemaVersion(schemaVersion);
    mSchemaVer.setVersionComment(comment);
    try {
      openTransaction();
      pm.makePersistent(mSchemaVer);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, null);
    }
  }

  private void debugLog(final String message) {
    if (LOG.isDebugEnabled()) {
      if (LOG.isTraceEnabled()) {
        LOG.debug("{}", message, new Exception("Debug Dump Stack Trace (Not an Exception)"));
      } else {
        LOG.debug("{}", message);
      }
    }
  }

  private Function convertToFunction(MFunction mfunc) {
    if (mfunc == null) {
      return null;
    }

    Function func = new Function(mfunc.getFunctionName(),
        mfunc.getDatabase().getName(),
        mfunc.getClassName(),
        mfunc.getOwnerName(),
        PrincipalType.valueOf(mfunc.getOwnerType()),
        mfunc.getCreateTime(),
        FunctionType.findByValue(mfunc.getFunctionType()),
        convertToResourceUriList(mfunc.getResourceUris()));
    func.setCatName(mfunc.getDatabase().getCatalogName());
    return func;
  }

  private List<Function> convertToFunctions(List<MFunction> mfuncs) {
    if (mfuncs == null) {
      return null;
    }
    List<Function> functions = new ArrayList<>();
    for (MFunction mfunc : mfuncs) {
      functions.add(convertToFunction(mfunc));
    }
    return functions;
  }

  private MFunction convertToMFunction(Function func) throws InvalidObjectException {
    if (func == null) {
      return null;
    }

    MDatabase mdb = null;
    String catName = func.isSetCatName() ? func.getCatName() : getDefaultCatalog(conf);
    try {
      mdb = getMDatabase(catName, func.getDbName());
    } catch (NoSuchObjectException e) {
      LOG.error("Database does not exist", e);
      throw new InvalidObjectException("Database " + func.getDbName() + " doesn't exist.");
    }

    return new MFunction(normalizeIdentifier(func.getFunctionName()),
        mdb,
        func.getClassName(),
        func.getOwnerName(),
        func.getOwnerType().name(),
        func.getCreateTime(),
        func.getFunctionType().getValue(),
        convertToMResourceUriList(func.getResourceUris()));
  }

  private List<ResourceUri> convertToResourceUriList(List<MResourceUri> mresourceUriList) {
    List<ResourceUri> resourceUriList = null;
    if (mresourceUriList != null) {
      resourceUriList = new ArrayList<>(mresourceUriList.size());
      for (MResourceUri mres : mresourceUriList) {
        resourceUriList.add(
            new ResourceUri(ResourceType.findByValue(mres.getResourceType()), mres.getUri()));
      }
    }
    return resourceUriList;
  }

  private List<MResourceUri> convertToMResourceUriList(List<ResourceUri> resourceUriList) {
    List<MResourceUri> mresourceUriList = null;
    if (resourceUriList != null) {
      mresourceUriList = new ArrayList<>(resourceUriList.size());
      for (ResourceUri res : resourceUriList) {
        mresourceUriList.add(new MResourceUri(res.getResourceType().getValue(), res.getUri()));
      }
    }
    return mresourceUriList;
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MFunction mfunc = convertToMFunction(func);
      pm.makePersistent(mfunc);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      String newFuncCat = newFunction.isSetCatName() ? newFunction.getCatName() :
          getDefaultCatalog(conf);
      if (!newFuncCat.equalsIgnoreCase(catName)) {
        throw new InvalidObjectException("You cannot move a function between catalogs");
      }
      openTransaction();
      catName = normalizeIdentifier(catName);
      funcName = normalizeIdentifier(funcName);
      dbName = normalizeIdentifier(dbName);
      MFunction newf = convertToMFunction(newFunction);
      if (newf == null) {
        throw new InvalidObjectException("new function is invalid");
      }

      MFunction oldf = getMFunction(catName, dbName, funcName);
      if (oldf == null) {
        throw new MetaException("function " + funcName + " doesn't exist");
      }

      // For now only alter name, owner, class name, type
      oldf.setFunctionName(normalizeIdentifier(newf.getFunctionName()));
      oldf.setDatabase(newf.getDatabase());
      oldf.setOwnerName(newf.getOwnerName());
      oldf.setOwnerType(newf.getOwnerType());
      oldf.setClassName(newf.getClassName());
      oldf.setFunctionType(newf.getFunctionType());

      // commit the changes
      success = commitTransaction();
    } finally {
      rollbackAndCleanup(success, null);
    }
  }

  @Override
  public void dropFunction(String catName, String dbName, String funcName) throws MetaException,
  NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean success = false;
    try {
      openTransaction();
      MFunction mfunc = getMFunction(catName, dbName, funcName);
      pm.retrieve(mfunc);
      if (mfunc != null) {
        // TODO: When function privileges are implemented, they should be deleted here.
        pm.deletePersistentAll(mfunc);
      }
      success = commitTransaction();
    } finally {
      rollbackAndCleanup(success, null);
    }
  }

  private MFunction getMFunction(String catName, String db, String function) {
    MFunction mfunc = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      catName = normalizeIdentifier(catName);
      db = normalizeIdentifier(db);
      function = normalizeIdentifier(function);
      query = pm.newQuery(MFunction.class,
          "functionName == function && database.name == db && database.catalogName == catName");
      query.declareParameters("java.lang.String function, java.lang.String db, java.lang.String catName");
      query.setUnique(true);
      mfunc = (MFunction) query.execute(function, db, catName);
      pm.retrieve(mfunc);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return mfunc;
  }

  @Override
  public Function getFunction(String catName, String dbName, String funcName) throws MetaException {
    boolean commited = false;
    Function func = null;
    Query query = null;
    try {
      openTransaction();
      func = convertToFunction(getMFunction(catName, dbName, funcName));
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return func;
  }

  @Override
  public List<Function> getAllFunctions(String catName) throws MetaException {
    try {
      return getFunctionsInternal(catName);
    } catch (NoSuchObjectException e) {
      throw new RuntimeException(e);
    }
  }

  protected List<Function> getFunctionsInternal(String catalogName)
      throws MetaException, NoSuchObjectException {
    return new GetListHelper<Function>(catalogName, "", "", true, true) {
      @Override
      protected List<Function> getSqlResult(GetHelper<List<Function>> ctx) throws MetaException {
        return directSql.getFunctions(catalogName);
      }
      @Override
      protected List<Function> getJdoResult(GetHelper<List<Function>> ctx) throws MetaException {
        try {
          return getAllFunctionsViaJDO(catalogName);
        } catch (Exception e) {
          LOG.error("Failed to convert to functions", e);
          throw new MetaException(e.getMessage());
        }
      }
    }.run(false);
  }

  private List<Function> getAllFunctionsViaJDO (String catName) {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      catName = normalizeIdentifier(catName);
      query = pm.newQuery(MFunction.class, "database.catalogName == catName");
      query.declareParameters("java.lang.String catName");
      List<MFunction> allFunctions = (List<MFunction>) query.execute(catName);
      pm.retrieveAll(allFunctions);
      commited = commitTransaction();
      return convertToFunctions(allFunctions);
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public <T> List<T> getFunctionsRequest(String catName, String dbName, String pattern,
      boolean isReturnNames) throws MetaException {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      dbName = normalizeIdentifier(dbName);
      // Take the pattern and split it on the | to get all the composing
      // patterns
      List<String> parameterVals = new ArrayList<>();
      StringBuilder filterBuilder = new StringBuilder();
      appendSimpleCondition(filterBuilder, "database.name", new String[] { dbName }, parameterVals);
      appendSimpleCondition(filterBuilder, "database.catalogName", new String[] {catName}, parameterVals);
      if(pattern != null) {
        appendPatternCondition(filterBuilder, "functionName", pattern, parameterVals);
      }
      query = pm.newQuery(MFunction.class, filterBuilder.toString());
      if (isReturnNames) {
        query.setResult("functionName");
      }
      query.setOrdering("functionName ascending");
      List<T> result;
      if (!isReturnNames) {
        List<MFunction> functionList =
            (List<MFunction>) query.executeWithArray(parameterVals.toArray(new String[0]));
        pm.retrieveAll(functionList);
        result = (List<T>) convertToFunctions(functionList);
      } else {
        List<String> functionList = (List<String>) query.executeWithArray(parameterVals.toArray(new String[0]));
        result = (List<T>) new ArrayList<>(functionList);
      }
      commited = commitTransaction();
      return result;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void createOrUpdateStoredProcedure(StoredProcedure proc) throws NoSuchObjectException, MetaException {
    boolean committed = false;
    MStoredProc mProc;
    Query query = null;
    String catName = normalizeIdentifier(proc.getCatName());
    String dbName = normalizeIdentifier(proc.getDbName());
    MDatabase db = getMDatabase(catName, dbName);
    try {
      openTransaction();
      query = storedProcQuery();
      mProc = (MStoredProc) query.execute(proc.getName(), dbName, catName);
      pm.retrieve(mProc);
      if (mProc == null) { // create new
        mProc = new MStoredProc();
        MStoredProc.populate(mProc, proc, db);
        pm.makePersistent(mProc);
      } else { // update existing
        MStoredProc.populate(mProc, proc, db);
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public StoredProcedure getStoredProcedure(String catName, String db, String name) throws MetaException {
    MStoredProc proc = getMStoredProcedure(catName, db, name);
    return proc == null ? null : convertToStoredProc(catName, proc);
  }

  private MStoredProc getMStoredProcedure(String catName, String db, String procName) {
    MStoredProc proc;
    catName = normalizeIdentifier(catName);
    db = normalizeIdentifier(db);
    boolean committed = false;
    Query query = null;
    try {
      openTransaction();
      query = storedProcQuery();
      proc = (MStoredProc) query.execute(procName, db, catName);
      pm.retrieve(proc);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, query);
    }
    return proc;
  }

  private Query storedProcQuery() {
    Query query = pm.newQuery(MStoredProc.class,
            "name == procName && database.name == db && database.catalogName == catName");
    query.declareParameters("java.lang.String procName, java.lang.String db, java.lang.String catName");
    query.setUnique(true);
    return query;
  }

  private Query findPackageQuery() {
    Query query = pm.newQuery(MPackage.class,
            "name == packageName && database.name == db && database.catalogName == catName");
    query.declareParameters("java.lang.String packageName, java.lang.String db, java.lang.String catName");
    query.setUnique(true);
    return query;
  }

  private StoredProcedure convertToStoredProc(String catName, MStoredProc proc) {
    return new StoredProcedure(
            proc.getName(),
            proc.getDatabase().getName(),
            catName,
            proc.getOwner(),
            proc.getSource());
  }

  @Override
  public void dropStoredProcedure(String catName, String dbName, String funcName) throws MetaException {
    boolean success = false;
    try {
      openTransaction();
      MStoredProc proc = getMStoredProcedure(catName, dbName, funcName);
      pm.retrieve(proc);
      if (proc != null) {
        pm.deletePersistentAll(proc);
      }
      success = commitTransaction();
    } finally {
      rollbackAndCleanup(success, null);
    }
  }

  @Override
  public List<String> getAllStoredProcedures(ListStoredProcedureRequest request) {
    boolean committed = false;
    Query query = null;
    final String catName = normalizeIdentifier(request.getCatName());
    final String dbName = request.isSetDbName() ? normalizeIdentifier(request.getDbName()) : null;
    List<String> names;
    try {
      openTransaction();
      if (request.isSetDbName()) {
        query = pm.newQuery("SELECT name FROM org.apache.hadoop.hive.metastore.model.MStoredProc " +
                "WHERE database.catalogName == catName && database.name == db");
        query.declareParameters("java.lang.String catName, java.lang.String db");
        query.setResult("name");
        names = new ArrayList<>((Collection<String>) query.execute(catName, dbName));
      } else {
        query = pm.newQuery("SELECT name FROM org.apache.hadoop.hive.metastore.model.MStoredProc " +
                "WHERE database.catalogName == catName");
        query.declareParameters("java.lang.String catName");
        query.setResult("name");
        names = new ArrayList<>((Collection<String>) query.execute(catName));
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, query);
    }
    return names;
  }

  @Override
  public void addPackage(AddPackageRequest request) throws NoSuchObjectException, MetaException {
    boolean committed = false;
    MPackage mPkg;
    Query query = null;
    String catName = normalizeIdentifier(request.getCatName());
    String dbName = normalizeIdentifier(request.getDbName());
    MDatabase db = getMDatabase(catName, dbName);
    try {
      openTransaction();
      query = findPackageQuery();
      mPkg = (MPackage) query.execute(request.getPackageName(), dbName, catName);
      pm.retrieve(mPkg);
      if (mPkg == null) { // create new
        mPkg = new MPackage();
        MPackage.populate(mPkg, db, request);
        pm.makePersistent(mPkg);
      } else { // update existing
        MPackage.populate(mPkg, db, request);
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public Package findPackage(GetPackageRequest request) {
    MPackage mPkg = findMPackage(request.getCatName(), request.getDbName(), request.getPackageName());
    return mPkg == null ? null : mPkg.toPackage();
  }

  public List<String> listPackages(ListPackageRequest request) {
    boolean committed = false;
    Query query = null;
    final String catName = normalizeIdentifier(request.getCatName());
    final String dbName = request.isSetDbName() ? normalizeIdentifier(request.getDbName()) : null;
    List<String> names;
    try {
      openTransaction();
      if (request.isSetDbName()) {
        query = pm.newQuery("SELECT name FROM org.apache.hadoop.hive.metastore.model.MPackage " +
                "WHERE database.catalogName == catName && database.name == db");
        query.declareParameters("java.lang.String catName, java.lang.String db");
        query.setResult("name");
        names = new ArrayList<>((Collection<String>) query.execute(catName, dbName));
      } else {
        query = pm.newQuery("SELECT name FROM org.apache.hadoop.hive.metastore.model.MPackage " +
                "WHERE database.catalogName == catName");
        query.declareParameters("java.lang.String catName");
        query.setResult("name");
        names = new ArrayList<>((Collection<String>) query.execute(catName));
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, query);
    }
    return names;
  }

  public void dropPackage(DropPackageRequest request) {
    boolean success = false;
    try {
      openTransaction();
      MPackage proc = findMPackage(request.getCatName(), request.getDbName(), request.getPackageName());
      pm.retrieve(proc);
      if (proc != null) {
        pm.deletePersistentAll(proc);
      }
      success = commitTransaction();
    } finally {
      rollbackAndCleanup(success, null);
    }
  }

  private MPackage findMPackage(String catName, String db, String packageName) {
    MPackage pkg;
    catName = normalizeIdentifier(catName);
    db = normalizeIdentifier(db);
    boolean committed = false;
    Query query = null;
    try {
      openTransaction();
      query = findPackageQuery();
      pkg = (MPackage) query.execute(packageName, db, catName);
      pm.retrieve(pkg);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, query);
    }
    return pkg;
  }

  private void executePlainSQL(String sql,
      boolean atLeastOneRecord,
      Consumer<Exception> exceptionConsumer)
      throws SQLException, MetaException {
    String s = dbType.getPrepareTxnStmt();
    assert pm.currentTransaction().isActive();
    JDOConnection jdoConn = pm.getDataStoreConnection();
    Connection conn = (Connection) jdoConn.getNativeConnection();
    try (Statement statement = conn.createStatement()) {
      if (s != null) {
        statement.execute(s);
      }
      try {
        statement.execute(sql);
        try (ResultSet rs = statement.getResultSet()) {
          // sqlserver needs rs.next for validating the s4u nowait
          if (atLeastOneRecord && !rs.next()) {
            throw new MetaException("At least one record but none is returned from the query: " + sql);
          }
        }
      } catch (SQLException e) {
        if (exceptionConsumer != null) {
          exceptionConsumer.accept(e);
        } else {
          throw e;
        }
      }
    } finally {
      jdoConn.close();
    }
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request) throws MetaException {
    try {
      return getPrimaryKeysInternal(request.getCatName(),
          request.getDb_name(),request.getTbl_name());
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLPrimaryKey> getPrimaryKeysInternal(final String catName,
                                                     final String dbNameInput,
                                                     final String tblNameInput)
  throws MetaException, NoSuchObjectException {
    final String dbName = dbNameInput != null ? normalizeIdentifier(dbNameInput) : null;
    final String tblName = normalizeIdentifier(tblNameInput);
    return new GetListHelper<SQLPrimaryKey>(catName, dbName, tblName, true, true) {

      @Override
      protected List<SQLPrimaryKey> getSqlResult(GetHelper<List<SQLPrimaryKey>> ctx) throws MetaException {
        return directSql.getPrimaryKeys(catName, dbName, tblName);
      }

      @Override
      protected List<SQLPrimaryKey> getJdoResult(
        GetHelper<List<SQLPrimaryKey>> ctx) throws MetaException, NoSuchObjectException {
        return getPrimaryKeysViaJdo(catName, dbName, tblName);
      }
    }.run(false);
  }

  private List<SQLPrimaryKey> getPrimaryKeysViaJdo(String catName, String dbName, String tblName) {
    boolean commited = false;
    List<SQLPrimaryKey> primaryKeys = null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
        + " parentTable.database.catalogName == cat_name &&"
        + " constraintType == MConstraint.PRIMARY_KEY_CONSTRAINT");
      query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, " +
          "java.lang.String cat_name");
      Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
      pm.retrieveAll(constraints);
      primaryKeys = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currPK = (MConstraint) i.next();
        List<MFieldSchema> cols = currPK.getParentColumn() != null ?
            currPK.getParentColumn().getCols() : currPK.getParentTable().getPartitionKeys();
        int enableValidateRely = currPK.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        SQLPrimaryKey keyCol = new SQLPrimaryKey(dbName,
            tblName,
            cols.get(currPK.getParentIntegerIndex()).getName(),
            currPK.getPosition(),
            currPK.getConstraintName(), enable, validate, rely);
        keyCol.setCatName(catName);
        primaryKeys.add(keyCol);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return primaryKeys;
  }

  private String getPrimaryKeyConstraintName(String catName, String dbName, String tblName) {
    boolean commited = false;
    String ret = null;
    Query query = null;

    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
        + " parentTable.database.catalogName == catName &&"
        + " constraintType == MConstraint.PRIMARY_KEY_CONSTRAINT");
      query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, " +
          "java.lang.String catName");
      Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
      pm.retrieveAll(constraints);
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currPK = (MConstraint) i.next();
        ret = currPK.getConstraintName();
        break;
      }
      commited = commitTransaction();
     } finally {
        rollbackAndCleanup(commited, query);
     }
     return ret;
   }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request) throws MetaException {
    try {
      return getForeignKeysInternal(request.getCatName(),
          request.getParent_db_name(), request.getParent_tbl_name() ,
          request.getForeign_db_name(),request.getForeign_tbl_name(), true,
          true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLForeignKey> getForeignKeysInternal(
      final String catName, final String parent_db_name_input, final String parent_tbl_name_input,
      final String foreign_db_name_input, final String foreign_tbl_name_input, boolean allowSql,
      boolean allowJdo) throws MetaException, NoSuchObjectException {
    final String parent_db_name = (parent_db_name_input != null) ? normalizeIdentifier(parent_db_name_input) : null;
    final String parent_tbl_name = (parent_tbl_name_input != null) ? normalizeIdentifier(parent_tbl_name_input) : null;
    final String foreign_db_name = (foreign_db_name_input != null) ? normalizeIdentifier(foreign_db_name_input) : null;
    final String foreign_tbl_name = (foreign_tbl_name_input != null)
                                    ? normalizeIdentifier(foreign_tbl_name_input) : null;
    final String db_name;
    final String tbl_name;
    if (foreign_tbl_name == null) {
      // The FK table name might be null if we are retrieving the constraint from the PK side
      db_name = parent_db_name;
      tbl_name = parent_tbl_name;
    } else {
      db_name = foreign_db_name;
      tbl_name = foreign_tbl_name;
    }
    return new GetListHelper<SQLForeignKey>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLForeignKey> getSqlResult(GetHelper<List<SQLForeignKey>> ctx) throws MetaException {
        return directSql.getForeignKeys(catName, parent_db_name,
          parent_tbl_name, foreign_db_name, foreign_tbl_name);
      }

      @Override
      protected List<SQLForeignKey> getJdoResult(
        GetHelper<List<SQLForeignKey>> ctx) throws MetaException, NoSuchObjectException {
        return getForeignKeysViaJdo(catName, parent_db_name,
          parent_tbl_name, foreign_db_name, foreign_tbl_name);
      }
    }.run(false);
  }

  private List<SQLForeignKey> getForeignKeysViaJdo(String catName, String parentDbName,
      String parentTblName, String foreignDbName, String foreignTblName) {
    boolean commited = false;
    List<SQLForeignKey> foreignKeys = null;
    Collection<?> constraints = null;
    Query query = null;
    Map<String, String> tblToConstraint = new HashMap<>();
    try {
      openTransaction();
      String queryText =
          " parentTable.database.catalogName == catName1 &&" + "childTable.database.catalogName == catName2 && " + (
              parentTblName != null ? "parentTable.tableName == parent_tbl_name && " : "") + (
              parentDbName != null ? " parentTable.database.name == parent_db_name && " : "") + (
              foreignTblName != null ? " childTable.tableName == foreign_tbl_name && " : "") + (
              foreignDbName != null ? " childTable.database.name == foreign_db_name && " : "")
              + " constraintType == MConstraint.FOREIGN_KEY_CONSTRAINT";
      queryText = queryText.trim();
      query = pm.newQuery(MConstraint.class, queryText);
      String paramText = "java.lang.String catName1, java.lang.String catName2" + (
          parentTblName == null ? "" : ", java.lang.String parent_tbl_name") + (
          parentDbName == null ? "" : " , java.lang.String parent_db_name") + (
          foreignTblName == null ? "" : ", java.lang.String foreign_tbl_name") + (
          foreignDbName == null ? "" : " , java.lang.String foreign_db_name");
      query.declareParameters(paramText);
      List<String> params = new ArrayList<>();
      params.add(catName);
      params.add(catName); // This is not a mistake, catName is in the where clause twice
      if (parentTblName != null) {
        params.add(parentTblName);
      }
      if (parentDbName != null) {
        params.add(parentDbName);
      }
      if (foreignTblName != null) {
        params.add(foreignTblName);
      }
      if (foreignDbName != null) {
        params.add(foreignDbName);
      }
      constraints = (Collection<?>) query.executeWithArray(params.toArray(new String[0]));

      pm.retrieveAll(constraints);
      foreignKeys = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currPKFK = (MConstraint) i.next();
        List<MFieldSchema> parentCols = currPKFK.getParentColumn() != null ?
            currPKFK.getParentColumn().getCols() : currPKFK.getParentTable().getPartitionKeys();
        List<MFieldSchema> childCols = currPKFK.getChildColumn() != null ?
            currPKFK.getChildColumn().getCols() : currPKFK.getChildTable().getPartitionKeys();
        int enableValidateRely = currPKFK.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        String consolidatedtblName =
          currPKFK.getParentTable().getDatabase().getName() + "." +
          currPKFK.getParentTable().getTableName();
        String pkName;
        if (tblToConstraint.containsKey(consolidatedtblName)) {
          pkName = tblToConstraint.get(consolidatedtblName);
        } else {
          pkName = getPrimaryKeyConstraintName(currPKFK.getParentTable().getDatabase().getCatalogName(),
              currPKFK.getParentTable().getDatabase().getName(),
              currPKFK.getParentTable().getTableName());
          tblToConstraint.put(consolidatedtblName, pkName);
        }
        SQLForeignKey fk = new SQLForeignKey(
          currPKFK.getParentTable().getDatabase().getName(),
          currPKFK.getParentTable().getTableName(),
          parentCols.get(currPKFK.getParentIntegerIndex()).getName(),
          currPKFK.getChildTable().getDatabase().getName(),
          currPKFK.getChildTable().getTableName(),
          childCols.get(currPKFK.getChildIntegerIndex()).getName(),
          currPKFK.getPosition(),
          currPKFK.getUpdateRule(),
          currPKFK.getDeleteRule(),
          currPKFK.getConstraintName(), pkName, enable, validate, rely);
        fk.setCatName(catName);
        foreignKeys.add(fk);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return foreignKeys;
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request) throws MetaException {
    try {
      return getUniqueConstraintsInternal(request.getCatName(),
         request.getDb_name(),request.getTbl_name(), true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLUniqueConstraint> getUniqueConstraintsInternal(
      String catNameInput, final String db_name_input, final String tbl_name_input,
      boolean allowSql, boolean allowJdo) throws MetaException, NoSuchObjectException {
    final String catName = normalizeIdentifier(catNameInput);
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new GetListHelper<SQLUniqueConstraint>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLUniqueConstraint> getSqlResult(GetHelper<List<SQLUniqueConstraint>> ctx)
              throws MetaException {
        return directSql.getUniqueConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLUniqueConstraint> getJdoResult(GetHelper<List<SQLUniqueConstraint>> ctx)
              throws MetaException, NoSuchObjectException {
        return getUniqueConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  private List<SQLUniqueConstraint> getUniqueConstraintsViaJdo(String catName, String dbName, String tblName) {
    boolean commited = false;
    List<SQLUniqueConstraint> uniqueConstraints = null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name && parentTable.database.catalogName == catName &&"
        + " constraintType == MConstraint.UNIQUE_CONSTRAINT");
      query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
      Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
      pm.retrieveAll(constraints);
      uniqueConstraints = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currConstraint = (MConstraint) i.next();
        List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
            currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
        int enableValidateRely = currConstraint.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        uniqueConstraints.add(new SQLUniqueConstraint(catName, dbName, tblName,
            cols.get(currConstraint.getParentIntegerIndex()).getName(), currConstraint.getPosition(),
            currConstraint.getConstraintName(), enable, validate, rely));
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return uniqueConstraints;
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest request) throws MetaException {
    try {
      return getNotNullConstraintsInternal(request.getCatName(),request.getDb_name(),request.getTbl_name(), true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest request) throws MetaException {
    try {
      return getDefaultConstraintsInternal(request.getCatName(),request.getDb_name(),request.getTbl_name(), true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest request) throws MetaException {
    try {
      return getCheckConstraintsInternal(request.getCatName(),request.getDb_name(),request.getTbl_name(), true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLDefaultConstraint> getDefaultConstraintsInternal(
      String catName, final String db_name_input, final String tbl_name_input, boolean allowSql,
      boolean allowJdo) throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new GetListHelper<SQLDefaultConstraint>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLDefaultConstraint> getSqlResult(GetHelper<List<SQLDefaultConstraint>> ctx)
              throws MetaException {
        return directSql.getDefaultConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLDefaultConstraint> getJdoResult(GetHelper<List<SQLDefaultConstraint>> ctx)
              throws MetaException, NoSuchObjectException {
        return getDefaultConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  protected List<SQLCheckConstraint> getCheckConstraintsInternal(String catName, final String db_name_input,
                                                                     final String tbl_name_input, boolean allowSql,
                                                                 boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new GetListHelper<SQLCheckConstraint>(normalizeIdentifier(catName), db_name, tbl_name,
        allowSql, allowJdo) {

      @Override
      protected List<SQLCheckConstraint> getSqlResult(GetHelper<List<SQLCheckConstraint>> ctx)
          throws MetaException {
        return directSql.getCheckConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLCheckConstraint> getJdoResult(GetHelper<List<SQLCheckConstraint>> ctx)
          throws MetaException, NoSuchObjectException {
        return getCheckConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  private List<SQLCheckConstraint> getCheckConstraintsViaJdo(String catName, String dbName, String tblName) {
    boolean commited = false;
    List<SQLCheckConstraint> checkConstraints= null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
          "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
          + " parentTable.database.catalogName == catName && constraintType == MConstraint.CHECK_CONSTRAINT");
      query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
      Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
      pm.retrieveAll(constraints);
      checkConstraints = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currConstraint = (MConstraint) i.next();
        List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
            currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
        int enableValidateRely = currConstraint.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        checkConstraints.add(new SQLCheckConstraint(catName, dbName, tblName,
                                                        cols.get(currConstraint.getParentIntegerIndex()).getName(),
                                                        currConstraint.getDefaultValue(),
                                                    currConstraint.getConstraintName(), enable, validate, rely));
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return checkConstraints;
  }

  private List<SQLDefaultConstraint> getDefaultConstraintsViaJdo(String catName, String dbName, String tblName) {
    boolean commited = false;
    List<SQLDefaultConstraint> defaultConstraints= null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
            + " parentTable.database.catalogName == catName &&"
        + " constraintType == MConstraint.DEFAULT_CONSTRAINT");
      query.declareParameters(
          "java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
      Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
      pm.retrieveAll(constraints);
      defaultConstraints = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currConstraint = (MConstraint) i.next();
        List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
            currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
        int enableValidateRely = currConstraint.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        defaultConstraints.add(new SQLDefaultConstraint(catName, dbName, tblName,
            cols.get(currConstraint.getParentIntegerIndex()).getName(), currConstraint.getDefaultValue(),
            currConstraint.getConstraintName(), enable, validate, rely));
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return defaultConstraints;
  }

  protected List<SQLNotNullConstraint> getNotNullConstraintsInternal(String catName, final String db_name_input,
                                                                     final String tbl_name_input, boolean allowSql, boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new GetListHelper<SQLNotNullConstraint>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLNotNullConstraint> getSqlResult(GetHelper<List<SQLNotNullConstraint>> ctx)
          throws MetaException {
        return directSql.getNotNullConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLNotNullConstraint> getJdoResult(GetHelper<List<SQLNotNullConstraint>> ctx)
          throws MetaException, NoSuchObjectException {
        return getNotNullConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  private List<SQLNotNullConstraint> getNotNullConstraintsViaJdo(String catName, String dbName, String tblName) {
    boolean commited = false;
    List<SQLNotNullConstraint> notNullConstraints = null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
          "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
              + " parentTable.database.catalogName == catName && constraintType == MConstraint.NOT_NULL_CONSTRAINT");
      query.declareParameters(
          "java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
      Collection<?> constraints = (Collection<?>) query.execute(tblName, dbName, catName);
      pm.retrieveAll(constraints);
      notNullConstraints = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currConstraint = (MConstraint) i.next();
        List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
            currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
        int enableValidateRely = currConstraint.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        notNullConstraints.add(new SQLNotNullConstraint(catName, dbName,
            tblName,
            cols.get(currConstraint.getParentIntegerIndex()).getName(),
            currConstraint.getConstraintName(), enable, validate, rely));
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return notNullConstraints;
  }

  /**
   * Api to fetch all constraints at once
   * @param request request object
   * @return all table constraints
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  @Override
  public SQLAllTableConstraints getAllTableConstraints(AllTableConstraintsRequest request)
      throws MetaException, NoSuchObjectException {
    String catName = request.getCatName();
    String dbName = request.getDbName();
    String tblName = request.getTblName();
    debugLog("Get all table constraints for the table - " + catName + "." + dbName + "." + tblName
        + " in class ObjectStore.java");
    SQLAllTableConstraints sqlAllTableConstraints = new SQLAllTableConstraints();
    PrimaryKeysRequest primaryKeysRequest = new PrimaryKeysRequest(dbName, tblName);
    primaryKeysRequest.setCatName(catName);
    sqlAllTableConstraints.setPrimaryKeys(getPrimaryKeys(primaryKeysRequest));
    ForeignKeysRequest foreignKeysRequest =
        new ForeignKeysRequest(null, null, dbName, tblName);
    foreignKeysRequest.setCatName(catName);
    sqlAllTableConstraints.setForeignKeys(getForeignKeys(foreignKeysRequest));
    sqlAllTableConstraints.
        setUniqueConstraints(getUniqueConstraints(new UniqueConstraintsRequest(catName, dbName, tblName)));
    sqlAllTableConstraints.
        setDefaultConstraints(getDefaultConstraints(new DefaultConstraintsRequest(catName, dbName, tblName)));
    sqlAllTableConstraints.
        setCheckConstraints(getCheckConstraints(new CheckConstraintsRequest(catName, dbName, tblName)));
    sqlAllTableConstraints.
        setNotNullConstraints(getNotNullConstraints(new NotNullConstraintsRequest(catName, dbName, tblName)));
    return sqlAllTableConstraints;
  }

  @Override
  public void dropConstraint(String catName, String dbName, String tableName,
                             String constraintName, boolean missingOk)
      throws NoSuchObjectException {
    boolean success = false;
    try {
      openTransaction();

      List<MConstraint> tabConstraints =
          listAllTableConstraintsWithOptionalConstraintName(catName,  dbName, tableName, constraintName);
      if (CollectionUtils.isNotEmpty(tabConstraints)) {
        pm.deletePersistentAll(tabConstraints);
      } else if (!missingOk) {
        throw new NoSuchObjectException("The constraint: " + constraintName +
          " does not exist for the associated table: " + dbName + "." + tableName);
      }
      success = commitTransaction();
    } finally {
      rollbackAndCleanup(success, null);
    }
  }

  @Override
  public void createISchema(ISchema schema) throws AlreadyExistsException, MetaException,
      NoSuchObjectException {
    boolean committed = false;
    MISchema mSchema = convertToMISchema(schema);
    try {
      openTransaction();
      if (getMISchema(schema.getCatName(), schema.getDbName(), schema.getName()) != null) {
        throw new AlreadyExistsException("Schema with name " + schema.getDbName() + "." +
            schema.getName() + " already exists");
      }
      pm.makePersistent(mSchema);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public void alterISchema(ISchemaName schemaName, ISchema newSchema)
      throws NoSuchObjectException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MISchema oldMSchema = getMISchema(schemaName.getCatName(), schemaName.getDbName(), schemaName.getSchemaName());
      if (oldMSchema == null) {
        throw new NoSuchObjectException("Schema " + schemaName + " does not exist");
      }

      // Don't support changing name or type
      oldMSchema.setCompatibility(newSchema.getCompatibility().getValue());
      oldMSchema.setValidationLevel(newSchema.getValidationLevel().getValue());
      oldMSchema.setCanEvolve(newSchema.isCanEvolve());
      if (newSchema.isSetSchemaGroup()) {
        oldMSchema.setSchemaGroup(newSchema.getSchemaGroup());
      }
      if (newSchema.isSetDescription()) {
        oldMSchema.setDescription(newSchema.getDescription());
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public ISchema getISchema(ISchemaName schemaName) throws MetaException {
    boolean committed = false;
    try {
      openTransaction();
      ISchema schema = convertToISchema(getMISchema(schemaName.getCatName(), schemaName.getDbName(),
          schemaName.getSchemaName()));
      committed = commitTransaction();
      return schema;
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  private MISchema getMISchema(String catName, String dbName, String name) {
    try (QueryWrapper query = new QueryWrapper(pm.newQuery(MISchema.class,
        "name == schemaName && db.name == dbname && db.catalogName == cat"))) {
      name = normalizeIdentifier(name);
      dbName = normalizeIdentifier(dbName);
      catName = normalizeIdentifier(catName);
      query.declareParameters(
          "java.lang.String schemaName, java.lang.String dbname, java.lang.String cat");
      query.setUnique(true);
      MISchema mSchema = (MISchema)query.execute(name, dbName, catName);
      pm.retrieve(mSchema);
      return mSchema;
    }
  }

  @Override
  public void dropISchema(ISchemaName schemaName) throws NoSuchObjectException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MISchema mSchema = getMISchema(schemaName.getCatName(), schemaName.getDbName(), schemaName.getSchemaName());
      if (mSchema != null) {
        pm.deletePersistentAll(mSchema);
      } else {
        throw new NoSuchObjectException("Schema " + schemaName + " does not exist");
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public void addSchemaVersion(SchemaVersion schemaVersion)
      throws AlreadyExistsException, NoSuchObjectException, MetaException {
    boolean committed = false;
    MSchemaVersion mSchemaVersion = convertToMSchemaVersion(schemaVersion);
    try {
      openTransaction();
      // Make sure it doesn't already exist
      if (getMSchemaVersion(schemaVersion.getSchema().getCatName(), schemaVersion.getSchema().getDbName(),
          schemaVersion.getSchema().getSchemaName(), schemaVersion.getVersion()) != null) {
        throw new AlreadyExistsException("Schema name " + schemaVersion.getSchema() +
            " version " + schemaVersion.getVersion() + " already exists");
      }
      // Make sure the referenced Schema exists
      if (getMISchema(schemaVersion.getSchema().getCatName(), schemaVersion.getSchema().getDbName(),
          schemaVersion.getSchema().getSchemaName()) == null) {
        throw new NoSuchObjectException("Schema " + schemaVersion.getSchema() + " does not exist");
      }
      pm.makePersistent(mSchemaVersion);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public void alterSchemaVersion(SchemaVersionDescriptor version, SchemaVersion newVersion)
      throws NoSuchObjectException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MSchemaVersion oldMSchemaVersion = getMSchemaVersion(version.getSchema().getCatName(),
          version.getSchema().getDbName(), version.getSchema().getSchemaName(), version.getVersion());
      if (oldMSchemaVersion == null) {
        throw new NoSuchObjectException("No schema version " + version + " exists");
      }

      // We only support changing the SerDe mapping and the state.
      if (newVersion.isSetSerDe()) {
        oldMSchemaVersion.setSerDe(convertToMSerDeInfo(newVersion.getSerDe()));
      }
      if (newVersion.isSetState()) {
        oldMSchemaVersion.setState(newVersion.getState().getValue());
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public SchemaVersion getSchemaVersion(SchemaVersionDescriptor version) throws MetaException {
    boolean committed = false;
    try {
      openTransaction();
      SchemaVersion schemaVersion = convertToSchemaVersion(getMSchemaVersion(
          version.getSchema().getCatName(), version.getSchema().getDbName(),
          version.getSchema().getSchemaName(), version.getVersion()));
      committed = commitTransaction();
      return schemaVersion;
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  private MSchemaVersion getMSchemaVersion(String catName, String dbName, String schemaName, int version) {
    try (QueryWrapper query = new QueryWrapper(pm.newQuery(MSchemaVersion.class,
        "iSchema.name == schemaName && iSchema.db.name == dbName &&" +
            "iSchema.db.catalogName == cat && version == schemaVersion"))) {
      dbName = normalizeIdentifier(dbName);
      schemaName = normalizeIdentifier(schemaName);
      query.declareParameters( "java.lang.String schemaName, java.lang.String dbName," +
          "java.lang.String cat, java.lang.Integer schemaVersion");
      query.setUnique(true);
      MSchemaVersion mSchemaVersion =
          (MSchemaVersion)query.executeWithArray(schemaName, dbName, catName, version);
      pm.retrieve(mSchemaVersion);
      if (mSchemaVersion != null) {
        pm.retrieveAll(mSchemaVersion.getCols());
        if (mSchemaVersion.getSerDe() != null) {
          pm.retrieve(mSchemaVersion.getSerDe());
        }
      }
      return mSchemaVersion;
    }
  }

  @Override
  public SchemaVersion getLatestSchemaVersion(ISchemaName schemaName) throws MetaException {
    boolean committed = false;
    Query query = null;
    try {
      openTransaction();
      String name = normalizeIdentifier(schemaName.getSchemaName());
      String dbName = normalizeIdentifier(schemaName.getDbName());
      String catName = normalizeIdentifier(schemaName.getCatName());
      query = pm.newQuery(MSchemaVersion.class,
          "iSchema.name == schemaName && iSchema.db.name == dbName && iSchema.db.catalogName == cat");
      query.declareParameters("java.lang.String schemaName, java.lang.String dbName, " +
          "java.lang.String cat");
      query.setUnique(true);
      query.setOrdering("version descending");
      query.setRange(0, 1);
      MSchemaVersion mSchemaVersion = (MSchemaVersion)query.execute(name, dbName, catName);
      pm.retrieve(mSchemaVersion);
      if (mSchemaVersion != null) {
        pm.retrieveAll(mSchemaVersion.getCols());
        if (mSchemaVersion.getSerDe() != null) {
          pm.retrieve(mSchemaVersion.getSerDe());
        }
      }
      SchemaVersion version = mSchemaVersion == null ? null : convertToSchemaVersion(mSchemaVersion);
      committed = commitTransaction();
      return version;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public List<SchemaVersion> getAllSchemaVersion(ISchemaName schemaName) throws MetaException {
    boolean committed = false;
    Query query = null;
    try {
      openTransaction();
      String name = normalizeIdentifier(schemaName.getSchemaName());
      String dbName = normalizeIdentifier(schemaName.getDbName());
      String catName = normalizeIdentifier(schemaName.getCatName());
      query = pm.newQuery(MSchemaVersion.class, "iSchema.name == schemaName &&" +
          "iSchema.db.name == dbName && iSchema.db.catalogName == cat");
      query.declareParameters("java.lang.String schemaName, java.lang.String dbName," +
          " java.lang.String cat");
      query.setOrdering("version descending");
      List<MSchemaVersion> mSchemaVersions = query.setParameters(name, dbName, catName).executeList();
      pm.retrieveAll(mSchemaVersions);
      if (mSchemaVersions == null || mSchemaVersions.isEmpty()) {
        return null;
      }
      List<SchemaVersion> schemaVersions = new ArrayList<>(mSchemaVersions.size());
      for (MSchemaVersion mSchemaVersion : mSchemaVersions) {
        pm.retrieveAll(mSchemaVersion.getCols());
        if (mSchemaVersion.getSerDe() != null) {
          pm.retrieve(mSchemaVersion.getSerDe());
        }
        schemaVersions.add(convertToSchemaVersion(mSchemaVersion));
      }
      committed = commitTransaction();
      return schemaVersions;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public List<SchemaVersion> getSchemaVersionsByColumns(String colName, String colNamespace,
                                                        String type) throws MetaException {
    if (colName == null && colNamespace == null) {
      // Don't allow a query that returns everything, it will blow stuff up.
      throw new MetaException("You must specify column name or column namespace, else your query " +
          "may be too large");
    }
    boolean committed = false;
    Query query = null;
    try {
      openTransaction();
      if (colName != null) {
        colName = normalizeIdentifier(colName);
      }
      if (type != null) {
        type = normalizeIdentifier(type);
      }
      Map<String, String> parameters = new HashMap<>(3);
      StringBuilder sql = new StringBuilder("select SCHEMA_VERSION_ID from " +
          "SCHEMA_VERSION, COLUMNS_V2 where SCHEMA_VERSION.CD_ID = COLUMNS_V2.CD_ID ");
      if (colName != null) {
        sql.append("and COLUMNS_V2.COLUMN_NAME = :colName ");
        parameters.put("colName", colName);
      }
      if (colNamespace != null) {
        sql.append("and COLUMNS_V2.COMMENT = :colComment ");
        parameters.put("colComment", colNamespace);
      }
      if (type != null) {
        sql.append("and COLUMNS_V2.TYPE_NAME = :colType ");
        parameters.put("colType", type);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("getSchemaVersionsByColumns going to execute query {}", sql);
        LOG.debug("With parameters");
        for (Map.Entry<String, String> p : parameters.entrySet()) {
          LOG.debug(p.getKey() + " : " + p.getValue());
        }
      }
      query = pm.newQuery("javax.jdo.query.SQL", sql.toString());
      query.setClass(MSchemaVersion.class);
      List<MSchemaVersion> mSchemaVersions = query.setNamedParameters(parameters).executeList();
      if (mSchemaVersions == null || mSchemaVersions.isEmpty()) {
        return Collections.emptyList();
      }
      pm.retrieveAll(mSchemaVersions);
      List<SchemaVersion> schemaVersions = new ArrayList<>(mSchemaVersions.size());
      for (MSchemaVersion mSchemaVersion : mSchemaVersions) {
        pm.retrieveAll(mSchemaVersion.getCols());
        if (mSchemaVersion.getSerDe() != null) {
          pm.retrieve(mSchemaVersion.getSerDe());
        }
        schemaVersions.add(convertToSchemaVersion(mSchemaVersion));
      }
      committed = commitTransaction();
      return schemaVersions;
    } finally {
      rollbackAndCleanup(committed, query);
    }

  }

  @Override
  public void dropSchemaVersion(SchemaVersionDescriptor version) throws NoSuchObjectException,
      MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MSchemaVersion mSchemaVersion = getMSchemaVersion(version.getSchema().getCatName(),
          version.getSchema().getDbName(),
          version.getSchema().getSchemaName(), version.getVersion());
      if (mSchemaVersion != null) {
        pm.deletePersistentAll(mSchemaVersion);
      } else {
        throw new NoSuchObjectException("Schema version " + version + "does not exist");
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public SerDeInfo getSerDeInfo(String serDeName) throws NoSuchObjectException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MSerDeInfo mSerDeInfo = getMSerDeInfo(serDeName);
      if (mSerDeInfo == null) {
        throw new NoSuchObjectException("No SerDe named " + serDeName);
      }
      SerDeInfo serde = convertToSerDeInfo(mSerDeInfo, conf, false);
      committed = commitTransaction();
      return serde;
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  private MSerDeInfo getMSerDeInfo(String serDeName) {
    try (QueryWrapper query = new QueryWrapper(pm.newQuery(MSerDeInfo.class, "name == serDeName"))) {
      query.declareParameters("java.lang.String serDeName");
      query.setUnique(true);
      MSerDeInfo mSerDeInfo = (MSerDeInfo) query.execute(serDeName);
      pm.retrieve(mSerDeInfo);
      return mSerDeInfo;
    }
  }

  @Override
  public void addSerde(SerDeInfo serde) throws AlreadyExistsException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      if (getMSerDeInfo(serde.getName()) != null) {
        throw new AlreadyExistsException("Serde with name " + serde.getName() + " already exists");
      }
      MSerDeInfo mSerde = convertToMSerDeInfo(serde);
      pm.makePersistent(mSerde);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }

  }

  private MISchema convertToMISchema(ISchema schema) throws NoSuchObjectException {
    return new MISchema(schema.getSchemaType().getValue(),
                        normalizeIdentifier(schema.getName()),
                        getMDatabase(schema.getCatName(), schema.getDbName()),
                        schema.getCompatibility().getValue(),
                        schema.getValidationLevel().getValue(),
                        schema.isCanEvolve(),
                        schema.isSetSchemaGroup() ? schema.getSchemaGroup() : null,
                        schema.isSetDescription() ? schema.getDescription() : null);
  }

  private ISchema convertToISchema(MISchema mSchema) {
    if (mSchema == null) {
      return null;
    }
    ISchema schema = new ISchema(SchemaType.findByValue(mSchema.getSchemaType()),
                                 mSchema.getName(),
                                 mSchema.getDb().getCatalogName(),
                                 mSchema.getDb().getName(),
                                 SchemaCompatibility.findByValue(mSchema.getCompatibility()),
                                 SchemaValidation.findByValue(mSchema.getValidationLevel()),
                                 mSchema.getCanEvolve());
    if (mSchema.getDescription() != null) {
      schema.setDescription(mSchema.getDescription());
    }
    if (mSchema.getSchemaGroup() != null) {
      schema.setSchemaGroup(mSchema.getSchemaGroup());
    }
    return schema;
  }

  private MSchemaVersion convertToMSchemaVersion(SchemaVersion schemaVersion) throws MetaException {
    return new MSchemaVersion(getMISchema(
        normalizeIdentifier(schemaVersion.getSchema().getCatName()),
        normalizeIdentifier(schemaVersion.getSchema().getDbName()),
        normalizeIdentifier(schemaVersion.getSchema().getSchemaName())),
        schemaVersion.getVersion(),
        schemaVersion.getCreatedAt(),
        createNewMColumnDescriptor(convertToMFieldSchemas(schemaVersion.getCols())),
        schemaVersion.isSetState() ? schemaVersion.getState().getValue() : 0,
        schemaVersion.isSetDescription() ? schemaVersion.getDescription() : null,
        schemaVersion.isSetSchemaText() ? schemaVersion.getSchemaText() : null,
        schemaVersion.isSetFingerprint() ? schemaVersion.getFingerprint() : null,
        schemaVersion.isSetName() ? schemaVersion.getName() : null,
        schemaVersion.isSetSerDe() ? convertToMSerDeInfo(schemaVersion.getSerDe()) : null);
  }

  private SchemaVersion convertToSchemaVersion(MSchemaVersion mSchemaVersion) throws MetaException {
    if (mSchemaVersion == null) {
      return null;
    }
    SchemaVersion schemaVersion = new SchemaVersion(
        new ISchemaName(mSchemaVersion.getiSchema().getDb().getCatalogName(),
            mSchemaVersion.getiSchema().getDb().getName(), mSchemaVersion.getiSchema().getName()),
        mSchemaVersion.getVersion(),
        mSchemaVersion.getCreatedAt(),
        convertToFieldSchemas(mSchemaVersion.getCols().getCols()));
    if (mSchemaVersion.getState() > 0) {
      schemaVersion.setState(SchemaVersionState.findByValue(mSchemaVersion.getState()));
    }
    if (mSchemaVersion.getDescription() != null) {
      schemaVersion.setDescription(mSchemaVersion.getDescription());
    }
    if (mSchemaVersion.getSchemaText() != null) {
      schemaVersion.setSchemaText(mSchemaVersion.getSchemaText());
    }
    if (mSchemaVersion.getFingerprint() != null) {
      schemaVersion.setFingerprint(mSchemaVersion.getFingerprint());
    }
    if (mSchemaVersion.getName() != null) {
      schemaVersion.setName(mSchemaVersion.getName());
    }
    if (mSchemaVersion.getSerDe() != null) {
      schemaVersion.setSerDe(convertToSerDeInfo(mSchemaVersion.getSerDe(), conf, false));
    }
    return schemaVersion;
  }

  /**
   * This is a cleanup method which is used to rollback a active transaction
   * if the success flag is false and close the associated Query object. This method is used
   * internally and visible for testing purposes only
   * @param success Rollback the current active transaction if false
   * @param query Query object which needs to be closed
   */
  @VisibleForTesting
  protected void rollbackAndCleanup(boolean success, Query query) {
    try {
      if (!success) {
        rollbackTransaction();
      }
    } finally {
      if (query != null) {
        query.closeAll();
      }
    }
  }

  private void checkForConstraintException(Exception e, String msg) throws AlreadyExistsException {
    if (getConstraintException(e) != null) {
      LOG.error(msg, e);
      throw new AlreadyExistsException(msg);
    }
  }

  private Throwable getConstraintException(Throwable t) {
    while (t != null) {
      if (t instanceof SQLIntegrityConstraintViolationException) {
        return t;
      }
      t = t.getCause();
    }
    return null;
  }

  @Override
  public void createResourcePlan(
      WMResourcePlan resourcePlan, String copyFromName, int defaultPoolSize)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException {
    boolean commited = false;
    String rpName = normalizeIdentifier(resourcePlan.getName());
    if (rpName.isEmpty()) {
      throw new InvalidObjectException("Resource name cannot be empty.");
    }
    MWMResourcePlan rp = null;
    if (copyFromName == null) {
      Integer queryParallelism = null;
      if (resourcePlan.isSetQueryParallelism()) {
        queryParallelism = resourcePlan.getQueryParallelism();
        if (queryParallelism <= 0) {
          throw new InvalidObjectException("Query parallelism should be positive.");
        }
      }
      rp = new MWMResourcePlan(rpName, queryParallelism, Status.DISABLED);
    } else {
      rp = new MWMResourcePlan(rpName, null, Status.DISABLED);
    }
    rp.setNs(resourcePlan.getNs());
    try {
      openTransaction();
      pm.makePersistent(rp);
      if (copyFromName != null) {
        String ns = getNsOrDefault(resourcePlan.getNs());
        MWMResourcePlan copyFrom = getMWMResourcePlan(copyFromName, ns, false);
        if (copyFrom == null) {
          throw new NoSuchObjectException(copyFromName + " in " + ns);
        }
        copyRpContents(rp, copyFrom);
      } else {
        // TODO: ideally, this should be moved outside to HiveMetaStore to be shared between
        //       all the RawStore-s. Right now there's no method to create a pool.
        if (defaultPoolSize > 0) {
          MWMPool defaultPool = new MWMPool(rp, "default", 1.0, defaultPoolSize, null);
          pm.makePersistent(defaultPool);
          rp.setPools(Sets.newHashSet(defaultPool));
          rp.setDefaultPool(defaultPool);
        }
      }
      commited = commitTransaction();
    } catch (InvalidOperationException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      checkForConstraintException(e, "Resource plan already exists: ");
      throw e;
    } finally {
      rollbackAndCleanup(commited, null);
    }
  }

  private void copyRpContents(MWMResourcePlan dest, MWMResourcePlan src) {
    dest.setQueryParallelism(src.getQueryParallelism());
    dest.setNs(src.getNs());
    Map<String, MWMPool> pools = new HashMap<>();
    Map<String, Set<MWMPool>> triggersToPools = new HashMap<>();
    for (MWMPool copyPool : src.getPools()) {
      MWMPool pool = new MWMPool(dest, copyPool.getPath(), copyPool.getAllocFraction(),
          copyPool.getQueryParallelism(), copyPool.getSchedulingPolicy());
      pm.makePersistent(pool);
      pools.put(copyPool.getPath(), pool);
      if (copyPool.getTriggers() != null) {
        for (MWMTrigger trigger : copyPool.getTriggers()) {
          Set<MWMPool> p2t = triggersToPools.get(trigger.getName());
          if (p2t == null) {
            p2t = new HashSet<>();
            triggersToPools.put(trigger.getName(), p2t);
          }
          p2t.add(pool);
          pool.setTriggers(new HashSet<>());
        }
      }
    }
    dest.setPools(new HashSet<>(pools.values()));
    if (src.getDefaultPool() != null) {
      dest.setDefaultPool(pools.get(src.getDefaultPool().getPath()));
    }
    Set<MWMMapping> mappings = new HashSet<>();
    for (MWMMapping copyMapping : src.getMappings()) {
      MWMPool pool = null;
      if (copyMapping.getPool() != null) {
        pool = pools.get(copyMapping.getPool().getPath());
      }
      MWMMapping mapping = new MWMMapping(dest, copyMapping.getEntityType(),
          copyMapping.getEntityName(), pool, copyMapping.getOrdering());
      pm.makePersistent(mapping);
      mappings.add(mapping);
    }
    dest.setMappings(mappings);
    Set<MWMTrigger> triggers = new HashSet<>();
    for (MWMTrigger copyTrigger : src.getTriggers()) {
      Set<MWMPool> p2t = triggersToPools.get(copyTrigger.getName());
      if (p2t == null) {
        p2t = new HashSet<>();
      }
      MWMTrigger trigger = new MWMTrigger(dest, copyTrigger.getName(),
          copyTrigger.getTriggerExpression(), copyTrigger.getActionExpression(), p2t,
          copyTrigger.getIsInUnmanaged());
      pm.makePersistent(trigger);
      for (MWMPool pool : p2t) {
        pool.getTriggers().add(trigger);
      }
      triggers.add(trigger);
    }
    dest.setTriggers(triggers);
  }

  private WMResourcePlan fromMResourcePlan(MWMResourcePlan mplan) {
    if (mplan == null) {
      return null;
    }
    WMResourcePlan rp = new WMResourcePlan();
    rp.setName(mplan.getName());
    rp.setNs(mplan.getNs());
    rp.setStatus(WMResourcePlanStatus.valueOf(mplan.getStatus().name()));
    if (mplan.getQueryParallelism() != null) {
      rp.setQueryParallelism(mplan.getQueryParallelism());
    }
    if (mplan.getDefaultPool() != null) {
      rp.setDefaultPoolPath(mplan.getDefaultPool().getPath());
    }
    return rp;
  }

  private WMFullResourcePlan fullFromMResourcePlan(MWMResourcePlan mplan) {
    if (mplan == null) {
      return null;
    }
    WMFullResourcePlan rp = new WMFullResourcePlan();
    rp.setPlan(fromMResourcePlan(mplan));
    for (MWMPool mPool : mplan.getPools()) {
      rp.addToPools(fromMPool(mPool, mplan.getName()));
      for (MWMTrigger mTrigger : mPool.getTriggers()) {
        rp.addToPoolTriggers(new WMPoolTrigger(mPool.getPath(), mTrigger.getName()));
      }
    }
    for (MWMTrigger mTrigger : mplan.getTriggers()) {
      rp.addToTriggers(fromMWMTrigger(mTrigger, mplan.getName()));
    }
    for (MWMMapping mMapping : mplan.getMappings()) {
      rp.addToMappings(fromMMapping(mMapping, mplan.getName()));
    }
    return rp;
  }

  private WMPool fromMPool(MWMPool mPool, String rpName) {
    WMPool result = new WMPool(rpName, mPool.getPath());
    assert mPool.getAllocFraction() != null;
    result.setAllocFraction(mPool.getAllocFraction());
    assert mPool.getQueryParallelism() != null;
    result.setQueryParallelism(mPool.getQueryParallelism());
    result.setSchedulingPolicy(mPool.getSchedulingPolicy());
    result.setNs(mPool.getResourcePlan().getNs());
    return result;
  }

  private WMMapping fromMMapping(MWMMapping mMapping, String rpName) {
    WMMapping result = new WMMapping(rpName,
        mMapping.getEntityType().toString(), mMapping.getEntityName());
    if (mMapping.getPool() != null) {
      result.setPoolPath(mMapping.getPool().getPath());
    }
    if (mMapping.getOrdering() != null) {
      result.setOrdering(mMapping.getOrdering());
    }
    result.setNs(mMapping.getResourcePlan().getNs());
    return result;
  }

  private String getNsOrDefault(String ns) {
    // This is only needed for old clients not setting NS in requests.
    // Not clear how to handle this... this is properly a HS2 config but metastore needs its default
    // value for backward compat, and we don't want it configurable separately because it's also
    // used in upgrade scripts, were it cannot be configured.
     return normalizeIdentifier(ns == null ? "default" : ns);
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String name, String ns) throws NoSuchObjectException {
    boolean commited = false;
    try {
      openTransaction();
      WMFullResourcePlan fullRp = fullFromMResourcePlan(getMWMResourcePlan(name, ns, false));
      commited = commitTransaction();
      return fullRp;
    } catch (InvalidOperationException e) {
      // Should not happen, edit check is false.
      throw new RuntimeException(e);
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  private MWMResourcePlan getMWMResourcePlan(String name, String ns, boolean editCheck)
      throws NoSuchObjectException, InvalidOperationException {
    return getMWMResourcePlan(name, ns, editCheck, true);
  }

  private MWMResourcePlan getMWMResourcePlan(String name, String ns, boolean editCheck, boolean mustExist)
      throws NoSuchObjectException, InvalidOperationException {
    MWMResourcePlan resourcePlan;
    boolean commited = false;
    Query query = null;

    name = normalizeIdentifier(name);
    try {
      query = createGetResourcePlanQuery();
      ns = getNsOrDefault(ns);
      resourcePlan = (MWMResourcePlan) query.execute(name, ns);
      pm.retrieve(resourcePlan);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    if (mustExist && resourcePlan == null) {
      throw new NoSuchObjectException("There is no resource plan named: " + name + " in " + ns);
    }
    if (editCheck && resourcePlan != null
        && resourcePlan.getStatus() != MWMResourcePlan.Status.DISABLED) {
      throw new InvalidOperationException("Resource plan must be disabled to edit it.");
    }
    return resourcePlan;
  }

  private Query createGetResourcePlanQuery() {
    openTransaction();
    Query query = pm.newQuery(MWMResourcePlan.class, "name == rpname && ns == nsname");
    query.declareParameters("java.lang.String rpname, java.lang.String nsname");
    query.setUnique(true);
    return query;
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans(String ns) throws MetaException {
    List<WMResourcePlan> resourcePlans = new ArrayList();
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MWMResourcePlan.class, "ns == nsname");
      query.declareParameters("java.lang.String nsname");
      List<MWMResourcePlan> mplans = (List<MWMResourcePlan>) query.execute(getNsOrDefault(ns));
      pm.retrieveAll(mplans);
      commited = commitTransaction();
      if (mplans != null) {
        for (MWMResourcePlan mplan : mplans) {
          resourcePlans.add(fromMResourcePlan(mplan));
        }
      }
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return resourcePlans;
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String name, String ns, WMNullableResourcePlan changes,
      boolean canActivateDisabled, boolean canDeactivate, boolean isReplace)
    throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    name = name == null ? null : normalizeIdentifier(name);
    if (isReplace && name == null) {
      throw new InvalidOperationException("Cannot replace without specifying the source plan");
    }
    boolean commited = false;
    Query query = null;
    // This method only returns the result when activating a resource plan.
    // We could also add a boolean flag to be specified by the caller to see
    // when the result might be needed.
    WMFullResourcePlan result = null;
    try {
      openTransaction();
      if (isReplace) {
        result = handleAlterReplace(name, ns, changes);
      } else {
        result = handleSimpleAlter(name, ns, changes, canActivateDisabled, canDeactivate);
      }

      commited = commitTransaction();
      return result;
    } catch (Exception e) {
      checkForConstraintException(e, "Resource plan name should be unique: ");
      throw e;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private WMFullResourcePlan handleSimpleAlter(String name, String ns, WMNullableResourcePlan changes,
      boolean canActivateDisabled, boolean canDeactivate)
          throws InvalidOperationException, NoSuchObjectException, MetaException {
    MWMResourcePlan plan = name == null ? getActiveMWMResourcePlan(ns)
        : getMWMResourcePlan(name, ns, !changes.isSetStatus());
    boolean hasNsChange = changes.isSetNs() && !changes.getNs().equals(getNsOrDefault(plan.getNs()));
    if (hasNsChange) {
      throw new InvalidOperationException("Cannot change ns; from " + getNsOrDefault(plan.getNs())
          + " to " + changes.getNs());
    }
    boolean hasNameChange = changes.isSetName() && !changes.getName().equals(name);
    // Verify that field changes are consistent with what Hive does. Note: we could handle this.
    if (changes.isSetIsSetQueryParallelism()
        || changes.isSetIsSetDefaultPoolPath() || hasNameChange) {
      if (changes.isSetStatus()) {
        throw new InvalidOperationException("Cannot change values during status switch.");
      } else if (plan.getStatus() != MWMResourcePlan.Status.DISABLED) {
        throw new InvalidOperationException("Resource plan must be disabled to edit it.");
      }
    }

    // Handle rename and other changes.
    if (changes.isSetName()) {
      String newName = normalizeIdentifier(changes.getName());
      if (newName.isEmpty()) {
        throw new InvalidOperationException("Cannot rename to empty value.");
      }
      if (!newName.equals(plan.getName())) {
        plan.setName(newName);
      }
    }
    if (changes.isSetIsSetQueryParallelism() && changes.isIsSetQueryParallelism()) {
      if (changes.isSetQueryParallelism()) {
        if (changes.getQueryParallelism() <= 0) {
          throw new InvalidOperationException("queryParallelism should be positive.");
        }
        plan.setQueryParallelism(changes.getQueryParallelism());
      } else {
        plan.setQueryParallelism(null);
      }
    }
    if (changes.isSetIsSetDefaultPoolPath() && changes.isIsSetDefaultPoolPath()) {
      if (changes.isSetDefaultPoolPath()) {
        MWMPool pool = getPool(plan, changes.getDefaultPoolPath());
        plan.setDefaultPool(pool);
      } else {
        plan.setDefaultPool(null);
      }
    }

    // Handle the status change.
    if (changes.isSetStatus()) {
      return switchStatus(name, plan,
          changes.getStatus().name(), canActivateDisabled, canDeactivate);
    }
    return null;
  }

  private WMFullResourcePlan handleAlterReplace(String name, String ns, WMNullableResourcePlan changes)
          throws InvalidOperationException, NoSuchObjectException, MetaException {
    // Verify that field changes are consistent with what Hive does. Note: we could handle this.
    if (changes.isSetQueryParallelism() || changes.isSetDefaultPoolPath()) {
      throw new InvalidOperationException("Cannot change values during replace.");
    }
    boolean isReplacingSpecific = changes.isSetName();
    boolean isReplacingActive = (changes.isSetStatus()
        && changes.getStatus() == WMResourcePlanStatus.ACTIVE);
    if (isReplacingActive == isReplacingSpecific) {
      throw new InvalidOperationException("Must specify a name, or the active plan; received "
          + changes.getName() + ", " + (changes.isSetStatus() ? changes.getStatus() : null));
    }
    if (name == null) {
      throw new InvalidOperationException("Invalid replace - no name specified");
    }
    ns = getNsOrDefault(ns);
    MWMResourcePlan replacedPlan = isReplacingSpecific
        ? getMWMResourcePlan(changes.getName(), ns, false) : getActiveMWMResourcePlan(ns);
    MWMResourcePlan plan = getMWMResourcePlan(name, ns, false);

    if (replacedPlan.getName().equals(plan.getName())) {
      throw new InvalidOperationException("A plan cannot replace itself");
    }
    String oldNs = getNsOrDefault(replacedPlan.getNs()), newNs = getNsOrDefault(plan.getNs());
    if (!oldNs.equals(newNs)) {
      throw new InvalidOperationException("Cannot change the namespace; replacing "
          + oldNs + " with " + newNs);
    }

    // We will inherit the name and status from the plan we are replacing.
    String newName = replacedPlan.getName();
    int i = 0;
    String copyName = generateOldPlanName(newName, i);
    while (true) {
      MWMResourcePlan dup = getMWMResourcePlan(copyName, ns, false, false);
      if (dup == null) {
        break;
      }
      // Note: this can still conflict with parallel transactions. We do not currently handle
      //       parallel changes from two admins (by design :().
      copyName = generateOldPlanName(newName, ++i);
    }
    replacedPlan.setName(copyName);
    plan.setName(newName);
    plan.setStatus(replacedPlan.getStatus());
    replacedPlan.setStatus(MWMResourcePlan.Status.DISABLED);
    // TODO: add a configurable option to skip the history and just drop it?
    return plan.getStatus() == Status.ACTIVE ? fullFromMResourcePlan(plan) : null;
  }

  private String generateOldPlanName(String newName, int i) {
    if (MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST)) {
      // Do not use datetime in tests to avoid result changes.
      return newName + "_old_" + i;
    } else {
      return newName + "_old_"
          + LocalDateTime.now().format(YMDHMS_FORMAT) + (i == 0 ? "" : ("_" + i));
    }
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan(String ns) throws MetaException {
    // Note: fullFromMResroucePlan needs to be called inside the txn, otherwise we could have
    //       deduplicated this with getActiveMWMResourcePlan.
    boolean commited = false;
    Query query = null;
    WMFullResourcePlan result = null;
    try {
      query = createActivePlanQuery();
      MWMResourcePlan mResourcePlan = (MWMResourcePlan) query.execute(
          Status.ACTIVE.toString(), getNsOrDefault(ns));
      if (mResourcePlan != null) {
        result = fullFromMResourcePlan(mResourcePlan);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return result;
  }

  private MWMResourcePlan getActiveMWMResourcePlan(String ns) {
    boolean commited = false;
    Query query = null;
    MWMResourcePlan result = null;
    try {
      query = createActivePlanQuery();
      result = (MWMResourcePlan) query.execute(
          Status.ACTIVE.toString(), getNsOrDefault(ns));
      pm.retrieve(result);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return result;
  }

  private Query createActivePlanQuery() {
    openTransaction();
    Query query = pm.newQuery(MWMResourcePlan.class, "status == activeStatus && ns == nsname");
    query.declareParameters("java.lang.String activeStatus, java.lang.String nsname");
    query.setUnique(true);
    return query;
  }

  private WMFullResourcePlan switchStatus(String name, MWMResourcePlan mResourcePlan, String status,
      boolean canActivateDisabled, boolean canDeactivate) throws InvalidOperationException {
    Status currentStatus = mResourcePlan.getStatus();
    Status newStatus = null;
    try {
      newStatus = Status.valueOf(status);
    } catch (IllegalArgumentException e) {
      throw new InvalidOperationException("Invalid status: " + status);
    }

    if (newStatus == currentStatus) {
      return null;
    }

    boolean doActivate = false, doValidate = false;
    switch (currentStatus) {
      case ACTIVE: // No status change for active resource plan, first activate another plan.
        if (!canDeactivate) {
          throw new InvalidOperationException(
            "Resource plan " + name
              + " is active; activate another plan first, or disable workload management.");
        }
        break;
      case DISABLED:
        assert newStatus == Status.ACTIVE || newStatus == Status.ENABLED;
        doValidate = true;
        doActivate = (newStatus == Status.ACTIVE);
        if (doActivate && !canActivateDisabled) {
          throw new InvalidOperationException("Resource plan " + name
              + " is disabled and should be enabled before activation (or in the same command)");
        }
        break;
      case ENABLED:
        if (newStatus == Status.DISABLED) {
          mResourcePlan.setStatus(newStatus);
          return null; // A simple case.
        }
        assert newStatus == Status.ACTIVE;
        doActivate = true;
        break;
      default: throw new AssertionError("Unexpected status " + currentStatus);
    }
    if (doValidate) {
      // Note: this may use additional inputs from the caller, e.g. maximum query
      // parallelism in the cluster based on physical constraints.
      WMValidateResourcePlanResponse response = getResourcePlanErrors(mResourcePlan);
      if (!response.getErrors().isEmpty()) {
        throw new InvalidOperationException(
            "ResourcePlan: " + name + " is invalid: " + response.getErrors());
      }
    }
    if (doActivate) {
      // Deactivate currently active resource plan.
      deactivateActiveResourcePlan(mResourcePlan.getNs());
      mResourcePlan.setStatus(newStatus);
      return fullFromMResourcePlan(mResourcePlan);
    } else {
      mResourcePlan.setStatus(newStatus);
    }
    return null;
  }

  private void deactivateActiveResourcePlan(String ns) {
    boolean commited = false;
    Query query = null;
    try {
      query = createActivePlanQuery();
      MWMResourcePlan mResourcePlan = (MWMResourcePlan) query.execute(
          Status.ACTIVE.toString(), getNsOrDefault(ns));
      // We may not have an active resource plan in the start.
      if (mResourcePlan != null) {
        mResourcePlan.setStatus(Status.ENABLED);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private static class PoolData {
    double totalChildrenAllocFraction = 0;
    boolean found = false;
    boolean hasChildren = false;
  }

  private PoolData getPoolData(Map<String, PoolData> poolInfo, String poolPath) {
    PoolData poolData = poolInfo.get(poolPath);
    if (poolData == null) {
      poolData = new PoolData();
      poolInfo.put(poolPath, poolData);
    }
    return poolData;
  }

  private WMValidateResourcePlanResponse getResourcePlanErrors(MWMResourcePlan mResourcePlan) {
    WMValidateResourcePlanResponse response = new WMValidateResourcePlanResponse();
    response.setErrors(new ArrayList());
    response.setWarnings(new ArrayList());
    Integer rpParallelism = mResourcePlan.getQueryParallelism();
    if (rpParallelism != null && rpParallelism < 1) {
      response.addToErrors("Query parallelism should for resource plan be positive. Got: " +
          rpParallelism);
    }
    int totalQueryParallelism = 0;
    Map<String, PoolData> poolInfo = new HashMap<>();
    for (MWMPool pool : mResourcePlan.getPools()) {
      PoolData currentPoolData = getPoolData(poolInfo, pool.getPath());
      currentPoolData.found = true;
      String parent = getParentPath(pool.getPath(), "");
      PoolData parentPoolData = getPoolData(poolInfo, parent);
      parentPoolData.hasChildren = true;
      parentPoolData.totalChildrenAllocFraction += pool.getAllocFraction();
      if (pool.getQueryParallelism() != null && pool.getQueryParallelism() < 1) {
        response.addToErrors("Invalid query parallelism for pool: " + pool.getPath());
      } else {
        totalQueryParallelism += pool.getQueryParallelism();
      }
      if (!MetaStoreUtils.isValidSchedulingPolicy(pool.getSchedulingPolicy())) {
        response.addToErrors("Invalid scheduling policy " + pool.getSchedulingPolicy() +
            " for pool: " + pool.getPath());
      }
    }
    if (rpParallelism != null) {
      if (rpParallelism < totalQueryParallelism) {
        response.addToErrors("Sum of all pools' query parallelism: " + totalQueryParallelism  +
            " exceeds resource plan query parallelism: " + rpParallelism);
      } else if (rpParallelism != totalQueryParallelism) {
        response.addToWarnings("Sum of all pools' query parallelism: " + totalQueryParallelism  +
            " is less than resource plan query parallelism: " + rpParallelism);
      }
    }
    for (Entry<String, PoolData> entry : poolInfo.entrySet()) {
      final PoolData poolData = entry.getValue();
      final boolean isRoot = entry.getKey().isEmpty();
      // Special case for root parent
      if (isRoot) {
        poolData.found = true;
        if (!poolData.hasChildren) {
          response.addToErrors("Root has no children");
          // TODO: change fractions to use decimal? somewhat brittle
        } else if (Math.abs(1.0 - poolData.totalChildrenAllocFraction) > 0.00001) {
          response.addToErrors("Sum of root children pools' alloc fraction should be 1.0 got: " +
              poolData.totalChildrenAllocFraction + " for pool: " + entry.getKey());
        }
      }
      if (!poolData.found) {
        response.addToErrors("Pool does not exists but has children: " + entry.getKey());
      }
      if (poolData.hasChildren) {

        if (!isRoot && (poolData.totalChildrenAllocFraction - 1.0) > 0.00001) {
          response.addToErrors("Sum of children pools' alloc fraction should be less than 1 got: "
              + poolData.totalChildrenAllocFraction + " for pool: " + entry.getKey());
        }
      }
    }
    // trigger and action expressions are not validated here, since counters are not
    // available and grammar check is there in the language itself.
    return response;
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String name, String ns)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    name = normalizeIdentifier(name);
    boolean committed = false;
    Query query = null;
    try {
      query = createGetResourcePlanQuery();
      MWMResourcePlan mResourcePlan = (MWMResourcePlan) query.execute(name, ns);
      if (mResourcePlan == null) {
        throw new NoSuchObjectException("Cannot find resourcePlan: " + name + " in " + ns);
      }
      WMValidateResourcePlanResponse result = getResourcePlanErrors(mResourcePlan);
      committed = commitTransaction();
      return result;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public void dropResourcePlan(String name, String ns) throws NoSuchObjectException, MetaException {
    name = normalizeIdentifier(name);
    boolean commited = false;
    Query query = null;
    try {
      query = createGetResourcePlanQuery();
      MWMResourcePlan resourcePlan = (MWMResourcePlan) query.execute(name, getNsOrDefault(ns));
      pm.retrieve(resourcePlan); // TODO: why do some codepaths call retrieve and some don't?
      if (resourcePlan == null) {
        throw new NoSuchObjectException("There is no resource plan named: " + name + " in " + ns);
      }
      if (resourcePlan.getStatus() == Status.ACTIVE) {
        throw new MetaException("Cannot drop an active resource plan");
      }
      // First, drop all the dependencies.
      resourcePlan.setDefaultPool(null);
      pm.deletePersistentAll(resourcePlan.getTriggers());
      pm.deletePersistentAll(resourcePlan.getMappings());
      pm.deletePersistentAll(resourcePlan.getPools());
      pm.deletePersistent(resourcePlan);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void createWMTrigger(WMTrigger trigger)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(
          trigger.getResourcePlanName(), trigger.getNs(), true);
      MWMTrigger mTrigger = new MWMTrigger(resourcePlan,
          normalizeIdentifier(trigger.getTriggerName()), trigger.getTriggerExpression(),
          trigger.getActionExpression(), null,
          trigger.isSetIsInUnmanaged() && trigger.isIsInUnmanaged());
      pm.makePersistent(mTrigger);
      commited = commitTransaction();
    } catch (Exception e) {
      checkForConstraintException(e, "Trigger already exists, use alter: ");
      throw e;
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  @Override
  public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(
          trigger.getResourcePlanName(), trigger.getNs(), true);
      MWMTrigger mTrigger = getTrigger(resourcePlan, trigger.getTriggerName());
      // Update the object.
      if (trigger.isSetTriggerExpression()) {
        mTrigger.setTriggerExpression(trigger.getTriggerExpression());
      }
      if (trigger.isSetActionExpression()) {
        mTrigger.setActionExpression(trigger.getActionExpression());
      }
      if (trigger.isSetIsInUnmanaged()) {
        mTrigger.setIsInUnmanaged(trigger.isIsInUnmanaged());
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private MWMTrigger getTrigger(MWMResourcePlan resourcePlan, String triggerName)
      throws NoSuchObjectException {
    triggerName = normalizeIdentifier(triggerName);
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      // Get the MWMTrigger object from DN
      query = pm.newQuery(MWMTrigger.class, "resourcePlan == rp && name == triggerName");
      query.declareParameters("MWMResourcePlan rp, java.lang.String triggerName");
      query.setUnique(true);
      MWMTrigger mTrigger = (MWMTrigger) query.execute(resourcePlan, triggerName);
      if (mTrigger == null) {
        throw new NoSuchObjectException("Cannot find trigger with name: " + triggerName);
      }
      pm.retrieve(mTrigger);
      commited = commitTransaction();
      return mTrigger;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName, String ns)
      throws NoSuchObjectException, InvalidOperationException, MetaException  {
    resourcePlanName = normalizeIdentifier(resourcePlanName);
    triggerName = normalizeIdentifier(triggerName);

    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, ns, true);
      query = pm.newQuery(MWMTrigger.class, "resourcePlan == rp && name == triggerName");
      query.declareParameters("MWMResourcePlan rp, java.lang.String triggerName");
      if (query.deletePersistentAll(resourcePlan, triggerName) != 1) {
        throw new NoSuchObjectException("Cannot delete trigger: " + triggerName);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException {
    List<WMTrigger> triggers = new ArrayList();
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan;
      try {
        resourcePlan = getMWMResourcePlan(resourcePlanName, ns, false);
      } catch (InvalidOperationException e) {
        // Should not happen, edit check is false.
        throw new RuntimeException(e);
      }
      query = pm.newQuery(MWMTrigger.class, "resourcePlan == rp");
      query.declareParameters("MWMResourcePlan rp");
      List<MWMTrigger> mTriggers = (List<MWMTrigger>) query.execute(resourcePlan);
      pm.retrieveAll(mTriggers);
      commited = commitTransaction();
      if (mTriggers != null) {
        for (MWMTrigger trigger : mTriggers) {
          triggers.add(fromMWMTrigger(trigger, resourcePlanName));
        }
      }
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return triggers;
  }

  private WMTrigger fromMWMTrigger(MWMTrigger mTrigger, String resourcePlanName) {
    WMTrigger trigger = new WMTrigger();
    trigger.setResourcePlanName(resourcePlanName);
    trigger.setTriggerName(mTrigger.getName());
    trigger.setTriggerExpression(mTrigger.getTriggerExpression());
    trigger.setActionExpression(mTrigger.getActionExpression());
    trigger.setIsInUnmanaged(mTrigger.getIsInUnmanaged());
    trigger.setNs(mTrigger.getResourcePlan().getNs());
    return trigger;
  }

  @Override
  public void createPool(WMPool pool) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(
          pool.getResourcePlanName(), pool.getNs(), true);

      if (!poolParentExists(resourcePlan, pool.getPoolPath())) {
        throw new NoSuchObjectException("Pool path is invalid, the parent does not exist");
      }
      String policy = pool.getSchedulingPolicy();
      if (!MetaStoreUtils.isValidSchedulingPolicy(policy)) {
        throw new InvalidOperationException("Invalid scheduling policy " + policy);
      }
      MWMPool mPool = new MWMPool(resourcePlan, pool.getPoolPath(), pool.getAllocFraction(),
          pool.getQueryParallelism(), policy);
      pm.makePersistent(mPool);
      commited = commitTransaction();
    } catch (Exception e) {
      checkForConstraintException(e, "Pool already exists: ");
      throw e;
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  @Override
  public void alterPool(WMNullablePool pool, String poolPath) throws AlreadyExistsException,
      NoSuchObjectException, InvalidOperationException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(
          pool.getResourcePlanName(), pool.getNs(), true);
      MWMPool mPool = getPool(resourcePlan, poolPath);
      pm.retrieve(mPool);
      if (pool.isSetAllocFraction()) {
        mPool.setAllocFraction(pool.getAllocFraction());
      }
      if (pool.isSetQueryParallelism()) {
        mPool.setQueryParallelism(pool.getQueryParallelism());
      }
      if (pool.isSetIsSetSchedulingPolicy() && pool.isIsSetSchedulingPolicy()) {
        if (pool.isSetSchedulingPolicy()) {
          String policy = pool.getSchedulingPolicy();
          if (!MetaStoreUtils.isValidSchedulingPolicy(policy)) {
            throw new InvalidOperationException("Invalid scheduling policy " + policy);
          }
          mPool.setSchedulingPolicy(pool.getSchedulingPolicy());
        } else {
          mPool.setSchedulingPolicy(null);
        }
      }
      if (pool.isSetPoolPath() && !pool.getPoolPath().equals(mPool.getPath())) {
        moveDescendents(resourcePlan, mPool.getPath(), pool.getPoolPath());
        mPool.setPath(pool.getPoolPath());
      }

      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  private MWMPool getPool(MWMResourcePlan resourcePlan, String poolPath)
      throws NoSuchObjectException {
    poolPath = normalizeIdentifier(poolPath);
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path == poolPath");
      query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
      query.setUnique(true);
      MWMPool mPool = (MWMPool) query.execute(resourcePlan, poolPath);
      commited = commitTransaction();
      if (mPool == null) {
        throw new NoSuchObjectException("Cannot find pool: " + poolPath);
      }
      pm.retrieve(mPool);
      return mPool;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private void moveDescendents(MWMResourcePlan resourcePlan, String path, String newPoolPath)
      throws NoSuchObjectException {
    if (!poolParentExists(resourcePlan, newPoolPath)) {
      throw new NoSuchObjectException("Pool path is invalid, the parent does not exist");
    }
    boolean commited = false;
    Query query = null;
    openTransaction();
    try {
      query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path.startsWith(poolPath)");
      query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
      List<MWMPool> descPools = (List<MWMPool>) query.execute(resourcePlan, path + ".");
      pm.retrieveAll(descPools);
      for (MWMPool pool : descPools) {
        pool.setPath(newPoolPath + pool.getPath().substring(path.length()));
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private String getParentPath(String poolPath, String defValue) {
    int idx = poolPath.lastIndexOf('.');
    if (idx == -1) {
      return defValue;
    }
    return poolPath.substring(0, idx);
  }

  private boolean poolParentExists(MWMResourcePlan resourcePlan, String poolPath) {
    String parent = getParentPath(poolPath, null);
    if (parent == null) {
      return true;
    }
    try {
      getPool(resourcePlan, parent);
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath, String ns)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    poolPath = normalizeIdentifier(poolPath);
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, ns, true);
      if (resourcePlan.getDefaultPool() != null &&
          resourcePlan.getDefaultPool().getPath().equals(poolPath)) {
        throw new InvalidOperationException("Cannot drop default pool of a resource plan");
      }
      if (poolHasChildren(resourcePlan, poolPath)) {
        throw new InvalidOperationException("Cannot drop a pool that has child pools");
      }
      query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path.startsWith(poolPath)");
      query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
      if (query.deletePersistentAll(resourcePlan, poolPath) != 1) {
        throw new NoSuchObjectException("Cannot delete pool: " + poolPath);
      }
      commited = commitTransaction();
    } catch(Exception e) {
      if (getConstraintException(e) != null) {
        throw new InvalidOperationException("Please remove all mappings for this pool.");
      }
      throw e;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private boolean poolHasChildren(MWMResourcePlan resourcePlan, String poolPath) {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path.startsWith(poolPath)");
      query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
      query.setResult("count(this)");
      query.setUnique(true);
      Long count = (Long) query.execute(resourcePlan, poolPath + ".");
      commited = commitTransaction();
      return count != null && count > 0;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
      MetaException {
    EntityType entityType = EntityType.valueOf(mapping.getEntityType().trim().toUpperCase());
    String entityName = normalizeIdentifier(mapping.getEntityName());
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(
          mapping.getResourcePlanName(), mapping.getNs(), true);
      MWMPool pool = null;
      if (mapping.isSetPoolPath()) {
        pool = getPool(resourcePlan, mapping.getPoolPath());
      }
      if (!update) {
        MWMMapping mMapping = new MWMMapping(resourcePlan, entityType, entityName, pool,
            mapping.getOrdering());
        pm.makePersistent(mMapping);
      } else {
        query = pm.newQuery(MWMMapping.class, "resourcePlan == rp && entityType == type " +
            "&& entityName == name");
        query.declareParameters(
            "MWMResourcePlan rp, java.lang.String type, java.lang.String name");
        query.setUnique(true);
        MWMMapping mMapping = (MWMMapping) query.execute(
            resourcePlan, entityType.toString(), entityName);
        mMapping.setPool(pool);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    String entityType = mapping.getEntityType().trim().toUpperCase();
    String entityName = normalizeIdentifier(mapping.getEntityName());
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(
          mapping.getResourcePlanName(), mapping.getNs(), true);
      query = pm.newQuery(MWMMapping.class,
          "resourcePlan == rp && entityType == type && entityName == name");
      query.declareParameters("MWMResourcePlan rp, java.lang.String type, java.lang.String name");
      if (query.deletePersistentAll(resourcePlan, entityType, entityName) != 1) {
        throw new NoSuchObjectException("Cannot delete mapping.");
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, String ns) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, ns, true);
      MWMPool pool = getPool(resourcePlan, poolPath);
      MWMTrigger trigger = getTrigger(resourcePlan, triggerName);
      pool.getTriggers().add(trigger);
      trigger.getPools().add(pool);
      pm.makePersistent(pool);
      pm.makePersistent(trigger);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  @Override
  public void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, String ns) throws NoSuchObjectException, InvalidOperationException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, ns, true);
      MWMPool pool = getPool(resourcePlan, poolPath);
      MWMTrigger trigger = getTrigger(resourcePlan, triggerName);
      pool.getTriggers().remove(trigger);
      trigger.getPools().remove(pool);
      pm.makePersistent(pool);
      pm.makePersistent(trigger);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  @Override
  public void addRuntimeStat(RuntimeStat stat) throws MetaException {
    LOG.debug("runtimeStat: {}", stat);
    MRuntimeStat mStat = MRuntimeStat.fromThrift(stat);
    boolean committed = false;
    openTransaction();
    try {
      pm.makePersistent(mStat);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public int deleteRuntimeStats(int maxRetainSecs) throws MetaException {
    if (maxRetainSecs < 0) {
      LOG.warn("runtime stats retention is disabled");
      return 0;
    }
    boolean committed = false;
    Query q = null;
    try {
      openTransaction();
      int maxCreateTime = (int) (System.currentTimeMillis() / 1000) - maxRetainSecs;
      q = pm.newQuery(MRuntimeStat.class);
      q.setFilter("createTime <= maxCreateTime");
      q.declareParameters("int maxCreateTime");
      long deleted = q.deletePersistentAll(maxCreateTime);
      committed = commitTransaction();
      return (int) deleted;
    } finally {
      rollbackAndCleanup(committed, q);
    }
  }

  @Override
  public List<RuntimeStat> getRuntimeStats(int maxEntries, int maxCreateTime) throws MetaException {
    boolean committed = false;
    try {
      openTransaction();
      List<RuntimeStat> stats = getMRuntimeStats(maxEntries, maxCreateTime);
      committed = commitTransaction();
      return stats;
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  private List<RuntimeStat> getMRuntimeStats(int maxEntries, int maxCreateTime) {
    try (QueryWrapper query = new QueryWrapper(pm.newQuery(MRuntimeStat.class))) {
      query.setOrdering("createTime descending");
      if (maxCreateTime > 0) {
        query.setFilter("createTime < " + maxCreateTime);
      }
      if (maxEntries < 0) {
        maxEntries = Integer.MAX_VALUE;
      }
      List<RuntimeStat> ret = new ArrayList<>();
      List<MRuntimeStat> res = (List<MRuntimeStat>) query.execute();
      int totalEntries = 0;
      for (MRuntimeStat mRuntimeStat : res) {
        pm.retrieve(mRuntimeStat);
        totalEntries += mRuntimeStat.getWeight();
        ret.add(MRuntimeStat.toThrift(mRuntimeStat));
        if (totalEntries >= maxEntries) {
          break;
        }
      }
      return ret;
    }
  }

  /**
   * Return true if the current statistics in the Metastore is valid
   * for the query of the given "txnId" and "queryValidWriteIdList".
   *
   * Note that a statistics entity is valid iff
   * the stats is written by the current query or
   * the conjunction of the following two are true:
   * ~ COLUMN_STATE_ACCURATE(CSA) state is true
   * ~ Isolation-level (snapshot) compliant with the query
   * @param tbl                    MTable of the stats entity
   * @param queryValidWriteIdList  valid writeId list of the query
   * @Precondition   "tbl" should be retrieved from the TBLS table.
   */
  private boolean isCurrentStatsValidForTheQuery(MTable tbl, String queryValidWriteIdList,
      boolean isCompleteStatsWriter) throws MetaException {
    return isCurrentStatsValidForTheQuery(tbl.getParameters(), tbl.getWriteId(),
        queryValidWriteIdList, isCompleteStatsWriter);
  }

  /**
   * Return true if the current statistics in the Metastore is valid
   * for the query of the given "txnId" and "queryValidWriteIdList".
   *
   * Note that a statistics entity is valid iff
   * the stats is written by the current query or
   * the conjunction of the following two are true:
   * ~ COLUMN_STATE_ACCURATE(CSA) state is true
   * ~ Isolation-level (snapshot) compliant with the query
   * @param queryValidWriteIdList  valid writeId list of the query
   * @Precondition   "part" should be retrieved from the PARTITIONS table.
   */
  // TODO: move to somewhere else
  public static boolean isCurrentStatsValidForTheQuery(
      Map<String, String> statsParams, long statsWriteId, String queryValidWriteIdList,
      boolean isCompleteStatsWriter) throws MetaException {

    // Note: can be changed to debug/info to verify the calls.
    LOG.debug("isCurrentStatsValidForTheQuery with stats write ID {}; query {}; writer: {} params {}",
        statsWriteId, queryValidWriteIdList, isCompleteStatsWriter, statsParams);
    // return true since the stats does not seem to be transactional.
    if (statsWriteId < 1) {
      return true;
    }
    // This COLUMN_STATS_ACCURATE(CSA) state checking also includes the case that the stats is
    // written by an aborted transaction but TXNS has no entry for the transaction
    // after compaction. Don't check for a complete stats writer - it may replace invalid stats.
    if (!isCompleteStatsWriter && !StatsSetupConst.areBasicStatsUptoDate(statsParams)) {
      return false;
    }

    if (queryValidWriteIdList != null) { // Can be null when stats are being reset to invalid.
      ValidWriteIdList list4TheQuery = ValidReaderWriteIdList.fromValue(queryValidWriteIdList);
      // Just check if the write ID is valid. If it's valid (i.e. we are allowed to see it),
      // that means it cannot possibly be a concurrent write. If it's not valid (we are not
      // allowed to see it), that means it's either concurrent or aborted, same thing for us.
      if (list4TheQuery.isWriteIdValid(statsWriteId)) {
        return true;
      }
      // Updater is also allowed to overwrite stats from aborted txns, as long as they are not concurrent.
      if (isCompleteStatsWriter && list4TheQuery.isWriteIdAborted(statsWriteId)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public ScheduledQueryPollResponse scheduledQueryPoll(ScheduledQueryPollRequest request) throws MetaException {
    ensureScheduledQueriesEnabled();
    boolean commited = false;
    ScheduledQueryPollResponse ret = new ScheduledQueryPollResponse();
    Query q = null;
    try {
      openTransaction();
      q = pm.newQuery(MScheduledQuery.class,
          "nextExecution <= now && enabled && clusterNamespace == ns && activeExecution == null");
      q.setSerializeRead(true);
      q.declareParameters("java.lang.Integer now, java.lang.String ns");
      q.setOrdering("nextExecution");
      int now = (int) (System.currentTimeMillis() / 1000);
      List<MScheduledQuery> results = (List<MScheduledQuery>) q.execute(now, request.getClusterNamespace());
      if (results == null || results.isEmpty()) {
        return new ScheduledQueryPollResponse();
      }
      MScheduledQuery schq = results.get(0);
      schq.setNextExecution(computeNextExecutionTime(schq.getSchedule()));

      MScheduledExecution execution = new MScheduledExecution();
      execution.setScheduledQuery(schq);
      execution.setState(QueryState.INITED);
      execution.setStartTime(now);
      execution.setLastUpdateTime(now);
      schq.setActiveExecution(execution);
      pm.makePersistent(execution);
      pm.makePersistent(schq);
      ObjectStoreTestHook.onScheduledQueryPoll();
      commited = commitTransaction();
      ret.setScheduleKey(schq.getScheduleKey());
      ret.setQuery("/* schedule: " + schq.getScheduleName() + " */" + schq.getQuery());
      ret.setUser(schq.getUser());
      int executionId = ((IntIdentity) pm.getObjectId(execution)).getKey();
      ret.setExecutionId(executionId);
    } catch (JDOException e) {
      LOG.debug("Caught jdo exception; exclusive", e);
      commited = false;
    } finally {
      rollbackAndCleanup(commited, q);
      return commited ? ret : new ScheduledQueryPollResponse();
    }
  }

  @Override
  public void scheduledQueryProgress(ScheduledQueryProgressInfo info) throws InvalidOperationException, MetaException {
    ensureScheduledQueriesEnabled();
    boolean commited = false;
    try {
      openTransaction();
      MScheduledExecution execution = pm.getObjectById(MScheduledExecution.class, info.getScheduledExecutionId());
      if (!validateStateChange(execution.getState(), info.getState())) {
        throw new InvalidOperationException("Invalid state change: " + execution.getState() + "=>" + info.getState());
      }
      execution.setState(info.getState());
      if (info.isSetExecutorQueryId()) {
        execution.setExecutorQueryId(info.getExecutorQueryId());
      }
      if (info.isSetErrorMessage()) {
        execution.setErrorMessage(abbreviateErrorMessage(info.getErrorMessage(), 1000));
      }

      switch (info.getState()) {
      case INITED:
      case EXECUTING:
        execution.setLastUpdateTime((int) (System.currentTimeMillis() / 1000));
        break;
      case FAILED:
      case FINISHED:
      case TIMED_OUT:
        execution.setEndTime((int) (System.currentTimeMillis() / 1000));
        execution.setLastUpdateTime(null);
        execution.getScheduledQuery().setActiveExecution(null);
        break;
      default:
        throw new InvalidOperationException("invalid state: " + info.getState());
      }
      pm.makePersistent(execution);

      processScheduledQueryPolicies(info);

      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, null);
    }
  }

  private void processScheduledQueryPolicies(ScheduledQueryProgressInfo info) throws MetaException {
    if (info.getState() != QueryState.FAILED && info.getState() != QueryState.TIMED_OUT) {
      return;
    }
    int autoDisableCount = MetastoreConf.getIntVar(conf, ConfVars.SCHEDULED_QUERIES_AUTODISABLE_COUNT);
    int skipCount = MetastoreConf.getIntVar(conf, ConfVars.SCHEDULED_QUERIES_SKIP_OPPORTUNITIES_AFTER_FAILURES);

    int lastN = Math.max(autoDisableCount, skipCount);
    if (lastN <= 0) {
      // disabled
      return;
    }

    boolean commited = false;
    Query query = null;
    try {
      openTransaction();

      MScheduledExecution lastExecution = pm.getObjectById(MScheduledExecution.class, info.getScheduledExecutionId());
      MScheduledQuery schq = lastExecution.getScheduledQuery();

      query = pm.newQuery(MScheduledExecution.class);
      query.setFilter("scheduledQuery == currentSchedule");
      query.setOrdering("scheduledExecutionId descending");
      query.declareParameters("MScheduledQuery currentSchedule");
      query.setRange(0, lastN);
      List<MScheduledExecution> list = (List<MScheduledExecution>) query.execute(schq);

      int failureCount=0;
      for(int i=0;i<list.size();i++) {
        if (list.get(i).getState() != QueryState.FAILED && list.get(i).getState() != QueryState.TIMED_OUT) {
          break;
        }
        failureCount++;
      }

      if (autoDisableCount > 0 && autoDisableCount <= failureCount) {
        LOG.info("Disabling {} after {} consequtive failures", schq.getScheduleKey(), autoDisableCount);
        schq.setEnabled(false);
        int now = (int) (System.currentTimeMillis() / 1000);
        MScheduledExecution execution = new MScheduledExecution();
        execution.setScheduledQuery(schq);
        execution.setState(QueryState.AUTO_DISABLED);
        execution.setStartTime(now);
        execution.setEndTime(now);
        execution.setLastUpdateTime(now);
        execution.setErrorMessage(String.format("Disabling query after {} consequtive failures", autoDisableCount));
        pm.makePersistent(execution);
      }
      if (skipCount > 0) {
        int n = Math.min(skipCount, failureCount) - 1;
        Integer scheduledTime = schq.getNextExecution();
        for (int i = 0; i < n; i++) {
          if (scheduledTime != null) {
            scheduledTime = computeNextExecutionTime(schq.getSchedule(), scheduledTime);
          }
        }
        if (scheduledTime != null) {
          schq.setNextExecution(scheduledTime);
        }
      }
      commited = commitTransaction();
    } catch (InvalidInputException e) {
      throw new MetaException("Unexpected InvalidInputException: " + e.getMessage());
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  /**
   * Abbreviates the error message to the given size.
   *
   * There might be error messages which may also contain a stack trace.
   */
  private String abbreviateErrorMessage(String errorMessage, int maxLength) {
    if (errorMessage.length() < maxLength) {
      return errorMessage;
    }
    String[] parts = errorMessage.split("\n", 2);
    return StringUtils.abbreviate(parts[0], maxLength);
  }

  @Override
  public void addReplicationMetrics(ReplicationMetricList replicationMetricList) {
    boolean commited = false;
    try {
      openTransaction();
      List<MReplicationMetrics> mReplicationMetricsList = new ArrayList<>();
      for (ReplicationMetrics replicationMetric : replicationMetricList.getReplicationMetricList()) {
        MReplicationMetrics mReplicationMetrics;
        try {
          mReplicationMetrics = pm.getObjectById(MReplicationMetrics.class,
            replicationMetric.getScheduledExecutionId());
        } catch (JDOObjectNotFoundException e) {
          mReplicationMetrics = new MReplicationMetrics();
          mReplicationMetrics.setDumpExecutionId(replicationMetric.getDumpExecutionId());
          mReplicationMetrics.setScheduledExecutionId(replicationMetric.getScheduledExecutionId());
          mReplicationMetrics.setPolicy(replicationMetric.getPolicy());
          mReplicationMetrics.setStartTime((int) (System.currentTimeMillis()/1000));
        }
        if (!StringUtils.isEmpty(replicationMetric.getMetadata())) {
          if (replicationMetric.getMetadata().length() > RM_METADATA_COL_WIDTH) {
            mReplicationMetrics.setProgress("RM_Metadata limit exceeded to " + replicationMetric.getMetadata().length());
          } else {
            mReplicationMetrics.setMetadata(replicationMetric.getMetadata());
          }
        }
        if (!StringUtils.isEmpty(replicationMetric.getProgress())) {
          // Check for the limit of RM_PROGRESS Column.
          if ((dbType.isORACLE() && replicationMetric.getProgress().length() > ORACLE_DB_MAX_COL_WIDTH)
              || replicationMetric.getProgress().length() > RM_PROGRESS_COL_WIDTH) {
            mReplicationMetrics.setProgress("RM_Progress limit exceeded to " + replicationMetric.getProgress().length());
          } else {
            mReplicationMetrics.setProgress(replicationMetric.getProgress());
          }
        }
        mReplicationMetrics.setMessageFormat(replicationMetric.getMessageFormat());
        mReplicationMetricsList.add(mReplicationMetrics);
      }
      pm.makePersistentAll(mReplicationMetricsList);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, null);
    }
  }

  @Override
  public ReplicationMetricList getReplicationMetrics(GetReplicationMetricsRequest replicationMetricsRequest) {
    boolean committed = false;
    try {
      openTransaction();
      ReplicationMetricList replicationMetrics = null;
      if (replicationMetricsRequest.isSetPolicy()) {
        replicationMetrics = getMReplicationMetrics(replicationMetricsRequest.getPolicy());
      } else if (replicationMetricsRequest.isSetScheduledExecutionId()) {
        replicationMetrics = getMReplicationMetrics(replicationMetricsRequest.getScheduledExecutionId());
      }
      committed = commitTransaction();
      return replicationMetrics;
    } finally {
      rollbackAndCleanup(committed, null);
    }
  }

  @Override
  public int deleteReplicationMetrics(int maxRetainSecs) {
    if (maxRetainSecs < 0) {
      LOG.debug("replication metrics deletion is disabled");
      return 0;
    }
    boolean committed = false;
    Query q = null;
    try {
      openTransaction();
      int maxCreateTime = (int) ((System.currentTimeMillis() / 1000) - maxRetainSecs);
      q = pm.newQuery(MReplicationMetrics.class);
      q.setFilter("startTime <= maxCreateTime");
      q.declareParameters("long maxCreateTime");
      long deleted = q.deletePersistentAll(maxCreateTime);
      committed = commitTransaction();
      return (int) deleted;
    } finally {
      rollbackAndCleanup(committed, q);
    }
  }

  private ReplicationMetricList getMReplicationMetrics(String policy) {
    ReplicationMetricList ret = new ReplicationMetricList();
    if (StringUtils.isEmpty(policy)) {
      return ret;
    }
    try (QueryWrapper query = new QueryWrapper(pm.newQuery(MReplicationMetrics.class, "policy == policyParam"))) {
      query.declareParameters("java.lang.String policyParam");
      query.setOrdering("scheduledExecutionId descending");
      List<MReplicationMetrics> list = (List<MReplicationMetrics>) query.execute(policy);
      List<ReplicationMetrics> returnList = new ArrayList<>();
      for (MReplicationMetrics mReplicationMetric : list) {
        pm.retrieve(mReplicationMetric);
        returnList.add(MReplicationMetrics.toThrift(mReplicationMetric));
      }
      ret.setReplicationMetricList(returnList);
      return ret;
    }
  }

  private ReplicationMetricList getMReplicationMetrics(long scheduledExecutionId) {
    ReplicationMetricList ret = new ReplicationMetricList();
    if (scheduledExecutionId < 0) {
      return ret;
    }
    try (QueryWrapper query = new QueryWrapper(pm.newQuery(MReplicationMetrics.class,
        "scheduledExecutionId == scheduledExecutionIdParam"))) {
      query.declareParameters("java.lang.Long scheduledExecutionIdParam");
      query.setOrdering("scheduledExecutionId descending");
      List<MReplicationMetrics> list = (List<MReplicationMetrics>) query.execute(scheduledExecutionId);
      List<ReplicationMetrics> returnList = new ArrayList<>();
      for (MReplicationMetrics mReplicationMetric : list) {
        pm.retrieve(mReplicationMetric);
        returnList.add(MReplicationMetrics.toThrift(mReplicationMetric));
      }
      ret.setReplicationMetricList(returnList);
      return ret;
    }
  }

  private void ensureScheduledQueriesEnabled() throws MetaException {
    if (!MetastoreConf.getBoolVar(conf, ConfVars.SCHEDULED_QUERIES_ENABLED)) {
      throw new MetaException(
          "Scheduled query request processing is disabled via " + ConfVars.SCHEDULED_QUERIES_ENABLED.getVarname());
    }
  }

  private boolean validateStateChange(QueryState from, QueryState to) {
    switch (from) {
    case INITED:
      return to != QueryState.INITED;
    case EXECUTING:
      return to == QueryState.FINISHED
          || to == QueryState.EXECUTING
          || to == QueryState.FAILED
          || to == QueryState.TIMED_OUT;
    default:
      return false;
    }
  }

  private Integer computeNextExecutionTime(String schedule) throws InvalidInputException {
    ZonedDateTime now = ZonedDateTime.now();
    return computeNextExecutionTime(schedule, now);
  }

  private Integer computeNextExecutionTime(String schedule, Integer time) throws InvalidInputException {
    ZonedDateTime now = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneId.systemDefault());
    return computeNextExecutionTime(schedule, now);
  }

  private Integer computeNextExecutionTime(String schedule, ZonedDateTime time) throws InvalidInputException {
    CronType cronType = CronType.QUARTZ;

    CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(cronType);
    CronParser parser = new CronParser(cronDefinition);

    // Get date for last execution
    try {
      ExecutionTime executionTime = ExecutionTime.forCron(parser.parse(schedule));
      Optional<ZonedDateTime> nextExecution = executionTime.nextExecution(time);
      if (!nextExecution.isPresent()) {
        // no valid next execution time.
        return null;
      }
      return (int) nextExecution.get().toEpochSecond();
    } catch (IllegalArgumentException iae) {
      String message = "Invalid " + cronType + " schedule expression: '" + schedule + "'";
      LOG.error(message, iae);
      throw new InvalidInputException(message);
    }
  }

  @Override
  public void scheduledQueryMaintenance(ScheduledQueryMaintenanceRequest request)
      throws MetaException, NoSuchObjectException, AlreadyExistsException, InvalidInputException {
    ensureScheduledQueriesEnabled();
    switch (request.getType()) {
    case CREATE:
      scheduledQueryInsert(request.getScheduledQuery());
      break;
    case ALTER:
      scheduledQueryUpdate(request.getScheduledQuery());
      break;
    case DROP:
      scheduledQueryDelete(request.getScheduledQuery());
      break;
    default:
      throw new MetaException("invalid request");
    }
  }

  public void scheduledQueryInsert(ScheduledQuery scheduledQuery)
      throws NoSuchObjectException, AlreadyExistsException, InvalidInputException {
    MScheduledQuery schq = MScheduledQuery.fromThrift(scheduledQuery);
    boolean commited = false;
    try {
      Optional<MScheduledQuery> existing = getMScheduledQuery(scheduledQuery.getScheduleKey());
      if (existing.isPresent()) {
        throw new AlreadyExistsException(
            "Scheduled query with name: " + scheduledQueryKeyRef(scheduledQuery.getScheduleKey()) + " already exists.");
      }
      openTransaction();
      Integer nextExecutionTime = computeNextExecutionTime(schq.getSchedule());
      schq.setNextExecution(nextExecutionTime);
      pm.makePersistent(schq);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, null);
    }
  }

  public void scheduledQueryDelete(ScheduledQuery scheduledQuery) throws NoSuchObjectException, AlreadyExistsException {
    MScheduledQuery schq = MScheduledQuery.fromThrift(scheduledQuery);
    boolean commited = false;
    try {
      openTransaction();
      Optional<MScheduledQuery> existing = getMScheduledQuery(scheduledQuery.getScheduleKey());
      if (!existing.isPresent()) {
        throw new NoSuchObjectException(
            "Scheduled query with name: " + scheduledQueryKeyRef(schq.getScheduleKey()) + " doesn't exists.");
      }
      MScheduledQuery persisted = existing.get();
      pm.deletePersistent(persisted);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, null);
    }
  }

  public void scheduledQueryUpdate(ScheduledQuery scheduledQuery)
      throws NoSuchObjectException, AlreadyExistsException, InvalidInputException {
    MScheduledQuery schq = MScheduledQuery.fromThrift(scheduledQuery);
    boolean commited = false;
    try {
      Optional<MScheduledQuery> existing = getMScheduledQuery(scheduledQuery.getScheduleKey());
      if (!existing.isPresent()) {
        throw new NoSuchObjectException(
            "Scheduled query with name: " + scheduledQueryKeyRef(scheduledQuery.getScheduleKey()) + " doesn't exists.");
      }
      openTransaction();
      MScheduledQuery persisted = existing.get();
      persisted.doUpdate(schq);
      if (!scheduledQuery.isSetNextExecution()) {
        Integer nextExecutionTime = computeNextExecutionTime(schq.getSchedule());
        persisted.setNextExecution(nextExecutionTime);
      } else {
        persisted.setNextExecution(schq.getNextExecution());
      }
      pm.makePersistent(persisted);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, null);
    }
  }

  @Override
  public ScheduledQuery getScheduledQuery(ScheduledQueryKey key) throws NoSuchObjectException {
    Optional<MScheduledQuery> mScheduledQuery = getMScheduledQuery(key);
    if (!mScheduledQuery.isPresent()) {
      throw new NoSuchObjectException(
          "There is no scheduled query for: " + scheduledQueryKeyRef(key));
    }
    return mScheduledQuery.get().toThrift();

  }

  private String scheduledQueryKeyRef(ScheduledQueryKey key) {
    return key.getScheduleName() + "@" + key.getClusterNamespace();
  }

  public Optional<MScheduledQuery> getMScheduledQuery(ScheduledQueryKey key) {
    MScheduledQuery s = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MScheduledQuery.class, "scheduleName == sName && clusterNamespace == ns");
      query.declareParameters("java.lang.String sName, java.lang.String ns");
      query.setUnique(true);
      s = (MScheduledQuery) query.execute(key.getScheduleName(), key.getClusterNamespace());
      pm.retrieve(s);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return Optional.ofNullable(s);
  }

  @Override
  public int deleteScheduledExecutions(int maxRetainSecs) {
    if (maxRetainSecs < 0) {
      LOG.debug("scheduled executions retention is disabled");
      return 0;
    }
    boolean committed = false;
    Query q = null;
    try {
      openTransaction();
      int maxCreateTime = (int) (System.currentTimeMillis() / 1000) - maxRetainSecs;
      q = pm.newQuery(MScheduledExecution.class);
      q.setFilter("startTime <= maxCreateTime");
      q.declareParameters("int maxCreateTime");
      long deleted = q.deletePersistentAll(maxCreateTime);
      committed = commitTransaction();
      return (int) deleted;
    } finally {
      rollbackAndCleanup(committed, q);
    }
  }

  @Override
  public int markScheduledExecutionsTimedOut(int timeoutSecs) throws InvalidOperationException, MetaException {
    if (timeoutSecs < 0) {
      LOG.debug("scheduled executions - time_out mark is disabled");
      return 0;
    }
    boolean committed = false;
    Query q = null;
    try {
      openTransaction();
      int maxLastUpdateTime = (int) (System.currentTimeMillis() / 1000) - timeoutSecs;
      q = pm.newQuery(MScheduledExecution.class);
      q.setFilter("lastUpdateTime <= maxLastUpdateTime && (state == 'INITED' || state == 'EXECUTING')");
      q.declareParameters("int maxLastUpdateTime");

      List<MScheduledExecution> results = (List<MScheduledExecution>) q.execute(maxLastUpdateTime);
      for (MScheduledExecution e : results) {

        ScheduledQueryProgressInfo info = new ScheduledQueryProgressInfo();
        info.setScheduledExecutionId(e.getScheduledExecutionId());
        info.setState(QueryState.TIMED_OUT);
        info.setErrorMessage(
            "Query stuck in: " + e.getState() + " state for >" + timeoutSecs + " seconds. Execution timed out.");
        //        info.set
        scheduledQueryProgress(info);
      }

      recoverInvalidScheduledQueryState(timeoutSecs);
      committed = commitTransaction();
      return results.size();
    } finally {
      rollbackAndCleanup(committed, q);
    }
  }

  private void recoverInvalidScheduledQueryState(int timeoutSecs) {
    int maxLastUpdateTime = (int) (System.currentTimeMillis() / 1000) - timeoutSecs;
    try (QueryWrapper q = new QueryWrapper(pm.newQuery(MScheduledQuery.class))) {
      q.setFilter("activeExecution != null");

      List<MScheduledQuery> results = (List<MScheduledQuery>) q.execute();
      for (MScheduledQuery e : results) {
        Integer lastUpdateTime = e.getActiveExecution().getLastUpdateTime();
        if (lastUpdateTime == null || lastUpdateTime < maxLastUpdateTime) {
          LOG.error("Scheduled query: {} stuck with an activeExecution - clearing",
              scheduledQueryKeyRef(e.getScheduleKey()));
          e.setActiveExecution(null);
          pm.makePersistent(e);
        }
      }
    }
  }
}
