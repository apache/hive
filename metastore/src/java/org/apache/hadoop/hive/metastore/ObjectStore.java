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

import static org.apache.commons.lang.StringUtils.join;

import java.net.URI;
import java.net.URISyntaxException;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jdo.JDODataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.DataStoreCache;
import javax.jdo.identity.IntIdentity;

import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.model.MColumnDescriptor;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MDelegationToken;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MIndex;
import org.apache.hadoop.hive.metastore.model.MMasterKey;
import org.apache.hadoop.hive.metastore.model.MOrder;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MPartitionEvent;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MRole;
import org.apache.hadoop.hive.metastore.model.MRoleMap;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MStringList;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.hadoop.hive.metastore.model.MType;
import org.apache.hadoop.hive.metastore.model.MVersionTable;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.ANTLRNoCaseStringStream;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.FilterBuilder;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.Operator;
import org.apache.hadoop.hive.metastore.parser.FilterLexer;
import org.apache.hadoop.hive.metastore.parser.FilterParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;

import org.antlr.runtime.Token;


/**
 * This class is the interface between the application logic and the database
 * store that contains the objects. Refrain putting any logic in mode.M* objects
 * or in this file as former could be auto generated and this class would need
 * to be made into a interface that can read both from a database and a
 * filestore.
 */
public class ObjectStore implements RawStore, Configurable {
  private static Properties prop = null;
  private static PersistenceManagerFactory pmf = null;

  private static Lock pmfPropLock = new ReentrantLock();
  private static final Log LOG = LogFactory.getLog(ObjectStore.class.getName());

  private static enum TXN_STATUS {
    NO_STATE, OPEN, COMMITED, ROLLBACK
  }

  private static final Map<String, Class> PINCLASSMAP;
  static {
    Map<String, Class> map = new HashMap();
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

  private boolean isInitialized = false;
  private PersistenceManager pm = null;
  private MetaStoreDirectSql directSql = null;
  private PartitionExpressionProxy expressionProxy = null;
  private Configuration hiveConf;
  int openTrasactionCalls = 0;
  private Transaction currentTransaction = null;
  private TXN_STATUS transactionStatus = TXN_STATUS.NO_STATE;
  private final AtomicBoolean isSchemaVerified = new AtomicBoolean(false);

  public ObjectStore() {
  }

  public Configuration getConf() {
    return hiveConf;
  }

  /**
   * Called whenever this object is instantiated using ReflectionUils, and also
   * on connection retries. In cases of connection retries, conf will usually
   * contain modified values.
   */
  @SuppressWarnings("nls")
  public void setConf(Configuration conf) {
    // Although an instance of ObjectStore is accessed by one thread, there may
    // be many threads with ObjectStore instances. So the static variables
    // pmf and prop need to be protected with locks.
    pmfPropLock.lock();
    try {
      isInitialized = false;
      hiveConf = conf;
      Properties propsFromConf = getDataSourceProps(conf);
      boolean propsChanged = !propsFromConf.equals(prop);

      if (propsChanged) {
        pmf = null;
        prop = null;
      }

      assert(!isActiveTransaction());
      shutdown();
      // Always want to re-create pm as we don't know if it were created by the
      // most recent instance of the pmf
      pm = null;
      directSql = null;
      expressionProxy = null;
      openTrasactionCalls = 0;
      currentTransaction = null;
      transactionStatus = TXN_STATUS.NO_STATE;

      initialize(propsFromConf);

      if (!isInitialized) {
        throw new RuntimeException(
        "Unable to create persistence manager. Check dss.log for details");
      } else {
        LOG.info("Initialized ObjectStore");
      }
    } finally {
      pmfPropLock.unlock();
    }
  }

  private ClassLoader classLoader;
  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = ObjectStore.class.getClassLoader();
    }
  }

  @SuppressWarnings("nls")
  private void initialize(Properties dsProps) {
    LOG.info("ObjectStore, initialize called");
    prop = dsProps;
    pm = getPersistenceManager();
    isInitialized = pm != null;
    if (isInitialized) {
      expressionProxy = createExpressionProxy(hiveConf);
      directSql = new MetaStoreDirectSql(pm);
    }
  }

  /**
   * Creates the proxy used to evaluate expressions. This is here to prevent circular
   * dependency - ql -&gt; metastore client &lt;-&gt metastore server -&gt ql. If server and
   * client are split, this can be removed.
   * @param conf Configuration.
   * @return The partition expression proxy.
   */
  private static PartitionExpressionProxy createExpressionProxy(Configuration conf) {
    String className = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS);
    try {
      @SuppressWarnings("unchecked")
      Class<? extends PartitionExpressionProxy> clazz =
          (Class<? extends PartitionExpressionProxy>)MetaStoreUtils.getClass(className);
      return (PartitionExpressionProxy)MetaStoreUtils.newInstance(
          clazz, new Class<?>[0], new Object[0]);
    } catch (MetaException e) {
      LOG.error("Error loading PartitionExpressionProxy", e);
      throw new RuntimeException("Error loading PartitionExpressionProxy: " + e.getMessage());
    }
  }

  /**
   * Properties specified in hive-default.xml override the properties specified
   * in jpox.properties.
   */
  @SuppressWarnings("nls")
  private static Properties getDataSourceProps(Configuration conf) {
    Properties prop = new Properties();

    Iterator<Map.Entry<String, String>> iter = conf.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> e = iter.next();
      if (e.getKey().contains("datanucleus") || e.getKey().contains("jdo")) {
        Object prevVal = prop.setProperty(e.getKey(), conf.get(e.getKey()));
        if (LOG.isDebugEnabled()
            && !e.getKey().equals(HiveConf.ConfVars.METASTOREPWD.varname)) {
          LOG.debug("Overriding " + e.getKey() + " value " + prevVal
              + " from  jpox.properties with " + e.getValue());
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      for (Entry<Object, Object> e : prop.entrySet()) {
        if (!e.getKey().equals(HiveConf.ConfVars.METASTOREPWD.varname)) {
          LOG.debug(e.getKey() + " = " + e.getValue());
        }
      }
    }
    return prop;
  }

  private static synchronized PersistenceManagerFactory getPMF() {
    if (pmf == null) {
      pmf = JDOHelper.getPersistenceManagerFactory(prop);
      DataStoreCache dsc = pmf.getDataStoreCache();
      if (dsc != null) {
        HiveConf conf = new HiveConf(ObjectStore.class);
        String objTypes = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CACHE_PINOBJTYPES);
        LOG.info("Setting MetaStore object pin classes with hive.metastore.cache.pinobjtypes=\"" + objTypes + "\"");
        if (objTypes != null && objTypes.length() > 0) {
          objTypes = objTypes.toLowerCase();
          String[] typeTokens = objTypes.split(",");
          for (String type : typeTokens) {
            type = type.trim();
            if (PINCLASSMAP.containsKey(type)) {
              dsc.pinAll(true, PINCLASSMAP.get(type));
            }
            else {
              LOG.warn(type + " is not one of the pinnable object types: " + org.apache.commons.lang.StringUtils.join(PINCLASSMAP.keySet(), " "));
            }
          }
        }
      } else {
        LOG.warn("PersistenceManagerFactory returned null DataStoreCache object. Unable to initialize object pin types defined by hive.metastore.cache.pinobjtypes");
      }
    }
    return pmf;
  }

  @InterfaceAudience.LimitedPrivate({"HCATALOG"})
  @InterfaceStability.Evolving
  public PersistenceManager getPersistenceManager() {
    return getPMF().getPersistenceManager();
  }

  public void shutdown() {
    if (pm != null) {
      pm.close();
    }
  }

  /**
   * Opens a new one or the one already created Every call of this function must
   * have corresponding commit or rollback function call
   *
   * @return an active transaction
   */

  public boolean openTransaction() {
    openTrasactionCalls++;
    if (openTrasactionCalls == 1) {
      currentTransaction = pm.currentTransaction();
      currentTransaction.begin();
      transactionStatus = TXN_STATUS.OPEN;
    } else {
      // something is wrong since openTransactionCalls is greater than 1 but
      // currentTransaction is not active
      assert ((currentTransaction != null) && (currentTransaction.isActive()));
    }
    return currentTransaction.isActive();
  }

  /**
   * if this is the commit of the first open call then an actual commit is
   * called.
   *
   * @return Always returns true
   */
  @SuppressWarnings("nls")
  public boolean commitTransaction() {
    if (TXN_STATUS.ROLLBACK == transactionStatus) {
      return false;
    }
    if (openTrasactionCalls <= 0) {
      throw new RuntimeException("commitTransaction was called but openTransactionCalls = "
          + openTrasactionCalls + ". This probably indicates that there are unbalanced " +
              "calls to openTransaction/commitTransaction");
    }
    if (!currentTransaction.isActive()) {
      throw new RuntimeException(
          "Commit is called, but transaction is not active. Either there are"
              + " mismatching open and close calls or rollback was called in the same trasaction");
    }
    openTrasactionCalls--;
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
  public boolean isActiveTransaction() {
    if (currentTransaction == null) {
      return false;
    }
    return currentTransaction.isActive();
  }

  /**
   * Rolls back the current transaction if it is active
   */
  public void rollbackTransaction() {
    if (openTrasactionCalls < 1) {
      return;
    }
    openTrasactionCalls = 0;
    if (currentTransaction.isActive()
        && transactionStatus != TXN_STATUS.ROLLBACK) {
      transactionStatus = TXN_STATUS.ROLLBACK;
      // could already be rolled back
      currentTransaction.rollback();
      // remove all detached objects from the cache, since the transaction is
      // being rolled back they are no longer relevant, and this prevents them
      // from reattaching in future transactions
      pm.evictAll();
    }
  }

  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    boolean commited = false;
    MDatabase mdb = new MDatabase();
    mdb.setName(db.getName().toLowerCase());
    mdb.setLocationUri(db.getLocationUri());
    mdb.setDescription(db.getDescription());
    mdb.setParameters(db.getParameters());
    try {
      openTransaction();
      pm.makePersistent(mdb);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
  }

  @SuppressWarnings("nls")
  private MDatabase getMDatabase(String name) throws NoSuchObjectException {
    MDatabase mdb = null;
    boolean commited = false;
    try {
      openTransaction();
      name = name.toLowerCase().trim();
      Query query = pm.newQuery(MDatabase.class, "name == dbname");
      query.declareParameters("java.lang.String dbname");
      query.setUnique(true);
      mdb = (MDatabase) query.execute(name);
      pm.retrieve(mdb);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    if (mdb == null) {
      throw new NoSuchObjectException("There is no database named " + name);
    }
    return mdb;
  }

  public Database getDatabase(String name) throws NoSuchObjectException {
    MDatabase mdb = null;
    boolean commited = false;
    try {
      openTransaction();
      mdb = getMDatabase(name);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    Database db = new Database();
    db.setName(mdb.getName());
    db.setDescription(mdb.getDescription());
    db.setLocationUri(mdb.getLocationUri());
    db.setParameters(mdb.getParameters());
    return db;
  }

  /**
   * Alter the database object in metastore. Currently only the parameters
   * of the database can be changed.
   * @param dbName the database name
   * @param db the Hive Database object
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  public boolean alterDatabase(String dbName, Database db)
    throws MetaException, NoSuchObjectException {

    MDatabase mdb = null;
    boolean committed = false;
    try {
      mdb = getMDatabase(dbName);
      // currently only allow changing database parameters
      mdb.setParameters(db.getParameters());
      openTransaction();
      pm.makePersistent(mdb);
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
        return false;
      }
    }
    return true;
  }

  public boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException {
    boolean success = false;
    LOG.info("Dropping database " + dbname + " along with all tables");
    dbname = dbname.toLowerCase();
    try {
      openTransaction();

      // then drop the database
      MDatabase db = getMDatabase(dbname);
      pm.retrieve(db);
      if (db != null) {
        List<MDBPrivilege> dbGrants = this.listDatabaseGrants(dbname);
        if (dbGrants != null && dbGrants.size() > 0) {
          pm.deletePersistentAll(dbGrants);
        }
        pm.deletePersistent(db);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }


  public List<String> getDatabases(String pattern) throws MetaException {
    boolean commited = false;
    List<String> databases = null;
    try {
      openTransaction();
      // Take the pattern and split it on the | to get all the composing
      // patterns
      String[] subpatterns = pattern.trim().split("\\|");
      String query = "select name from org.apache.hadoop.hive.metastore.model.MDatabase where (";
      boolean first = true;
      for (String subpattern : subpatterns) {
        subpattern = "(?i)" + subpattern.replaceAll("\\*", ".*");
        if (!first) {
          query = query + " || ";
        }
        query = query + " name.matches(\"" + subpattern + "\")";
        first = false;
      }
      query = query + ")";

      Query q = pm.newQuery(query);
      q.setResult("name");
      q.setOrdering("name ascending");
      Collection names = (Collection) q.execute();
      databases = new ArrayList<String>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        databases.add((String) i.next());
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return databases;
  }

  public List<String> getAllDatabases() throws MetaException {
    return getDatabases(".*");
  }

  private MType getMType(Type type) {
    List<MFieldSchema> fields = new ArrayList<MFieldSchema>();
    if (type.getFields() != null) {
      for (FieldSchema field : type.getFields()) {
        fields.add(new MFieldSchema(field.getName(), field.getType(), field
            .getComment()));
      }
    }
    return new MType(type.getName(), type.getType1(), type.getType2(), fields);
  }

  private Type getType(MType mtype) {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
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
      if (!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  public Type getType(String typeName) {
    Type type = null;
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MType.class, "name == typeName");
      query.declareParameters("java.lang.String typeName");
      query.setUnique(true);
      MType mtype = (MType) query.execute(typeName.trim());
      pm.retrieve(type);
      if (mtype != null) {
        type = getType(mtype);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return type;
  }

  public boolean dropType(String typeName) {
    boolean success = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MType.class, "name == typeName");
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
      LOG.debug("type not found " + typeName, e);
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MTable mtbl = convertToMTable(tbl);
      pm.makePersistent(mtbl);
      PrincipalPrivilegeSet principalPrivs = tbl.getPrivileges();
      List<Object> toPersistPrivObjs = new ArrayList<Object>();
      if (principalPrivs != null) {
        int now = (int)(System.currentTimeMillis()/1000);

        Map<String, List<PrivilegeGrantInfo>> userPrivs = principalPrivs.getUserPrivileges();
        putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, userPrivs, PrincipalType.USER);

        Map<String, List<PrivilegeGrantInfo>> groupPrivs = principalPrivs.getGroupPrivileges();
        putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, groupPrivs, PrincipalType.GROUP);

        Map<String, List<PrivilegeGrantInfo>> rolePrivs = principalPrivs.getRolePrivileges();
        putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, rolePrivs, PrincipalType.ROLE);
      }
      pm.makePersistentAll(toPersistPrivObjs);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
  }

  /**
   * Convert PrivilegeGrantInfo from privMap to MTablePrivilege, and add all of
   * them to the toPersistPrivObjs. These privilege objects will be persisted as
   * part of createTable.
   *
   * @param mtbl
   * @param toPersistPrivObjs
   * @param now
   * @param privMap
   * @param type
   */
  private void putPersistentPrivObjects(MTable mtbl, List<Object> toPersistPrivObjs,
      int now, Map<String, List<PrivilegeGrantInfo>> privMap, PrincipalType type) {
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
                  .isGrantOption());
          toPersistPrivObjs.add(mTblSec);
        }
      }
    }
  }

  public boolean dropTable(String dbName, String tableName) throws MetaException,
    NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean success = false;
    try {
      openTransaction();
      MTable tbl = getMTable(dbName, tableName);
      pm.retrieve(tbl);
      if (tbl != null) {
        // first remove all the grants
        List<MTablePrivilege> tabGrants = listAllTableGrants(dbName, tableName);
        if (tabGrants != null && tabGrants.size() > 0) {
          pm.deletePersistentAll(tabGrants);
        }
        List<MTableColumnPrivilege> tblColGrants = listTableAllColumnGrants(dbName,
            tableName);
        if (tblColGrants != null && tblColGrants.size() > 0) {
          pm.deletePersistentAll(tblColGrants);
        }

        List<MPartitionPrivilege> partGrants = this.listTableAllPartitionGrants(dbName, tableName);
        if (partGrants != null && partGrants.size() > 0) {
          pm.deletePersistentAll(partGrants);
        }

        List<MPartitionColumnPrivilege> partColGrants = listTableAllPartitionColumnGrants(dbName,
            tableName);
        if (partColGrants != null && partColGrants.size() > 0) {
          pm.deletePersistentAll(partColGrants);
        }
        // delete column statistics if present
        try {
          deleteTableColumnStatistics(dbName, tableName, null);
        } catch (NoSuchObjectException e) {
          LOG.info("Found no table level column statistics associated with db " + dbName +
          " table " + tableName + " record to delete");
        }

        preDropStorageDescriptor(tbl.getSd());
        // then remove the table
        pm.deletePersistentAll(tbl);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  public Table getTable(String dbName, String tableName) throws MetaException {
    boolean commited = false;
    Table tbl = null;
    try {
      openTransaction();
      tbl = convertToTable(getMTable(dbName, tableName));
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return tbl;
  }

  public List<String> getTables(String dbName, String pattern)
      throws MetaException {
    boolean commited = false;
    List<String> tbls = null;
    try {
      openTransaction();
      dbName = dbName.toLowerCase().trim();
      // Take the pattern and split it on the | to get all the composing
      // patterns
      String[] subpatterns = pattern.trim().split("\\|");
      String query =
        "select tableName from org.apache.hadoop.hive.metastore.model.MTable "
        + "where database.name == dbName && (";
      boolean first = true;
      for (String subpattern : subpatterns) {
        subpattern = "(?i)" + subpattern.replaceAll("\\*", ".*");
        if (!first) {
          query = query + " || ";
        }
        query = query + " tableName.matches(\"" + subpattern + "\")";
        first = false;
      }
      query = query + ")";

      Query q = pm.newQuery(query);
      q.declareParameters("java.lang.String dbName");
      q.setResult("tableName");
      q.setOrdering("tableName ascending");
      Collection names = (Collection) q.execute(dbName);
      tbls = new ArrayList<String>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        tbls.add((String) i.next());
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return tbls;
  }

  public List<String> getAllTables(String dbName) throws MetaException {
    return getTables(dbName, ".*");
  }

  private MTable getMTable(String db, String table) {
    MTable mtbl = null;
    boolean commited = false;
    try {
      openTransaction();
      db = db.toLowerCase().trim();
      table = table.toLowerCase().trim();
      Query query = pm.newQuery(MTable.class, "tableName == table && database.name == db");
      query.declareParameters("java.lang.String table, java.lang.String db");
      query.setUnique(true);
      mtbl = (MTable) query.execute(table, db);
      pm.retrieve(mtbl);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return mtbl;
  }

  public List<Table> getTableObjectsByName(String db, List<String> tbl_names)
      throws MetaException, UnknownDBException {
    List<Table> tables = new ArrayList<Table>();
    boolean committed = false;
    try {
      openTransaction();

      db = db.toLowerCase().trim();
      Query dbExistsQuery = pm.newQuery(MDatabase.class, "name == db");
      dbExistsQuery.declareParameters("java.lang.String db");
      dbExistsQuery.setUnique(true);
      dbExistsQuery.setResult("name");
      String dbNameIfExists = (String) dbExistsQuery.execute(db);
      if (dbNameIfExists == null || dbNameIfExists.isEmpty()) {
        throw new UnknownDBException("Could not find database " + db);
      }

      List<String> lowered_tbl_names = new ArrayList<String>();
      for (String t : tbl_names) {
        lowered_tbl_names.add(t.toLowerCase().trim());
      }
      Query query = pm.newQuery(MTable.class);
      query.setFilter("database.name == db && tbl_names.contains(tableName)");
      query.declareParameters("java.lang.String db, java.util.Collection tbl_names");
      Collection mtables = (Collection) query.execute(db, lowered_tbl_names);
      for (Iterator iter = mtables.iterator(); iter.hasNext();) {
        tables.add(convertToTable((MTable) iter.next()));
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
    return tables;
  }

  private Table convertToTable(MTable mtbl) throws MetaException {
    if (mtbl == null) {
      return null;
    }
    String tableType = mtbl.getTableType();
    if (tableType == null) {
      // for backwards compatibility with old metastore persistence
      if (mtbl.getViewOriginalText() != null) {
        tableType = TableType.VIRTUAL_VIEW.toString();
      } else if ("TRUE".equals(mtbl.getParameters().get("EXTERNAL"))) {
        tableType = TableType.EXTERNAL_TABLE.toString();
      } else {
        tableType = TableType.MANAGED_TABLE.toString();
      }
    }
    return new Table(mtbl.getTableName(), mtbl.getDatabase().getName(), mtbl
        .getOwner(), mtbl.getCreateTime(), mtbl.getLastAccessTime(), mtbl
        .getRetention(), convertToStorageDescriptor(mtbl.getSd()),
        convertToFieldSchemas(mtbl.getPartitionKeys()), mtbl.getParameters(),
        mtbl.getViewOriginalText(), mtbl.getViewExpandedText(),
        tableType);
  }

  private MTable convertToMTable(Table tbl) throws InvalidObjectException,
      MetaException {
    if (tbl == null) {
      return null;
    }
    MDatabase mdb = null;
    try {
      mdb = getMDatabase(tbl.getDbName());
    } catch (NoSuchObjectException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new InvalidObjectException("Database " + tbl.getDbName()
          + " doesn't exist.");
    }

    // If the table has property EXTERNAL set, update table type
    // accordingly
    String tableType = tbl.getTableType();
    boolean isExternal = "TRUE".equals(tbl.getParameters().get("EXTERNAL"));
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

    // A new table is always created with a new column descriptor
    return new MTable(tbl.getTableName().toLowerCase(), mdb,
        convertToMStorageDescriptor(tbl.getSd()), tbl.getOwner(), tbl
            .getCreateTime(), tbl.getLastAccessTime(), tbl.getRetention(),
        convertToMFieldSchemas(tbl.getPartitionKeys()), tbl.getParameters(),
        tbl.getViewOriginalText(), tbl.getViewExpandedText(),
        tableType);
  }

  private List<MFieldSchema> convertToMFieldSchemas(List<FieldSchema> keys) {
    List<MFieldSchema> mkeys = null;
    if (keys != null) {
      mkeys = new ArrayList<MFieldSchema>(keys.size());
      for (FieldSchema part : keys) {
        mkeys.add(new MFieldSchema(part.getName().toLowerCase(),
            part.getType(), part.getComment()));
      }
    }
    return mkeys;
  }

  private List<FieldSchema> convertToFieldSchemas(List<MFieldSchema> mkeys) {
    List<FieldSchema> keys = null;
    if (mkeys != null) {
      keys = new ArrayList<FieldSchema>(mkeys.size());
      for (MFieldSchema part : mkeys) {
        keys.add(new FieldSchema(part.getName(), part.getType(), part
            .getComment()));
      }
    }
    return keys;
  }

  private List<MOrder> convertToMOrders(List<Order> keys) {
    List<MOrder> mkeys = null;
    if (keys != null) {
      mkeys = new ArrayList<MOrder>(keys.size());
      for (Order part : keys) {
        mkeys.add(new MOrder(part.getCol().toLowerCase(), part.getOrder()));
      }
    }
    return mkeys;
  }

  private List<Order> convertToOrders(List<MOrder> mkeys) {
    List<Order> keys = null;
    if (mkeys != null) {
      keys = new ArrayList<Order>(mkeys.size());
      for (MOrder part : mkeys) {
        keys.add(new Order(part.getCol(), part.getOrder()));
      }
    }
    return keys;
  }

  private SerDeInfo converToSerDeInfo(MSerDeInfo ms) throws MetaException {
    if (ms == null) {
      throw new MetaException("Invalid SerDeInfo object");
    }
    return new SerDeInfo(ms.getName(), ms.getSerializationLib(), ms
        .getParameters());
  }

  private MSerDeInfo converToMSerDeInfo(SerDeInfo ms) throws MetaException {
    if (ms == null) {
      throw new MetaException("Invalid SerDeInfo object");
    }
    return new MSerDeInfo(ms.getName(), ms.getSerializationLib(), ms
        .getParameters());
  }

  /**
   * Given a list of model field schemas, create a new model column descriptor.
   * @param cols the columns the column descriptor contains
   * @return a new column descriptor db-backed object
   */
  private MColumnDescriptor createNewMColumnDescriptor(List<MFieldSchema> cols) {
    if (cols == null) {
      return null;
    }
    return new MColumnDescriptor(cols);
  }

  // MSD and SD should be same objects. Not sure how to make then same right now
  // MSerdeInfo *& SerdeInfo should be same as well
  private StorageDescriptor convertToStorageDescriptor(MStorageDescriptor msd,
      boolean noFS)
      throws MetaException {
    if (msd == null) {
      return null;
    }
    List<MFieldSchema> mFieldSchemas = msd.getCD() == null ? null : msd.getCD().getCols();

    StorageDescriptor sd = new StorageDescriptor(noFS ? null : convertToFieldSchemas(mFieldSchemas),
        msd.getLocation(), msd.getInputFormat(), msd.getOutputFormat(), msd
        .isCompressed(), msd.getNumBuckets(), converToSerDeInfo(msd
        .getSerDeInfo()), msd.getBucketCols(), convertToOrders(msd
        .getSortCols()), msd.getParameters());
    SkewedInfo skewedInfo = new SkewedInfo(msd.getSkewedColNames(),
        convertToSkewedValues(msd.getSkewedColValues()),
        covertToSkewedMap(msd.getSkewedColValueLocationMaps()));
    sd.setSkewedInfo(skewedInfo);
    sd.setStoredAsSubDirectories(msd.isStoredAsSubDirectories());
    return sd;
  }

  private StorageDescriptor convertToStorageDescriptor(MStorageDescriptor msd)
      throws MetaException {
    return convertToStorageDescriptor(msd, false);
  }

  /**
   * Convert a list of MStringList to a list of list string
   *
   * @param mLists
   * @return
   */
  private List<List<String>> convertToSkewedValues(List<MStringList> mLists) {
    List<List<String>> lists = null;
    if (mLists != null) {
      lists = new ArrayList<List<String>>(mLists.size());
      for (MStringList element : mLists) {
        lists.add(new ArrayList<String>(element.getInternalList()));
      }
    }
    return lists;
  }

  private List<MStringList> convertToMStringLists(List<List<String>> mLists) {
    List<MStringList> lists = null ;
    if (null != mLists) {
      lists = new ArrayList<MStringList>();
      for (List<String> mList : mLists) {
        lists.add(new MStringList(mList));
      }
    }
    return lists;
  }

  /**
   * Convert a MStringList Map to a Map
   * @param mMap
   * @return
   */
  private Map<List<String>, String> covertToSkewedMap(Map<MStringList, String> mMap) {
    Map<List<String>, String> map = null;
    if (mMap != null) {
      map = new HashMap<List<String>, String>(mMap.size());
      Set<MStringList> keys = mMap.keySet();
      for (MStringList key : keys) {
        map.put(new ArrayList<String>(key.getInternalList()), mMap.get(key));
      }
    }
    return map;
  }

  /**
   * Covert a Map to a MStringList Map
   * @param mMap
   * @return
   */
  private Map<MStringList, String> covertToMapMStringList(Map<List<String>, String> mMap) {
    Map<MStringList, String> map = null;
    if (mMap != null) {
      map = new HashMap<MStringList, String>(mMap.size());
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
   * @throws MetaException
   */
  private MStorageDescriptor convertToMStorageDescriptor(StorageDescriptor sd)
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
   * @throws MetaException
   */
  private MStorageDescriptor convertToMStorageDescriptor(StorageDescriptor sd,
      MColumnDescriptor mcd) throws MetaException {
    if (sd == null) {
      return null;
    }
    return new MStorageDescriptor(mcd, sd
        .getLocation(), sd.getInputFormat(), sd.getOutputFormat(), sd
        .isCompressed(), sd.getNumBuckets(), converToMSerDeInfo(sd
        .getSerdeInfo()), sd.getBucketCols(),
        convertToMOrders(sd.getSortCols()), sd.getParameters(),
        (null == sd.getSkewedInfo()) ? null
            : sd.getSkewedInfo().getSkewedColNames(),
        convertToMStringLists((null == sd.getSkewedInfo()) ? null : sd.getSkewedInfo()
            .getSkewedColValues()),
        covertToMapMStringList((null == sd.getSkewedInfo()) ? null : sd.getSkewedInfo()
            .getSkewedColValueLocationMaps()), sd.isStoredAsSubDirectories());
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    openTransaction();
    try {
      List<MTablePrivilege> tabGrants = null;
      List<MTableColumnPrivilege> tabColumnGrants = null;
      MTable table = this.getMTable(dbName, tblName);
      if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
        tabGrants = this.listAllTableGrants(dbName, tblName);
        tabColumnGrants = this.listTableAllColumnGrants(dbName, tblName);
      }
      List<Object> toPersist = new ArrayList<Object>();
      for (Partition part : parts) {
        if (!part.getTableName().equals(tblName) || !part.getDbName().equals(dbName)) {
          throw new MetaException("Partition does not belong to target table "
              + dbName + "." + tblName + ": " + part);
        }
        MPartition mpart = convertToMPart(part, true);
        toPersist.add(mpart);
        int now = (int)(System.currentTimeMillis()/1000);
        if (tabGrants != null) {
          for (MTablePrivilege tab: tabGrants) {
            toPersist.add(new MPartitionPrivilege(tab.getPrincipalName(),
                tab.getPrincipalType(), mpart, tab.getPrivilege(), now,
                tab.getGrantor(), tab.getGrantorType(), tab.getGrantOption()));
          }
        }

        if (tabColumnGrants != null) {
          for (MTableColumnPrivilege col : tabColumnGrants) {
            toPersist.add(new MPartitionColumnPrivilege(col.getPrincipalName(),
                col.getPrincipalType(), mpart, col.getColumnName(), col.getPrivilege(),
                now, col.getGrantor(), col.getGrantorType(), col.getGrantOption()));
          }
        }
      }
      if (toPersist.size() > 0) {
        pm.makePersistentAll(toPersist);
      }

      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException,
      MetaException {
    boolean success = false;
    boolean commited = false;
    try {
      MTable table = this.getMTable(part.getDbName(), part.getTableName());
      List<MTablePrivilege> tabGrants = null;
      List<MTableColumnPrivilege> tabColumnGrants = null;
      if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
        tabGrants = this.listAllTableGrants(part
            .getDbName(), part.getTableName());
        tabColumnGrants = this.listTableAllColumnGrants(
            part.getDbName(), part.getTableName());
      }
      openTransaction();
      MPartition mpart = convertToMPart(part, true);
      pm.makePersistent(mpart);

      int now = (int)(System.currentTimeMillis()/1000);
      List<Object> toPersist = new ArrayList<Object>();
      if (tabGrants != null) {
        for (MTablePrivilege tab: tabGrants) {
          MPartitionPrivilege partGrant = new MPartitionPrivilege(tab
              .getPrincipalName(), tab.getPrincipalType(),
              mpart, tab.getPrivilege(), now, tab.getGrantor(), tab
                  .getGrantorType(), tab.getGrantOption());
          toPersist.add(partGrant);
        }
      }

      if (tabColumnGrants != null) {
        for (MTableColumnPrivilege col : tabColumnGrants) {
          MPartitionColumnPrivilege partColumn = new MPartitionColumnPrivilege(col
              .getPrincipalName(), col.getPrincipalType(), mpart, col
              .getColumnName(), col.getPrivilege(), now, col.getGrantor(), col
              .getGrantorType(), col.getGrantOption());
          toPersist.add(partColumn);
        }

        if (toPersist.size() > 0) {
          pm.makePersistentAll(toPersist);
        }
      }

      commited = commitTransaction();
      success = true;
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  public Partition getPartition(String dbName, String tableName,
      List<String> part_vals) throws NoSuchObjectException, MetaException {
    openTransaction();
    Partition part = convertToPart(getMPartition(dbName, tableName, part_vals));
    commitTransaction();
    if(part == null) {
      throw new NoSuchObjectException("partition values="
          + part_vals.toString());
    }
    part.setValues(part_vals);
    return part;
  }

  private MPartition getMPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException {
    MPartition mpart = null;
    boolean commited = false;
    try {
      openTransaction();
      dbName = dbName.toLowerCase().trim();
      tableName = tableName.toLowerCase().trim();
      MTable mtbl = getMTable(dbName, tableName);
      if (mtbl == null) {
        commited = commitTransaction();
        return null;
      }
      // Change the query to use part_vals instead of the name which is
      // redundant
      String name = Warehouse.makePartName(convertToFieldSchemas(mtbl
          .getPartitionKeys()), part_vals);
      Query query = pm.newQuery(MPartition.class,
          "table.tableName == t1 && table.database.name == t2 && partitionName == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      query.setUnique(true);
      mpart = (MPartition) query.execute(tableName, dbName, name);
      pm.retrieve(mpart);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return mpart;
  }

  /**
   * Convert a Partition object into an MPartition, which is an object backed by the db
   * If the Partition's set of columns is the same as the parent table's AND useTableCD
   * is true, then this partition's storage descriptor's column descriptor will point
   * to the same one as the table's storage descriptor.
   * @param part the partition to convert
   * @param useTableCD whether to try to use the parent table's column descriptor.
   * @return the model partition object
   * @throws InvalidObjectException
   * @throws MetaException
   */
  private MPartition convertToMPart(Partition part, boolean useTableCD)
      throws InvalidObjectException, MetaException {
    if (part == null) {
      return null;
    }
    MTable mt = getMTable(part.getDbName(), part.getTableName());
    if (mt == null) {
      throw new InvalidObjectException(
          "Partition doesn't have a valid table or database name");
    }

    // If this partition's set of columns is the same as the parent table's,
    // use the parent table's, so we do not create a duplicate column descriptor,
    // thereby saving space
    MStorageDescriptor msd;
    if (useTableCD &&
        mt.getSd() != null && mt.getSd().getCD() != null &&
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

  private Partition convertToPart(MPartition mpart) throws MetaException {
    if (mpart == null) {
      return null;
    }
    return new Partition(mpart.getValues(), mpart.getTable().getDatabase()
        .getName(), mpart.getTable().getTableName(), mpart.getCreateTime(),
        mpart.getLastAccessTime(), convertToStorageDescriptor(mpart.getSd()),
        mpart.getParameters());
  }

  private Partition convertToPart(String dbName, String tblName, MPartition mpart)
      throws MetaException {
    if (mpart == null) {
      return null;
    }
    return new Partition(mpart.getValues(), dbName, tblName, mpart.getCreateTime(),
        mpart.getLastAccessTime(), convertToStorageDescriptor(mpart.getSd(), false),
        mpart.getParameters());
  }

  @Override
  public boolean dropPartition(String dbName, String tableName,
    List<String> part_vals) throws MetaException, NoSuchObjectException, InvalidObjectException,
    InvalidInputException {
    boolean success = false;
    try {
      openTransaction();
      MPartition part = getMPartition(dbName, tableName, part_vals);
      dropPartitionCommon(part);
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  /**
   * Drop an MPartition and cascade deletes (e.g., delete partition privilege grants,
   *   drop the storage descriptor cleanly, etc.)
   * @param part - the MPartition to drop
   * @return whether the transaction committed successfully
   * @throws InvalidInputException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  private boolean dropPartitionCommon(MPartition part) throws NoSuchObjectException, MetaException,
    InvalidObjectException, InvalidInputException {
    boolean success = false;
    try {
      openTransaction();
      if (part != null) {
        List<MFieldSchema> schemas = part.getTable().getPartitionKeys();
        List<String> colNames = new ArrayList<String>();
        for (MFieldSchema col: schemas) {
          colNames.add(col.getName());
        }
        String partName = FileUtils.makePartName(colNames, part.getValues());

        List<MPartitionPrivilege> partGrants = listPartitionGrants(
            part.getTable().getDatabase().getName(),
            part.getTable().getTableName(),
            partName);

        if (partGrants != null && partGrants.size() > 0) {
          pm.deletePersistentAll(partGrants);
        }

        List<MPartitionColumnPrivilege> partColumnGrants = listPartitionAllColumnGrants(
            part.getTable().getDatabase().getName(),
            part.getTable().getTableName(),
            partName);
        if (partColumnGrants != null && partColumnGrants.size() > 0) {
          pm.deletePersistentAll(partColumnGrants);
        }

        String dbName = part.getTable().getDatabase().getName();
        String tableName = part.getTable().getTableName();

        // delete partition level column stats if it exists
       try {
          deletePartitionColumnStatistics(dbName, tableName, partName, part.getValues(), null);
        } catch (NoSuchObjectException e) {
          LOG.info("No column statistics records found to delete");
        }

        preDropStorageDescriptor(part.getSd());
        pm.deletePersistent(part);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  public List<Partition> getPartitions(
      String dbName, String tableName, int maxParts) throws MetaException {
    return getPartitionsInternal(dbName, tableName, maxParts, true, true);
  }

  protected List<Partition> getPartitionsInternal(String dbName, String tblName,
      int maxParts, boolean allowSql, boolean allowJdo) throws MetaException {
    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();
    GetPartsHelper ctx = new GetPartsHelper(dbName, tblName, allowSql, allowJdo);
    try {
      ctx.start(false);
      if (ctx.canUseDirectSql()) {
        try {
          Integer max = (maxParts < 0) ? null : maxParts;
          ctx.setResult(directSql.getPartitions(dbName, tblName, max));
        } catch (Exception ex) {
          ctx.handleDirectSqlError(ex);
        }
      }

      if (!ctx.canUseDirectSql()) {
        ctx.setResult(convertToParts(listMPartitions(dbName, tblName, maxParts)));
      }
      return ctx.commit();
    } catch (NoSuchObjectException ex) {
      throw new MetaException(ex.getMessage());
    } finally {
      ctx.close();
    }
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName,
      short max, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    boolean success = false;
    try {
      openTransaction();
      List<MPartition> mparts = listMPartitions(dbName, tblName, max);
      List<Partition> parts = new ArrayList<Partition>(mparts.size());
      if (mparts != null && mparts.size()>0) {
        for (MPartition mpart : mparts) {
          MTable mtbl = mpart.getTable();
          Partition part = convertToPart(mpart);
          parts.add(part);

          if ("TRUE".equalsIgnoreCase(mtbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
            String partName = Warehouse.makePartName(this.convertToFieldSchemas(mtbl
                .getPartitionKeys()), part.getValues());
            PrincipalPrivilegeSet partAuth = this.getPartitionPrivilegeSet(dbName,
                tblName, partName, userName, groupNames);
            part.setPrivileges(partAuth);
          }
        }
      }
      success =  commitTransaction();
      return parts;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public Partition getPartitionWithAuth(String dbName, String tblName,
      List<String> partVals, String user_name, List<String> group_names)
      throws NoSuchObjectException, MetaException, InvalidObjectException {
    boolean success = false;
    try {
      openTransaction();
      MPartition mpart = getMPartition(dbName, tblName, partVals);
      if (mpart == null) {
        commitTransaction();
        throw new NoSuchObjectException("partition values="
            + partVals.toString());
      }
      Partition part = null;
      MTable mtbl = mpart.getTable();
      part = convertToPart(mpart);
      if ("TRUE".equalsIgnoreCase(mtbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
        String partName = Warehouse.makePartName(this.convertToFieldSchemas(mtbl
            .getPartitionKeys()), partVals);
        PrincipalPrivilegeSet partAuth = this.getPartitionPrivilegeSet(dbName,
            tblName, partName, user_name, group_names);
        part.setPrivileges(partAuth);
      }

      success = commitTransaction();
      return part;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }


  private List<Partition> convertToParts(List<MPartition> mparts) throws MetaException {
    return convertToParts(mparts, null);
  }

  private List<Partition> convertToParts(List<MPartition> src, List<Partition> dest)
      throws MetaException {
    if (src == null) {
      return dest;
    }
    if (dest == null) {
      dest = new ArrayList<Partition>(src.size());
    }
    for (MPartition mp : src) {
      dest.add(convertToPart(mp));
    }
    return dest;
  }

  private List<Partition> convertToParts(String dbName, String tblName, List<MPartition> mparts)
      throws MetaException {
    List<Partition> parts = new ArrayList<Partition>(mparts.size());
    for (MPartition mp : mparts) {
      parts.add(convertToPart(dbName, tblName, mp));
    }
    return parts;
  }

  // TODO:pc implement max
  public List<String> listPartitionNames(String dbName, String tableName,
      short max) throws MetaException {
    List<String> pns = null;
    boolean success = false;
    try {
      openTransaction();
      LOG.debug("Executing getPartitionNames");
      pns = getPartitionNamesNoTxn(dbName, tableName, max);
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return pns;
  }

  private List<String> getPartitionNamesNoTxn(String dbName, String tableName, short max) {
    List<String> pns = new ArrayList<String>();
    dbName = dbName.toLowerCase().trim();
    tableName = tableName.toLowerCase().trim();
    Query q = pm.newQuery(
        "select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
        + "where table.database.name == t1 && table.tableName == t2 "
        + "order by partitionName asc");
    q.declareParameters("java.lang.String t1, java.lang.String t2");
    q.setResult("partitionName");

    if(max > 0) {
      q.setRange(0, max);
    }
    Collection names = (Collection) q.execute(dbName, tableName);
    for (Iterator i = names.iterator(); i.hasNext();) {
      pns.add((String) i.next());
    }
    return pns;
  }

  /**
   * Retrieves a Collection of partition-related results from the database that match
   *  the partial specification given for a specific table.
   * @param dbName the name of the database
   * @param tableName the name of the table
   * @param part_vals the partial specification values
   * @param max_parts the maximum number of partitions to return
   * @param resultsCol the metadata column of the data to return, e.g. partitionName, etc.
   *        if resultsCol is empty or null, a collection of MPartition objects is returned
   * @throws NoSuchObjectException
   * @results A Collection of partition-related items from the db that match the partial spec
   *          for a table.  The type of each item in the collection corresponds to the column
   *          you want results for.  E.g., if resultsCol is partitionName, the Collection
   *          has types of String, and if resultsCol is null, the types are MPartition.
   */
  private Collection getPartitionPsQueryResults(String dbName, String tableName,
      List<String> part_vals, short max_parts, String resultsCol)
      throws MetaException, NoSuchObjectException {
    dbName = dbName.toLowerCase().trim();
    tableName = tableName.toLowerCase().trim();
    Table table = getTable(dbName, tableName);

    if (table == null) {
      throw new NoSuchObjectException(dbName + "." + tableName + " table not found");
    }

    List<FieldSchema> partCols = table.getPartitionKeys();
    int numPartKeys = partCols.size();
    if (part_vals.size() > numPartKeys) {
      throw new MetaException("Incorrect number of partition values");
    }

    partCols = partCols.subList(0, part_vals.size());
    //Construct a pattern of the form: partKey=partVal/partKey2=partVal2/...
    // where partVal is either the escaped partition value given as input,
    // or a regex of the form ".*"
    //This works because the "=" and "/" separating key names and partition key/values
    // are not escaped.
    String partNameMatcher = Warehouse.makePartName(partCols, part_vals, ".*");
    //add ".*" to the regex to match anything else afterwards the partial spec.
    if (part_vals.size() < numPartKeys) {
      partNameMatcher += ".*";
    }

    Query q = pm.newQuery(MPartition.class);
    StringBuilder queryFilter = new StringBuilder("table.database.name == dbName");
    queryFilter.append(" && table.tableName == tableName");
    queryFilter.append(" && partitionName.matches(partialRegex)");
    q.setFilter(queryFilter.toString());
    q.declareParameters("java.lang.String dbName, " +
        "java.lang.String tableName, java.lang.String partialRegex");

    if( max_parts >= 0 ) {
      //User specified a row limit, set it on the Query
      q.setRange(0, max_parts);
    }
    if (resultsCol != null && !resultsCol.isEmpty()) {
      q.setResult(resultsCol);
    }

    return (Collection) q.execute(dbName, tableName, partNameMatcher);
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    List<Partition> partitions = new ArrayList<Partition>();
    boolean success = false;
    try {
      openTransaction();
      LOG.debug("executing listPartitionNamesPsWithAuth");
      Collection parts = getPartitionPsQueryResults(db_name, tbl_name,
          part_vals, max_parts, null);
      MTable mtbl = getMTable(db_name, tbl_name);
      for (Object o : parts) {
        Partition part = convertToPart((MPartition) o);
        //set auth privileges
        if (null != userName && null != groupNames &&
            "TRUE".equalsIgnoreCase(mtbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
          String partName = Warehouse.makePartName(this.convertToFieldSchemas(mtbl
              .getPartitionKeys()), part.getValues());
          PrincipalPrivilegeSet partAuth = getPartitionPrivilegeSet(db_name,
              tbl_name, partName, userName, groupNames);
          part.setPrivileges(partAuth);
        }
        partitions.add(part);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return partitions;
  }

  @Override
  public List<String> listPartitionNamesPs(String dbName, String tableName,
      List<String> part_vals, short max_parts) throws MetaException, NoSuchObjectException {
    List<String> partitionNames = new ArrayList<String>();
    boolean success = false;
    try {
      openTransaction();
      LOG.debug("Executing listPartitionNamesPs");
      Collection names = getPartitionPsQueryResults(dbName, tableName,
          part_vals, max_parts, "partitionName");
      for (Object o : names) {
        partitionNames.add((String) o);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return partitionNames;
  }

  // TODO:pc implement max
  private List<MPartition> listMPartitions(String dbName, String tableName,
      int max) {

    boolean success = false;
    List<MPartition> mparts = null;
    try {
      openTransaction();
      LOG.debug("Executing listMPartitions");
      dbName = dbName.toLowerCase().trim();
      tableName = tableName.toLowerCase().trim();
      Query query = pm.newQuery(MPartition.class,
          "table.tableName == t1 && table.database.name == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      query.setOrdering("partitionName ascending");
      if(max > 0) {
        query.setRange(0, max);
      }
      mparts = (List<MPartition>) query.execute(tableName, dbName);
      LOG.debug("Done executing query for listMPartitions");
      pm.retrieveAll(mparts);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMPartitions " + mparts);
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mparts;
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    return getPartitionsByNamesInternal(dbName, tblName, partNames, true, true);
  }

  protected List<Partition> getPartitionsByNamesInternal(String dbName, String tblName,
      List<String> partNames, boolean allowSql, boolean allowJdo)
          throws MetaException, NoSuchObjectException {
    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();
    GetPartsHelper ctx = new GetPartsHelper(dbName, tblName, allowSql, allowJdo);
    try {
      ctx.start(false);
      if (ctx.canUseDirectSql()) {
        try {
          ctx.setResult(directSql.getPartitionsViaSqlFilter(dbName, tblName, partNames, null));
        } catch (Exception ex) {
          ctx.handleDirectSqlError(ex);
        }
      }

      if (!ctx.canUseDirectSql()) {
        ctx.setResult(getPartitionsViaOrmFilter(dbName, tblName, partNames));
      }
      return ctx.commit();
    } finally {
      ctx.close();
    }
  }

  @Override
  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
    return getPartitionsByExprInternal(
        dbName, tblName, expr, defaultPartitionName, maxParts, result, true, true);
  }

  protected boolean getPartitionsByExprInternal(String dbName, String tblName,
      byte[] expr, String defaultPartitionName, short maxParts, List<Partition> result,
      boolean allowSql, boolean allowJdo) throws TException {
    assert result != null;

    // We will try pushdown first, so make the filter. This will also validate the expression,
    // if serialization fails we will throw incompatible metastore error to the client.
    String filter = null;
    try {
      filter = expressionProxy.convertExprToFilter(expr);
    } catch (MetaException ex) {
      throw new IMetaStoreClient.IncompatibleMetastoreException(ex.getMessage());
    }

    // Make a tree out of the filter.
    // TODO: this is all pretty ugly. The only reason we need all these transformations
    //       is to maintain support for simple filters for HCat users that query metastore.
    //       If forcing everyone to use thick client is out of the question, maybe we could
    //       parse the filter into standard hive expressions and not all this separate tree
    //       Filter.g stuff. That way this method and ...ByFilter would just be merged.
    ExpressionTree exprTree = makeExpressionTree(filter);

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();
    GetPartsHelper ctx = new GetPartsHelper(dbName, tblName, allowSql, allowJdo);
    boolean hasUnknownPartitions = false;
    try {
      ctx.start(true);
      if (ctx.canUseDirectSql()) {
        try {
          // If we have some sort of expression tree, try SQL filter pushdown.
          boolean haveResult = (exprTree != null) && ctx.setResult(
              directSql.getPartitionsViaSqlFilter(ctx.getTable(), exprTree, null));
          if (!haveResult) {
            // We couldn't do SQL filter pushdown. Get names via normal means.
            List<String> partNames = new LinkedList<String>();
            hasUnknownPartitions = getPartitionNamesPrunedByExprNoTxn(
                ctx.getTable(), expr, defaultPartitionName, maxParts, partNames);
            ctx.setResult(directSql.getPartitionsViaSqlFilter(dbName, tblName, partNames, null));
          }
        } catch (Exception ex) {
          ctx.handleDirectSqlError(ex);
        }
      }

      if (!ctx.canUseDirectSql()) {
        // If we have some sort of expression tree, try JDOQL filter pushdown.
        boolean haveResult = (exprTree != null) && ctx.setResult(
            getPartitionsViaOrmFilter(ctx.getTable(), exprTree, maxParts, false));
        if (!haveResult) {
          // We couldn't do JDOQL filter pushdown. Get names via normal means.
          List<String> partNames = new ArrayList<String>();
          hasUnknownPartitions = getPartitionNamesPrunedByExprNoTxn(
              ctx.getTable(), expr, defaultPartitionName, maxParts, partNames);
          ctx.setResult(getPartitionsViaOrmFilter(dbName, tblName, partNames));
        }
      }
      result.addAll(ctx.commit());
    } finally {
      ctx.close();
    }
    return hasUnknownPartitions;
  }

  private class LikeChecker extends ExpressionTree.TreeVisitor {
    private boolean hasLike;

    public boolean hasLike() {
      return hasLike;
    }

    @Override
    protected boolean shouldStop() {
      return hasLike;
    }

    @Override
    protected void visit(LeafNode node) throws MetaException {
      hasLike = hasLike || (node.operator == Operator.LIKE);
    }
  }

  /**
   * Makes expression tree out of expr.
   * @param filter Filter.
   * @return Expression tree. Null if there was an error.
   */
  private ExpressionTree makeExpressionTree(String filter) throws MetaException {
    // TODO: ExprNodeDesc is an expression tree, we could just use that and be rid of Filter.g.
    if (filter == null || filter.isEmpty()) {
      return ExpressionTree.EMPTY_TREE;
    }
    LOG.debug("Filter specified is " + filter);
    ExpressionTree tree = null;
    try {
      tree = getFilterParser(filter).tree;
    } catch (MetaException ex) {
      LOG.info("Unable to make the expression tree from expression string ["
          + filter + "]" + ex.getMessage()); // Don't log the stack, this is normal.
    }
    if (tree == null) {
      return null;
    }
    // We suspect that LIKE pushdown into JDO is invalid; see HIVE-5134. Check for like here.
    LikeChecker lc = new LikeChecker();
    tree.accept(lc);
    return lc.hasLike() ? null : tree;
  }

  /**
   * Gets the partition names from a table, pruned using an expression.
   * @param table Table.
   * @param expr Expression.
   * @param defaultPartName Default partition name from job config, if any.
   * @param maxParts Maximum number of partition names to return.
   * @param result The resulting names.
   * @return Whether the result contains any unknown partitions.
   */
  private boolean getPartitionNamesPrunedByExprNoTxn(Table table, byte[] expr,
      String defaultPartName, short maxParts, List<String> result) throws MetaException {
    result.addAll(getPartitionNamesNoTxn(
        table.getDbName(), table.getTableName(), maxParts));
    List<String> columnNames = new ArrayList<String>();
    for (FieldSchema fs : table.getPartitionKeys()) {
      columnNames.add(fs.getName());
    }
    if (defaultPartName == null || defaultPartName.isEmpty()) {
      defaultPartName = HiveConf.getVar(getConf(), HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    }
    return expressionProxy.filterPartitionsByExpr(
        columnNames, expr, defaultPartName, result);
  }

  /**
   * Gets partition names from the table via ORM (JDOQL) filter pushdown.
   * @param table The table.
   * @param tree The expression tree from which JDOQL filter will be made.
   * @param maxParts Maximum number of partitions to return.
   * @param isValidatedFilter Whether the filter was pre-validated for JDOQL pushdown by a client
   *   (old hive client or non-hive one); if it was and we fail to create a filter, we will throw.
   * @return Resulting partitions. Can be null if isValidatedFilter is false, and
   *         there was error deriving the JDO filter.
   */
  private List<Partition> getPartitionsViaOrmFilter(Table table, ExpressionTree tree,
      short maxParts, boolean isValidatedFilter) throws MetaException {
    Map<String, Object> params = new HashMap<String, Object>();
    String jdoFilter = makeQueryFilterString(
        table.getDbName(), table, tree, params, isValidatedFilter);
    if (jdoFilter == null) {
      assert !isValidatedFilter;
      return null;
    }
    Query query = pm.newQuery(MPartition.class, jdoFilter);
    if (maxParts >= 0) {
      // User specified a row limit, set it on the Query
      query.setRange(0, maxParts);
    }

    String parameterDeclaration = makeParameterDeclarationStringObj(params);
    query.declareParameters(parameterDeclaration);
    query.setOrdering("partitionName ascending");

    @SuppressWarnings("unchecked")
    List<MPartition> mparts = (List<MPartition>) query.executeWithMap(params);

    LOG.debug("Done executing query for getPartitionsViaOrmFilter");
    pm.retrieveAll(mparts); // TODO: why is this inconsistent with what we get by names?
    LOG.debug("Done retrieving all objects for getPartitionsViaOrmFilter");
    List<Partition> results = convertToParts(mparts);
    query.closeAll();
    return results;
  }

  /**
   * Gets partition names from the table via ORM (JDOQL) name filter.
   * @param dbName Database name.
   * @param tblName Table name.
   * @param partNames Partition names to get the objects for.
   * @return Resulting partitions.
   */
  private List<Partition> getPartitionsViaOrmFilter(
      String dbName, String tblName, List<String> partNames) throws MetaException {
    if (partNames.isEmpty()) {
      return new ArrayList<Partition>();
    }
    StringBuilder sb = new StringBuilder(
        "table.tableName == t1 && table.database.name == t2 && (");
    int n = 0;
    Map<String, String> params = new HashMap<String, String>();
    for (Iterator<String> itr = partNames.iterator(); itr.hasNext();) {
      String pn = "p" + n;
      n++;
      String part = itr.next();
      params.put(pn, part);
      sb.append("partitionName == ").append(pn);
      sb.append(" || ");
    }
    sb.setLength(sb.length() - 4); // remove the last " || "
    sb.append(')');

    Query query = pm.newQuery(MPartition.class, sb.toString());

    LOG.debug(" JDOQL filter is " + sb.toString());
    params.put("t1", tblName.trim());
    params.put("t2", dbName.trim());

    String parameterDeclaration = makeParameterDeclarationString(params);

    query.declareParameters(parameterDeclaration);
    query.setOrdering("partitionName ascending");

    @SuppressWarnings("unchecked")
    List<MPartition> mparts = (List<MPartition>) query.executeWithMap(params);
    // pm.retrieveAll(mparts); // retrieveAll is pessimistic. some fields may not be needed
    List<Partition> results = convertToParts(dbName, tblName, mparts);
    // pm.makeTransientAll(mparts); // makeTransient will prohibit future access of unfetched fields
    query.closeAll();
    return results;
  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName, String tblName,
      String filter, short maxParts) throws MetaException, NoSuchObjectException {
    return getPartitionsByFilterInternal(dbName, tblName, filter, maxParts, true, true);
  }

  /** Helper class for getting partitions w/transaction, direct SQL, perf logging, etc. */
  private class GetPartsHelper {
    private final boolean isInTxn, doTrace, allowSql, allowJdo;
    private boolean doUseDirectSql;
    private long start;
    private Table table;
    private String dbName = null, tblName = null;
    boolean success = false;
    private List<Partition> results = null;

    public GetPartsHelper(String dbName, String tblName, boolean allowSql, boolean allowJdo)
        throws MetaException {
      assert allowSql || allowJdo;
      this.allowSql = allowSql;
      this.allowJdo = allowJdo;
      this.dbName = dbName;
      this.tblName = tblName;
      this.doTrace = LOG.isDebugEnabled();
      this.isInTxn = isActiveTransaction();

      // SQL usage inside a larger transaction (e.g. droptable) may not be desirable because
      // some databases (e.g. Postgres) abort the entire transaction when any query fails, so
      // the fallback from failed SQL to JDO is not possible.
      boolean isConfigEnabled = HiveConf.getBoolVar(getConf(), ConfVars.METASTORE_TRY_DIRECT_SQL)
          && (HiveConf.getBoolVar(getConf(), ConfVars.METASTORE_TRY_DIRECT_SQL_DDL) || !isInTxn);
      if (!allowJdo && isConfigEnabled && !directSql.isCompatibleDatastore()) {
        throw new MetaException("SQL is not operational"); // test path; SQL is enabled and broken.
      }
      this.doUseDirectSql = allowSql && isConfigEnabled && directSql.isCompatibleDatastore();
    }

    public void start(boolean initTable) throws MetaException, NoSuchObjectException {
      start = doTrace ? System.nanoTime() : 0;
      openTransaction();
      if (initTable) {
        table = ensureGetTable(dbName, tblName);
      }
    }

    public boolean setResult(List<Partition> results) {
      this.results = results;
      return this.results != null;
    }

    public void handleDirectSqlError(Exception ex) throws MetaException, NoSuchObjectException {
      LOG.error("Direct SQL failed" + (allowJdo ? ", falling back to ORM" : ""), ex);
      if (!allowJdo) {
        if (ex instanceof MetaException) {
          throw (MetaException)ex;
        }
        throw new MetaException(ex.getMessage());
      }
      if (!isInTxn) {
        rollbackTransaction();
        start = doTrace ? System.nanoTime() : 0;
        openTransaction();
        if (table != null) {
          table = ensureGetTable(dbName, tblName);
        }
      } else {
        start = doTrace ? System.nanoTime() : 0;
      }
      doUseDirectSql = false;
    }

    public void disableDirectSql() {
      this.doUseDirectSql = false;
    }

    public List<Partition> commit() {
      success = commitTransaction();
      if (doTrace) {
        LOG.debug(results.size() + " partition retrieved using " + (doUseDirectSql ? "SQL" : "ORM")
            + " in " + ((System.nanoTime() - start) / 1000000.0) + "ms");
      }
      return results;
    }

    public void close() {
      if (!success) {
        rollbackTransaction();
      }
    }

    public boolean canUseDirectSql() {
      return doUseDirectSql;
    }

    public Table getTable() {
      return table;
    }
  }

  protected List<Partition> getPartitionsByFilterInternal(String dbName, String tblName,
      String filter, short maxParts, boolean allowSql, boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    ExpressionTree tree = (filter != null && !filter.isEmpty())
        ? getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();
    GetPartsHelper ctx = new GetPartsHelper(dbName, tblName, allowSql, allowJdo);
    try {
      ctx.start(true);
      if (ctx.canUseDirectSql()) {
        try {
          if (!ctx.setResult(directSql.getPartitionsViaSqlFilter(
              ctx.getTable(), tree, (maxParts < 0) ? null : (int)maxParts))) {
            // Cannot push down SQL filter. The message has been logged internally.
            // This is not an error so don't roll back, just go to JDO.
            ctx.disableDirectSql();
          }
        } catch (Exception ex) {
          ctx.handleDirectSqlError(ex);
        }
      }

      if (!ctx.canUseDirectSql()) {
        ctx.setResult(getPartitionsViaOrmFilter(ctx.getTable(), tree, maxParts, true));
      }
      return ctx.commit();
    } finally {
      ctx.close();
    }
  }

  /**
   * Gets the table object for a given table, throws if anything goes wrong.
   * @param dbName Database name.
   * @param tblName Table name.
   * @return Table object.
   */
  private Table ensureGetTable(
      String dbName, String tblName) throws NoSuchObjectException, MetaException {
    MTable mtable = getMTable(dbName, tblName);
    if (mtable == null) {
      throw new NoSuchObjectException("Specified database/table does not exist : "
          + dbName + "." + tblName);
    }
    return convertToTable(mtable);
  }

  private FilterParser getFilterParser(String filter) throws MetaException {
    FilterLexer lexer = new FilterLexer(new ANTLRNoCaseStringStream(filter));
    CommonTokenStream tokens = new CommonTokenStream(lexer);

    FilterParser parser = new FilterParser(tokens);
    try {
      parser.filter();
    } catch(RecognitionException re) {
      throw new MetaException("Error parsing partition filter; lexer error: "
          + lexer.errorMsg + "; exception " + re);
    }

    if (lexer.errorMsg != null) {
      throw new MetaException("Error parsing partition filter : " + lexer.errorMsg);
    }
    return parser;
  }

  /**
   * Makes a JDO query filter string.
   * Makes a JDO query filter string for tables or partitions.
   * @param dbName Database name.
   * @param table Table. If null, the query returned is over tables in a database.
   *   If not null, the query returned is over partitions in a table.
   * @param filter The filter from which JDOQL filter will be made.
   * @param params Parameters for the filter. Some parameters may be added here.
   * @return Resulting filter.
   */
  private String makeQueryFilterString(String dbName, MTable mtable, String filter,
      Map<String, Object> params) throws MetaException {
    ExpressionTree tree = (filter != null && !filter.isEmpty())
        ? getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;
    return makeQueryFilterString(dbName, convertToTable(mtable), tree, params, true);
  }

  /**
   * Makes a JDO query filter string for tables or partitions.
   * @param dbName Database name.
   * @param table Table. If null, the query returned is over tables in a database.
   *   If not null, the query returned is over partitions in a table.
   * @param tree The expression tree from which JDOQL filter will be made.
   * @param params Parameters for the filter. Some parameters may be added here.
   * @param isValidatedFilter Whether the filter was pre-validated for JDOQL pushdown
   *   by the client; if it was and we fail to create a filter, we will throw.
   * @return Resulting filter. Can be null if isValidatedFilter is false, and there was error.
   */
  private String makeQueryFilterString(String dbName, Table table, ExpressionTree tree,
      Map<String, Object> params, boolean isValidatedFilter) throws MetaException {
    assert tree != null;
    FilterBuilder queryBuilder = new FilterBuilder(isValidatedFilter);
    if (table != null) {
      queryBuilder.append("table.tableName == t1 && table.database.name == t2");
      params.put("t1", table.getTableName());
      params.put("t2", table.getDbName());
    } else {
      queryBuilder.append("database.name == dbName");
      params.put("dbName", dbName);
    }

    tree.generateJDOFilterFragment(getConf(), table, params, queryBuilder);
    if (queryBuilder.hasError()) {
      assert !isValidatedFilter;
      LOG.info("JDO filter pushdown cannot be used: " + queryBuilder.getErrorMessage());
      return null;
    }
    String jdoFilter = queryBuilder.getFilter();
    LOG.debug("jdoFilter = " + jdoFilter);
    return jdoFilter;
  }

  private String makeParameterDeclarationString(Map<String, String> params) {
    //Create the parameter declaration string
    StringBuilder paramDecl = new StringBuilder();
    for (String key : params.keySet()) {
      paramDecl.append(", java.lang.String " + key);
    }
    return paramDecl.toString();
  }

  private String makeParameterDeclarationStringObj(Map<String, Object> params) {
    //Create the parameter declaration string
    StringBuilder paramDecl = new StringBuilder();
    for (Entry<String, Object> entry : params.entrySet()) {
      paramDecl.append(", ");
      paramDecl.append(entry.getValue().getClass().getName());
      paramDecl.append(" ");
      paramDecl.append(entry.getKey());
    }
    return paramDecl.toString();
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws MetaException {
    boolean success = false;
    List<String> tableNames = new ArrayList<String>();
    try {
      openTransaction();
      LOG.debug("Executing listTableNamesByFilter");
      dbName = dbName.toLowerCase().trim();
      Map<String, Object> params = new HashMap<String, Object>();
      String queryFilterString = makeQueryFilterString(dbName, null, filter, params);
      Query query = pm.newQuery(MTable.class);
      query.declareImports("import java.lang.String");
      query.setResult("tableName");
      query.setResultClass(java.lang.String.class);
      if (maxTables >= 0) {
        query.setRange(0, maxTables);
      }
      LOG.debug("filter specified is " + filter + "," + " JDOQL filter is " + queryFilterString);
      for (Entry<String, Object> entry : params.entrySet()) {
        LOG.debug("key: " + entry.getKey() + " value: " + entry.getValue() +
            " class: " + entry.getValue().getClass().getName());
      }
      String parameterDeclaration = makeParameterDeclarationStringObj(params);
      query.declareParameters(parameterDeclaration);
      query.setFilter(queryFilterString);
      Collection names = (Collection) query.executeWithMap(params);
      //have to emulate "distinct", otherwise tables with the same name may be returned
      Set<String> tableNamesSet = new HashSet<String>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        tableNamesSet.add((String) i.next());
      }
      tableNames = new ArrayList<String>(tableNamesSet);
      LOG.debug("Done executing query for listTableNamesByFilter");
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listTableNamesByFilter");

    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return tableNames;
  }

  @Override
  public List<String> listPartitionNamesByFilter(String dbName, String tableName,
      String filter, short maxParts) throws MetaException {
    boolean success = false;
    List<String> partNames = new ArrayList<String>();
    try {
      openTransaction();
      LOG.debug("Executing listMPartitionNamesByFilter");
      dbName = dbName.toLowerCase();
      tableName = tableName.toLowerCase();

      MTable mtable = getMTable(dbName, tableName);
      if( mtable == null ) {
        // To be consistent with the behavior of listPartitionNames, if the
        // table or db does not exist, we return an empty list
        return partNames;
      }
      Map<String, Object> params = new HashMap<String, Object>();
      String queryFilterString = makeQueryFilterString(dbName, mtable, filter, params);
      Query query = pm.newQuery(
          "select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
          + "where " + queryFilterString);

      if( maxParts >= 0 ) {
        //User specified a row limit, set it on the Query
        query.setRange(0, maxParts);
      }

      LOG.debug("Filter specified is " + filter + "," +
          " JDOQL filter is " + queryFilterString);
      LOG.debug("Parms is " + params);

      String parameterDeclaration = makeParameterDeclarationStringObj(params);
      query.declareParameters(parameterDeclaration);
      query.setOrdering("partitionName ascending");
      query.setResult("partitionName");

      Collection names = (Collection) query.executeWithMap(params);
      partNames = new ArrayList<String>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        partNames.add((String) i.next());
      }

      LOG.debug("Done executing query for listMPartitionNamesByFilter");
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMPartitionNamesByFilter");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return partNames;
  }

  public void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      name = name.toLowerCase();
      dbname = dbname.toLowerCase();
      MTable newt = convertToMTable(newTable);
      if (newt == null) {
        throw new InvalidObjectException("new table is invalid");
      }

      MTable oldt = getMTable(dbname, name);
      if (oldt == null) {
        throw new MetaException("table " + name + " doesn't exist");
      }

      // For now only alter name, owner, paramters, cols, bucketcols are allowed
      oldt.setTableName(newt.getTableName().toLowerCase());
      oldt.setParameters(newt.getParameters());
      oldt.setOwner(newt.getOwner());
      // Fully copy over the contents of the new SD into the old SD,
      // so we don't create an extra SD in the metastore db that has no references.
      copyMSD(newt.getSd(), oldt.getSd());
      oldt.setDatabase(newt.getDatabase());
      oldt.setRetention(newt.getRetention());
      oldt.setPartitionKeys(newt.getPartitionKeys());
      oldt.setTableType(newt.getTableType());
      oldt.setLastAccessTime(newt.getLastAccessTime());
      oldt.setViewOriginalText(newt.getViewOriginalText());
      oldt.setViewExpandedText(newt.getViewExpandedText());

      // commit the changes
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  public void alterIndex(String dbname, String baseTblName, String name, Index newIndex)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      name = name.toLowerCase();
      baseTblName = baseTblName.toLowerCase();
      dbname = dbname.toLowerCase();
      MIndex newi = convertToMIndex(newIndex);
      if (newi == null) {
        throw new InvalidObjectException("new index is invalid");
      }

      MIndex oldi = getMIndex(dbname, baseTblName, name);
      if (oldi == null) {
        throw new MetaException("index " + name + " doesn't exist");
      }

      // For now only alter paramters are allowed
      oldi.setParameters(newi.getParameters());

      // commit the changes
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  private void alterPartitionNoTxn(String dbname, String name, List<String> part_vals,
      Partition newPart) throws InvalidObjectException, MetaException {
    name = name.toLowerCase();
    dbname = dbname.toLowerCase();
    MPartition oldp = getMPartition(dbname, name, part_vals);
    MPartition newp = convertToMPart(newPart, false);
    if (oldp == null || newp == null) {
      throw new InvalidObjectException("partition does not exist.");
    }
    oldp.setValues(newp.getValues());
    oldp.setPartitionName(newp.getPartitionName());
    oldp.setParameters(newPart.getParameters());
    if (!TableType.VIRTUAL_VIEW.name().equals(oldp.getTable().getTableType())) {
      copyMSD(newp.getSd(), oldp.getSd());
    }
    if (newp.getCreateTime() != oldp.getCreateTime()) {
      oldp.setCreateTime(newp.getCreateTime());
    }
    if (newp.getLastAccessTime() != oldp.getLastAccessTime()) {
      oldp.setLastAccessTime(newp.getLastAccessTime());
    }
  }

  public void alterPartition(String dbname, String name, List<String> part_vals, Partition newPart)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    Exception e = null;
    try {
      openTransaction();
      alterPartitionNoTxn(dbname, name, part_vals, newPart);
      // commit the changes
      success = commitTransaction();
    } catch (Exception exception) {
      e = exception;
    } finally {
      if (!success) {
        rollbackTransaction();
        MetaException metaException = new MetaException(
            "The transaction for alter partition did not commit successfully.");
        if (e != null) {
          metaException.initCause(e);
        }
        throw metaException;
      }
    }
  }

  public void alterPartitions(String dbname, String name, List<List<String>> part_vals,
      List<Partition> newParts) throws InvalidObjectException, MetaException {
    boolean success = false;
    Exception e = null;
    try {
      openTransaction();
      Iterator<List<String>> part_val_itr = part_vals.iterator();
      for (Partition tmpPart: newParts) {
        List<String> tmpPartVals = part_val_itr.next();
        alterPartitionNoTxn(dbname, name, tmpPartVals, tmpPart);
      }
      // commit the changes
      success = commitTransaction();
    } catch (Exception exception) {
      e = exception;
    } finally {
      if (!success) {
        rollbackTransaction();
        MetaException metaException = new MetaException(
            "The transaction for alter partition did not commit successfully.");
        if (e != null) {
          metaException.initCause(e);
        }
        throw metaException;
      }
    }
  }

  private void copyMSD(MStorageDescriptor newSd, MStorageDescriptor oldSd) {
    oldSd.setLocation(newSd.getLocation());
    MColumnDescriptor oldCD = oldSd.getCD();
    // If the columns of the old column descriptor != the columns of the new one,
    // then change the old storage descriptor's column descriptor.
    // Convert the MFieldSchema's to their thrift object counterparts, because we maintain
    // datastore identity (i.e., identity of the model objects are managed by JDO,
    // not the application).
    if (!(oldSd != null && oldSd.getCD() != null &&
         oldSd.getCD().getCols() != null &&
         newSd != null && newSd.getCD() != null &&
         newSd.getCD().getCols() != null &&
         convertToFieldSchemas(newSd.getCD().getCols()).
         equals(convertToFieldSchemas(oldSd.getCD().getCols()))
       )) {
        oldSd.setCD(newSd.getCD());
    }

    //If oldCd does not have any more references, then we should delete it
    // from the backend db
    removeUnusedColumnDescriptor(oldCD);
    oldSd.setBucketCols(newSd.getBucketCols());
    oldSd.setCompressed(newSd.isCompressed());
    oldSd.setInputFormat(newSd.getInputFormat());
    oldSd.setOutputFormat(newSd.getOutputFormat());
    oldSd.setNumBuckets(newSd.getNumBuckets());
    oldSd.getSerDeInfo().setName(newSd.getSerDeInfo().getName());
    oldSd.getSerDeInfo().setSerializationLib(
        newSd.getSerDeInfo().getSerializationLib());
    oldSd.getSerDeInfo().setParameters(newSd.getSerDeInfo().getParameters());
    oldSd.setSkewedColNames(newSd.getSkewedColNames());
    oldSd.setSkewedColValues(newSd.getSkewedColValues());
    oldSd.setSkewedColValueLocationMaps(newSd.getSkewedColValueLocationMaps());
    oldSd.setSortCols(newSd.getSortCols());
    oldSd.setParameters(newSd.getParameters());
    oldSd.setStoredAsSubDirectories(newSd.isStoredAsSubDirectories());
  }

  /**
   * Checks if a column descriptor has any remaining references by storage descriptors
   * in the db.  If it does not, then delete the CD.  If it does, then do nothing.
   * @param oldCD the column descriptor to delete if it is no longer referenced anywhere
   */
  private void removeUnusedColumnDescriptor(MColumnDescriptor oldCD) {
    if (oldCD == null) {
      return;
    }

    boolean success = false;
    try {
      openTransaction();
      LOG.debug("execute removeUnusedColumnDescriptor");
      List<MStorageDescriptor> referencedSDs = listStorageDescriptorsWithCD(oldCD, 1);
      //if no other SD references this CD, we can throw it out.
      if (referencedSDs != null && referencedSDs.isEmpty()) {
        pm.retrieve(oldCD);
        pm.deletePersistent(oldCD);
      }
      success = commitTransaction();
      LOG.debug("successfully deleted a CD in removeUnusedColumnDescriptor");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  /**
   * Called right before an action that would drop a storage descriptor.
   * This function makes the SD's reference to a CD null, and then deletes the CD
   * if it no longer is referenced in the table.
   * @param msd the storage descriptor to drop
   */
  private void preDropStorageDescriptor(MStorageDescriptor msd) {
    if (msd == null || msd.getCD() == null) {
      return;
    }

    MColumnDescriptor mcd = msd.getCD();
    // Because there is a 1-N relationship between CDs and SDs,
    // we must set the SD's CD to null first before dropping the storage descriptor
    // to satisfy foriegn key constraints.
    msd.setCD(null);
    removeUnusedColumnDescriptor(mcd);
  }

  /**
   * Get a list of storage descriptors that reference a particular Column Descriptor
   * @param oldCD the column descriptor to get storage descriptors for
   * @param maxSDs the maximum number of SDs to return
   * @return a list of storage descriptors
   */
  private List<MStorageDescriptor> listStorageDescriptorsWithCD(MColumnDescriptor oldCD,
      long maxSDs) {
    boolean success = false;
    List<MStorageDescriptor> sds = null;
    try {
      openTransaction();
      LOG.debug("Executing listStorageDescriptorsWithCD");
      Query query = pm.newQuery(MStorageDescriptor.class,
          "this.cd == inCD");
      query.declareParameters("MColumnDescriptor inCD");
      if(maxSDs >= 0) {
        //User specified a row limit, set it on the Query
        query.setRange(0, maxSDs);
      }
      sds = (List<MStorageDescriptor>) query.execute(oldCD);
      LOG.debug("Done executing query for listStorageDescriptorsWithCD");
      pm.retrieveAll(sds);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listStorageDescriptorsWithCD");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return sds;
  }

  @Override
  public boolean addIndex(Index index) throws InvalidObjectException,
      MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MIndex idx = convertToMIndex(index);
      pm.makePersistent(idx);
      commited = commitTransaction();
      return true;
    } finally {
      if (!commited) {
        rollbackTransaction();
        return false;
      }
    }
  }

  private MIndex convertToMIndex(Index index) throws InvalidObjectException,
      MetaException {

    StorageDescriptor sd = index.getSd();
    if (sd == null) {
      throw new InvalidObjectException("Storage descriptor is not defined for index.");
    }

    MStorageDescriptor msd = this.convertToMStorageDescriptor(sd);
    MTable origTable = getMTable(index.getDbName(), index.getOrigTableName());
    if (origTable == null) {
      throw new InvalidObjectException(
          "Original table does not exist for the given index.");
    }

    MTable indexTable = getMTable(index.getDbName(), index.getIndexTableName());
    if (indexTable == null) {
      throw new InvalidObjectException(
          "Underlying index table does not exist for the given index.");
    }

    return new MIndex(index.getIndexName(), origTable, index.getCreateTime(),
        index.getLastAccessTime(), index.getParameters(), indexTable, msd,
        index.getIndexHandlerClass(), index.isDeferredRebuild());
  }

  @Override
  public boolean dropIndex(String dbName, String origTableName, String indexName)
      throws MetaException {
    boolean success = false;
    try {
      openTransaction();
      MIndex index = getMIndex(dbName, origTableName, indexName);
      if (index != null) {
        pm.deletePersistent(index);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  private MIndex getMIndex(String dbName, String originalTblName, String indexName) throws MetaException {
    MIndex midx = null;
    boolean commited = false;
    try {
      openTransaction();
      dbName = dbName.toLowerCase().trim();
      originalTblName = originalTblName.toLowerCase().trim();
      MTable mtbl = getMTable(dbName, originalTblName);
      if (mtbl == null) {
        commited = commitTransaction();
        return null;
      }

      Query query = pm.newQuery(MIndex.class,
        "origTable.tableName == t1 && origTable.database.name == t2 && indexName == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      query.setUnique(true);
      midx = (MIndex) query.execute(originalTblName, dbName, indexName);
      pm.retrieve(midx);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return midx;
  }

  @Override
  public Index getIndex(String dbName, String origTableName, String indexName)
      throws MetaException {
    openTransaction();
    MIndex mIndex = this.getMIndex(dbName, origTableName, indexName);
    Index ret = convertToIndex(mIndex);
    commitTransaction();
    return ret;
  }

  private Index convertToIndex(MIndex mIndex) throws MetaException {
    if(mIndex == null) {
      return null;
    }

    return new Index(
    mIndex.getIndexName(),
    mIndex.getIndexHandlerClass(),
    mIndex.getOrigTable().getDatabase().getName(),
    mIndex.getOrigTable().getTableName(),
    mIndex.getCreateTime(),
    mIndex.getLastAccessTime(),
    mIndex.getIndexTable().getTableName(),
    this.convertToStorageDescriptor(mIndex.getSd()),
    mIndex.getParameters(),
    mIndex.getDeferredRebuild());

  }

  @Override
  public List<Index> getIndexes(String dbName, String origTableName, int max)
      throws MetaException {
    boolean success = false;
    try {
      openTransaction();
      List<MIndex> mIndexList = listMIndexes(dbName, origTableName, max);
      List<Index> indexes = new ArrayList<Index>(mIndexList.size());
      for (MIndex midx : mIndexList) {
        indexes.add(this.convertToIndex(midx));
      }
      success = commitTransaction();
      return indexes;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  private List<MIndex> listMIndexes(String dbName, String origTableName,
      int max) {
    boolean success = false;
    List<MIndex> mindexes = null;
    try {
      openTransaction();
      LOG.debug("Executing listMIndexes");
      dbName = dbName.toLowerCase().trim();
      origTableName = origTableName.toLowerCase().trim();
      Query query = pm.newQuery(MIndex.class,
          "origTable.tableName == t1 && origTable.database.name == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mindexes = (List<MIndex>) query.execute(origTableName, dbName);
      LOG.debug("Done executing query for listMIndexes");
      pm.retrieveAll(mindexes);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMIndexes");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mindexes;
  }

  @Override
  public List<String> listIndexNames(String dbName, String origTableName,
      short max) throws MetaException {
    List<String> pns = new ArrayList<String>();
    boolean success = false;
    try {
      openTransaction();
      LOG.debug("Executing listIndexNames");
      dbName = dbName.toLowerCase().trim();
      origTableName = origTableName.toLowerCase().trim();
      Query q = pm.newQuery(
          "select indexName from org.apache.hadoop.hive.metastore.model.MIndex "
          + "where origTable.database.name == t1 && origTable.tableName == t2 "
          + "order by indexName asc");
      q.declareParameters("java.lang.String t1, java.lang.String t2");
      q.setResult("indexName");
      Collection names = (Collection) q.execute(dbName, origTableName);
      for (Iterator i = names.iterator(); i.hasNext();) {
        pns.add((String) i.next());
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return pns;
  }

  @Override
  public boolean addRole(String roleName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    boolean success = false;
    boolean commited = false;
    try {
      openTransaction();
      MRole nameCheck = this.getMRole(roleName);
      if (nameCheck != null) {
        throw new InvalidObjectException("Role " + roleName + " already exists.");
      }
      int now = (int)(System.currentTimeMillis()/1000);
      MRole mRole = new MRole(roleName, now,
          ownerName);
      pm.makePersistent(mRole);
      commited = commitTransaction();
      success = true;
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  @Override
  public boolean grantRole(Role role, String userName,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption) throws MetaException, NoSuchObjectException,InvalidObjectException {
    boolean success = false;
    boolean commited = false;
    try {
      MRoleMap roleMap = null;
      try {
        roleMap = this.getMSecurityUserRoleMap(userName, principalType, role
            .getRoleName());
      } catch (Exception e) {
      }
      if (roleMap != null) {
        throw new InvalidObjectException("Principal " + userName
            + " already has the role " + role.getRoleName());
      }
      openTransaction();
      MRole mRole = getMRole(role.getRoleName());
      long now = System.currentTimeMillis()/1000;
      MRoleMap roleMember = new MRoleMap(userName, principalType.toString(),
          mRole, (int) now, grantor, grantorType.toString(), grantOption);
      pm.makePersistent(roleMember);
      commited = commitTransaction();
      success = true;
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType) throws MetaException, NoSuchObjectException {
    boolean success = false;
    try {
      openTransaction();
      MRoleMap roleMember = getMSecurityUserRoleMap(userName, principalType,
          role.getRoleName());
      pm.deletePersistent(roleMember);
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  private MRoleMap getMSecurityUserRoleMap(String userName,
      PrincipalType principalType, String roleName) {
    MRoleMap mRoleMember = null;
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MRoleMap.class, "principalName == t1 && principalType == t2 && role.roleName == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      query.setUnique(true);
      mRoleMember = (MRoleMap) query.executeWithArray(userName, principalType.toString(), roleName);
      pm.retrieve(mRoleMember);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return mRoleMember;
  }

  @Override
  public boolean removeRole(String roleName) throws MetaException,
      NoSuchObjectException {
    boolean success = false;
    try {
      openTransaction();
      MRole mRol = getMRole(roleName);
      pm.retrieve(mRol);
      if (mRol != null) {
        // first remove all the membership, the membership that this role has
        // been granted
        List<MRoleMap> roleMap = listRoleMembers(mRol);
        if (roleMap.size() > 0) {
          pm.deletePersistentAll(roleMap);
        }
        List<MRoleMap> roleMember = listMSecurityPrincipalMembershipRole(mRol
            .getRoleName(), PrincipalType.ROLE);
        if (roleMember.size() > 0) {
          pm.deletePersistentAll(roleMember);
        }
        // then remove all the grants
        List<MGlobalPrivilege> userGrants = listPrincipalGlobalGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (userGrants.size() > 0) {
          pm.deletePersistentAll(userGrants);
        }
        List<MDBPrivilege> dbGrants = listPrincipalAllDBGrant(mRol
            .getRoleName(), PrincipalType.ROLE);
        if (dbGrants.size() > 0) {
          pm.deletePersistentAll(dbGrants);
        }
        List<MTablePrivilege> tabPartGrants = listPrincipalAllTableGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (tabPartGrants.size() > 0) {
          pm.deletePersistentAll(tabPartGrants);
        }
        List<MPartitionPrivilege> partGrants = listPrincipalAllPartitionGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (partGrants.size() > 0) {
          pm.deletePersistentAll(partGrants);
        }
        List<MTableColumnPrivilege> tblColumnGrants = listPrincipalAllTableColumnGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (tblColumnGrants.size() > 0) {
          pm.deletePersistentAll(tblColumnGrants);
        }
        List<MPartitionColumnPrivilege> partColumnGrants = listPrincipalAllPartitionColumnGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (tblColumnGrants.size() > 0) {
          pm.deletePersistentAll(partColumnGrants);
        }
        // finally remove the role
        pm.deletePersistent(mRol);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  private List<MRoleMap> listRoles(String userName,
      List<String> groupNames) {
    List<MRoleMap> ret = new ArrayList<MRoleMap>();
    if(userName != null) {
      ret.addAll(listRoles(userName, PrincipalType.USER));
    }
    if (groupNames != null) {
      for (String groupName: groupNames) {
        ret.addAll(listRoles(groupName, PrincipalType.GROUP));
      }
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<MRoleMap> listRoles(String principalName,
      PrincipalType principalType) {
    boolean success = false;
    List<MRoleMap> mRoleMember = null;
    try {
      openTransaction();
      LOG.debug("Executing listRoles");
      Query query = pm
          .newQuery(
              MRoleMap.class,
              "principalName == t1 && principalType == t2");
      query
          .declareParameters("java.lang.String t1, java.lang.String t2");
      query.setUnique(false);
      mRoleMember = (List<MRoleMap>) query.executeWithArray(
          principalName, principalType.toString());
      LOG.debug("Done executing query for listMSecurityUserRoleMap");
      pm.retrieveAll(mRoleMember);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMSecurityUserRoleMap");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mRoleMember;
  }

  @SuppressWarnings("unchecked")
  private List<MRoleMap> listMSecurityPrincipalMembershipRole(final String roleName,
      final PrincipalType principalType) {
    boolean success = false;
    List<MRoleMap> mRoleMemebership = null;
    try {
      openTransaction();
      LOG.debug("Executing listMSecurityPrincipalMembershipRole");
      Query query = pm.newQuery(MRoleMap.class,
          "principalName == t1 && principalType == t2");
      query
          .declareParameters("java.lang.String t1, java.lang.String t2");
      mRoleMemebership = (List<MRoleMap>) query.execute(roleName, principalType.toString());
      LOG
          .debug("Done executing query for listMSecurityPrincipalMembershipRole");
      pm.retrieveAll(mRoleMemebership);
      success = commitTransaction();
      LOG
          .debug("Done retrieving all objects for listMSecurityPrincipalMembershipRole");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mRoleMemebership;
  }

  public Role getRole(String roleName) throws NoSuchObjectException {
    MRole mRole = this.getMRole(roleName);
    if (mRole == null) {
      throw new NoSuchObjectException(roleName + " role can not be found.");
    }
    Role ret = new Role(mRole.getRoleName(), mRole.getCreateTime(), mRole
        .getOwnerName());
    return ret;
  }

  private MRole getMRole(String roleName) {
    MRole mrole = null;
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MRole.class, "roleName == t1");
      query.declareParameters("java.lang.String t1");
      query.setUnique(true);
      mrole = (MRole) query.execute(roleName);
      pm.retrieve(mrole);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return mrole;
  }

  public List<String> listRoleNames() {
    boolean success = false;
    try {
      openTransaction();
      LOG.debug("Executing listAllRoleNames");
      Query query = pm.newQuery("select roleName from org.apache.hadoop.hive.metastore.model.MRole");
      query.setResult("roleName");
      Collection names = (Collection) query.execute();
      List<String> roleNames  = new ArrayList<String>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        roleNames.add((String) i.next());
      }
      success = commitTransaction();
      return roleNames;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    try {
      openTransaction();
      if (userName != null) {
        List<MGlobalPrivilege> user = this.listPrincipalGlobalGrants(userName, PrincipalType.USER);
        if(user.size()>0) {
          Map<String, List<PrivilegeGrantInfo>> userPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
          List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(user.size());
          for (int i = 0; i < user.size(); i++) {
            MGlobalPrivilege item = user.get(i);
            grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
                .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
                .getGrantorType()), item.getGrantOption()));
          }
          userPriv.put(userName, grantInfos);
          ret.setUserPrivileges(userPriv);
        }
      }
      if (groupNames != null && groupNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> groupPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for(String groupName: groupNames) {
          List<MGlobalPrivilege> group = this.listPrincipalGlobalGrants(groupName, PrincipalType.GROUP);
          if(group.size()>0) {
            List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(group.size());
            for (int i = 0; i < group.size(); i++) {
              MGlobalPrivilege item = group.get(i);
              grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
                  .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
                  .getGrantorType()), item.getGrantOption()));
            }
            groupPriv.put(groupName, grantInfos);
          }
        }
        ret.setGroupPrivileges(groupPriv);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  public List<PrivilegeGrantInfo> getDBPrivilege(String dbName,
      String principalName, PrincipalType principalType)
      throws InvalidObjectException, MetaException {
    dbName = dbName.toLowerCase().trim();

    if (principalName != null) {
      List<MDBPrivilege> userNameDbPriv = this.listPrincipalDBGrants(
          principalName, principalType, dbName);
      if (userNameDbPriv != null && userNameDbPriv.size() > 0) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(
            userNameDbPriv.size());
        for (int i = 0; i < userNameDbPriv.size(); i++) {
          MDBPrivilege item = userNameDbPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    }
    return new ArrayList<PrivilegeGrantInfo>(0);
  }


  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName,
      String userName, List<String> groupNames) throws InvalidObjectException,
      MetaException {
    boolean commited = false;
    dbName = dbName.toLowerCase().trim();

    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> dbUserPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        dbUserPriv.put(userName, getDBPrivilege(dbName, userName,
            PrincipalType.USER));
        ret.setUserPrivileges(dbUserPriv);
      }
      if (groupNames != null && groupNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> dbGroupPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String groupName : groupNames) {
          dbGroupPriv.put(groupName, getDBPrivilege(dbName, groupName,
              PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(dbGroupPriv);
      }
      List<MRoleMap> roles = listRoles(userName, groupNames);
      if (roles != null && roles.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> dbRolePriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (MRoleMap role : roles) {
          String name = role.getRole().getRoleName();
          dbRolePriv
              .put(name, getDBPrivilege(dbName, name, PrincipalType.ROLE));
        }
        ret.setRolePrivileges(dbRolePriv);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName,
      String tableName, String partition, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();

    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> partUserPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        partUserPriv.put(userName, getPartitionPrivilege(dbName,
            tableName, partition, userName, PrincipalType.USER));
        ret.setUserPrivileges(partUserPriv);
      }
      if (groupNames != null && groupNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> partGroupPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String groupName : groupNames) {
          partGroupPriv.put(groupName, getPartitionPrivilege(dbName, tableName,
              partition, groupName, PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(partGroupPriv);
      }
      List<MRoleMap> roles = listRoles(userName, groupNames);
      if (roles != null && roles.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> partRolePriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (MRoleMap role : roles) {
          String roleName = role.getRole().getRoleName();
          partRolePriv.put(roleName, getPartitionPrivilege(dbName, tableName,
              partition, roleName, PrincipalType.ROLE));
        }
        ret.setRolePrivileges(partRolePriv);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName,
      String tableName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();

    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> tableUserPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        tableUserPriv.put(userName, getTablePrivilege(dbName,
            tableName, userName, PrincipalType.USER));
        ret.setUserPrivileges(tableUserPriv);
      }
      if (groupNames != null && groupNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> tableGroupPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String groupName : groupNames) {
          tableGroupPriv.put(groupName, getTablePrivilege(dbName, tableName,
              groupName, PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(tableGroupPriv);
      }
      List<MRoleMap> roles = listRoles(userName, groupNames);
      if (roles != null && roles.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> tableRolePriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (MRoleMap role : roles) {
          String roleName = role.getRole().getRoleName();
          tableRolePriv.put(roleName, getTablePrivilege(dbName, tableName,
              roleName, PrincipalType.ROLE));
        }
        ret.setRolePrivileges(tableRolePriv);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName,
      String tableName, String partitionName, String columnName,
      String userName, List<String> groupNames) throws InvalidObjectException,
      MetaException {
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();
    columnName = columnName.toLowerCase().trim();

    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> columnUserPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        columnUserPriv.put(userName, getColumnPrivilege(dbName, tableName,
            columnName, partitionName, userName, PrincipalType.USER));
        ret.setUserPrivileges(columnUserPriv);
      }
      if (groupNames != null && groupNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> columnGroupPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String groupName : groupNames) {
          columnGroupPriv.put(groupName, getColumnPrivilege(dbName, tableName,
              columnName, partitionName, groupName, PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(columnGroupPriv);
      }
      List<MRoleMap> roles = listRoles(userName, groupNames);
      if (roles != null && roles.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> columnRolePriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (MRoleMap role : roles) {
          String roleName = role.getRole().getRoleName();
          columnRolePriv.put(roleName, getColumnPrivilege(dbName, tableName,
              columnName, partitionName, roleName, PrincipalType.ROLE));
        }
        ret.setRolePrivileges(columnRolePriv);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  private List<PrivilegeGrantInfo> getPartitionPrivilege(String dbName,
      String tableName, String partName, String principalName,
      PrincipalType principalType) {

    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();

    if (principalName != null) {
      List<MPartitionPrivilege> userNameTabPartPriv = this
          .listPrincipalPartitionGrants(principalName, principalType,
              dbName, tableName, partName);
      if (userNameTabPartPriv != null && userNameTabPartPriv.size() > 0) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(
            userNameTabPartPriv.size());
        for (int i = 0; i < userNameTabPartPriv.size(); i++) {
          MPartitionPrivilege item = userNameTabPartPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(),
              getPrincipalTypeFromStr(item.getGrantorType()), item.getGrantOption()));

        }
        return grantInfos;
      }
    }
    return new ArrayList<PrivilegeGrantInfo>(0);
  }

  private PrincipalType getPrincipalTypeFromStr(String str) {
    return str == null ? null : PrincipalType.valueOf(str);
  }

  private List<PrivilegeGrantInfo> getTablePrivilege(String dbName,
      String tableName, String principalName, PrincipalType principalType) {
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();

    if (principalName != null) {
      List<MTablePrivilege> userNameTabPartPriv = this
          .listAllTableGrants(principalName, principalType,
              dbName, tableName);
      if (userNameTabPartPriv != null && userNameTabPartPriv.size() > 0) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(
            userNameTabPartPriv.size());
        for (int i = 0; i < userNameTabPartPriv.size(); i++) {
          MTablePrivilege item = userNameTabPartPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    }
    return new ArrayList<PrivilegeGrantInfo>(0);
  }

  private List<PrivilegeGrantInfo> getColumnPrivilege(String dbName,
      String tableName, String columnName, String partitionName,
      String principalName, PrincipalType principalType) {

    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();
    columnName = columnName.toLowerCase().trim();

    if (partitionName == null) {
      List<MTableColumnPrivilege> userNameColumnPriv = this
          .listPrincipalTableColumnGrants(principalName, principalType,
              dbName, tableName, columnName);
      if (userNameColumnPriv != null && userNameColumnPriv.size() > 0) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(
            userNameColumnPriv.size());
        for (int i = 0; i < userNameColumnPriv.size(); i++) {
          MTableColumnPrivilege item = userNameColumnPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    } else {
      List<MPartitionColumnPrivilege> userNameColumnPriv = this
          .listPrincipalPartitionColumnGrants(principalName,
              principalType, dbName, tableName, partitionName, columnName);
      if (userNameColumnPriv != null && userNameColumnPriv.size() > 0) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(
            userNameColumnPriv.size());
        for (int i = 0; i < userNameColumnPriv.size(); i++) {
          MPartitionColumnPrivilege item = userNameColumnPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    }
    return new ArrayList<PrivilegeGrantInfo>(0);
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    boolean committed = false;
    int now = (int) (System.currentTimeMillis() / 1000);
    try {
      openTransaction();
      List<Object> persistentObjs = new ArrayList<Object>();

      List<HiveObjectPrivilege> privilegeList = privileges.getPrivileges();

      if (privilegeList != null && privilegeList.size() > 0) {
        Iterator<HiveObjectPrivilege> privIter = privilegeList.iterator();
        Set<String> privSet = new HashSet<String>();
        while (privIter.hasNext()) {
          HiveObjectPrivilege privDef = privIter.next();
          HiveObjectRef hiveObject = privDef.getHiveObject();
          String privilegeStr = privDef.getGrantInfo().getPrivilege();
          String[] privs = privilegeStr.split(",");
          String userName = privDef.getPrincipalName();
          PrincipalType principalType = privDef.getPrincipalType();
          String grantor = privDef.getGrantInfo().getGrantor();
          String grantorType = privDef.getGrantInfo().getGrantorType().toString();
          boolean grantOption = privDef.getGrantInfo().isGrantOption();
          privSet.clear();

          if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
            List<MGlobalPrivilege> globalPrivs = this
                .listPrincipalGlobalGrants(userName, principalType);
            if (globalPrivs != null) {
              for (MGlobalPrivilege priv : globalPrivs) {
                if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                  privSet.add(priv.getPrivilege());
                }
              }
            }
            for (String privilege : privs) {
              if (privSet.contains(privilege)) {
                throw new InvalidObjectException(privilege
                    + " is already granted by " + grantor);
              }
              MGlobalPrivilege mGlobalPrivs = new MGlobalPrivilege(userName,
                  principalType.toString(), privilege, now, grantor, grantorType, grantOption);
              persistentObjs.add(mGlobalPrivs);
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
            MDatabase dbObj = getMDatabase(hiveObject.getDbName());
            if (dbObj != null) {
              List<MDBPrivilege> dbPrivs = this.listPrincipalDBGrants(
                  userName, principalType, hiveObject.getDbName());
              if (dbPrivs != null) {
                for (MDBPrivilege priv : dbPrivs) {
                  if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                    privSet.add(priv.getPrivilege());
                  }
                }
              }
              for (String privilege : privs) {
                if (privSet.contains(privilege)) {
                  throw new InvalidObjectException(privilege
                      + " is already granted on database "
                      + hiveObject.getDbName() + " by " + grantor);
                }
                MDBPrivilege mDb = new MDBPrivilege(userName, principalType
                    .toString(), dbObj, privilege, now, grantor, grantorType, grantOption);
                persistentObjs.add(mDb);
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
            MTable tblObj = getMTable(hiveObject.getDbName(), hiveObject
                .getObjectName());
            if (tblObj != null) {
              List<MTablePrivilege> tablePrivs = this
                  .listAllTableGrants(userName, principalType,
                      hiveObject.getDbName(), hiveObject.getObjectName());
              if (tablePrivs != null) {
                for (MTablePrivilege priv : tablePrivs) {
                  if (priv.getGrantor() != null
                      && priv.getGrantor().equalsIgnoreCase(grantor)) {
                    privSet.add(priv.getPrivilege());
                  }
                }
              }
              for (String privilege : privs) {
                if (privSet.contains(privilege)) {
                  throw new InvalidObjectException(privilege
                      + " is already granted on table ["
                      + hiveObject.getDbName() + ","
                      + hiveObject.getObjectName() + "] by " + grantor);
                }
                MTablePrivilege mTab = new MTablePrivilege(
                    userName, principalType.toString(), tblObj,
                    privilege, now, grantor, grantorType, grantOption);
                persistentObjs.add(mTab);
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
            MPartition partObj = this.getMPartition(hiveObject.getDbName(),
                hiveObject.getObjectName(), hiveObject.getPartValues());
            String partName = null;
            if (partObj != null) {
              partName = partObj.getPartitionName();
              List<MPartitionPrivilege> partPrivs = this
                  .listPrincipalPartitionGrants(userName,
                      principalType, hiveObject.getDbName(), hiveObject
                          .getObjectName(), partObj.getPartitionName());
              if (partPrivs != null) {
                for (MPartitionPrivilege priv : partPrivs) {
                  if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                    privSet.add(priv.getPrivilege());
                  }
                }
              }
              for (String privilege : privs) {
                if (privSet.contains(privilege)) {
                  throw new InvalidObjectException(privilege
                      + " is already granted on partition ["
                      + hiveObject.getDbName() + ","
                      + hiveObject.getObjectName() + ","
                      + partName + "] by " + grantor);
                }
                MPartitionPrivilege mTab = new MPartitionPrivilege(userName,
                    principalType.toString(), partObj, privilege, now, grantor,
                    grantorType, grantOption);
                persistentObjs.add(mTab);
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
            MTable tblObj = getMTable(hiveObject.getDbName(), hiveObject
                .getObjectName());
            if (tblObj != null) {
              if (hiveObject.getPartValues() != null) {
                MPartition partObj = null;
                List<MPartitionColumnPrivilege> colPrivs = null;
                partObj = this.getMPartition(hiveObject.getDbName(), hiveObject
                    .getObjectName(), hiveObject.getPartValues());
                if (partObj == null) {
                  continue;
                }
                colPrivs = this.listPrincipalPartitionColumnGrants(
                    userName, principalType, hiveObject.getDbName(), hiveObject
                        .getObjectName(), partObj.getPartitionName(),
                    hiveObject.getColumnName());

                if (colPrivs != null) {
                  for (MPartitionColumnPrivilege priv : colPrivs) {
                    if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                      privSet.add(priv.getPrivilege());
                    }
                  }
                }
                for (String privilege : privs) {
                  if (privSet.contains(privilege)) {
                    throw new InvalidObjectException(privilege
                        + " is already granted on column "
                        + hiveObject.getColumnName() + " ["
                        + hiveObject.getDbName() + ","
                        + hiveObject.getObjectName() + ","
                        + partObj.getPartitionName() + "] by " + grantor);
                  }
                  MPartitionColumnPrivilege mCol = new MPartitionColumnPrivilege(userName,
                      principalType.toString(), partObj, hiveObject
                          .getColumnName(), privilege, now, grantor, grantorType,
                      grantOption);
                  persistentObjs.add(mCol);
                }

              } else {
                List<MTableColumnPrivilege> colPrivs = null;
                colPrivs = this.listPrincipalTableColumnGrants(
                    userName, principalType, hiveObject.getDbName(), hiveObject
                        .getObjectName(), hiveObject.getColumnName());

                if (colPrivs != null) {
                  for (MTableColumnPrivilege priv : colPrivs) {
                    if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                      privSet.add(priv.getPrivilege());
                    }
                  }
                }
                for (String privilege : privs) {
                  if (privSet.contains(privilege)) {
                    throw new InvalidObjectException(privilege
                        + " is already granted on column "
                        + hiveObject.getColumnName() + " ["
                        + hiveObject.getDbName() + ","
                        + hiveObject.getObjectName() + "] by " + grantor);
                  }
                  MTableColumnPrivilege mCol = new MTableColumnPrivilege(userName,
                      principalType.toString(), tblObj, hiveObject
                          .getColumnName(), privilege, now, grantor, grantorType,
                      grantOption);
                  persistentObjs.add(mCol);
                }
              }
            }
          }
        }
      }
      if (persistentObjs.size() > 0) {
        pm.makePersistentAll(persistentObjs);
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
    return committed;
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    boolean committed = false;
    try {
      openTransaction();
      List<Object> persistentObjs = new ArrayList<Object>();

      List<HiveObjectPrivilege> privilegeList = privileges.getPrivileges();


      if (privilegeList != null && privilegeList.size() > 0) {
        Iterator<HiveObjectPrivilege> privIter = privilegeList.iterator();

        while (privIter.hasNext()) {
          HiveObjectPrivilege privDef = privIter.next();
          HiveObjectRef hiveObject = privDef.getHiveObject();
          String privilegeStr = privDef.getGrantInfo().getPrivilege();
          if (privilegeStr == null || privilegeStr.trim().equals("")) {
            continue;
          }
          String[] privs = privilegeStr.split(",");
          String userName = privDef.getPrincipalName();
          PrincipalType principalType = privDef.getPrincipalType();

          if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
            List<MGlobalPrivilege> mSecUser = this.listPrincipalGlobalGrants(
                userName, principalType);
            boolean found = false;
            if (mSecUser != null) {
              for (String privilege : privs) {
                for (MGlobalPrivilege userGrant : mSecUser) {
                  String userGrantPrivs = userGrant.getPrivilege();
                  if (privilege.equals(userGrantPrivs)) {
                    found = true;
                    persistentObjs.add(userGrant);
                    break;
                  }
                }
                if (!found) {
                  throw new InvalidObjectException(
                      "No user grant found for privileges " + privilege);
                }
              }
            }

          } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
            MDatabase dbObj = getMDatabase(hiveObject.getDbName());
            if (dbObj != null) {
              String db = hiveObject.getDbName();
              boolean found = false;
              List<MDBPrivilege> dbGrants = this.listPrincipalDBGrants(
                  userName, principalType, db);
              for (String privilege : privs) {
                for (MDBPrivilege dbGrant : dbGrants) {
                  String dbGrantPriv = dbGrant.getPrivilege();
                  if (privilege.equals(dbGrantPriv)) {
                    found = true;
                    persistentObjs.add(dbGrant);
                    break;
                  }
                }
                if (!found) {
                  throw new InvalidObjectException(
                      "No database grant found for privileges " + privilege
                          + " on database " + db);
                }
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
            boolean found = false;
            List<MTablePrivilege> tableGrants = this
                .listAllTableGrants(userName, principalType,
                    hiveObject.getDbName(), hiveObject.getObjectName());
            for (String privilege : privs) {
              for (MTablePrivilege tabGrant : tableGrants) {
                String tableGrantPriv = tabGrant.getPrivilege();
                if (privilege.equalsIgnoreCase(tableGrantPriv)) {
                  found = true;
                  persistentObjs.add(tabGrant);
                  break;
                }
              }
              if (!found) {
                throw new InvalidObjectException("No grant (" + privilege
                    + ") found " + " on table " + hiveObject.getObjectName()
                    + ", database is " + hiveObject.getDbName());
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {

            boolean found = false;
            Table tabObj = this.getTable(hiveObject.getDbName(), hiveObject.getObjectName());
            String partName = null;
            if (hiveObject.getPartValues() != null) {
              partName = Warehouse.makePartName(tabObj.getPartitionKeys(), hiveObject.getPartValues());
            }
            List<MPartitionPrivilege> partitionGrants = this
                .listPrincipalPartitionGrants(userName, principalType,
                    hiveObject.getDbName(), hiveObject.getObjectName(), partName);
            for (String privilege : privs) {
              for (MPartitionPrivilege partGrant : partitionGrants) {
                String partPriv = partGrant.getPrivilege();
                if (partPriv.equalsIgnoreCase(privilege)) {
                  found = true;
                  persistentObjs.add(partGrant);
                  break;
                }
              }
              if (!found) {
                throw new InvalidObjectException("No grant (" + privilege
                    + ") found " + " on table " + tabObj.getTableName()
                    + ", partition is " + partName + ", database is " + tabObj.getDbName());
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {

            Table tabObj = this.getTable(hiveObject.getDbName(), hiveObject
                .getObjectName());
            String partName = null;
            if (hiveObject.getPartValues() != null) {
              partName = Warehouse.makePartName(tabObj.getPartitionKeys(),
                  hiveObject.getPartValues());
            }

            if (partName != null) {
              List<MPartitionColumnPrivilege> mSecCol = listPrincipalPartitionColumnGrants(
                  userName, principalType, hiveObject.getDbName(), hiveObject
                      .getObjectName(), partName, hiveObject.getColumnName());
              boolean found = false;
              if (mSecCol != null) {
                for (String privilege : privs) {
                  for (MPartitionColumnPrivilege col : mSecCol) {
                    String colPriv = col.getPrivilege();
                    if (colPriv.equalsIgnoreCase(privilege)) {
                      found = true;
                      persistentObjs.add(col);
                      break;
                    }
                  }
                  if (!found) {
                    throw new InvalidObjectException("No grant (" + privilege
                        + ") found " + " on table " + tabObj.getTableName()
                        + ", partition is " + partName + ", column name = "
                        + hiveObject.getColumnName() + ", database is "
                        + tabObj.getDbName());
                  }
                }
              }
            } else {
              List<MTableColumnPrivilege> mSecCol = listPrincipalTableColumnGrants(
                  userName, principalType, hiveObject.getDbName(), hiveObject
                      .getObjectName(), hiveObject.getColumnName());
              boolean found = false;
              if (mSecCol != null) {
                for (String privilege : privs) {
                  for (MTableColumnPrivilege col : mSecCol) {
                    String colPriv = col.getPrivilege();
                    if (colPriv.equalsIgnoreCase(privilege)) {
                      found = true;
                      persistentObjs.add(col);
                      break;
                    }
                  }
                  if (!found) {
                    throw new InvalidObjectException("No grant (" + privilege
                        + ") found " + " on table " + tabObj.getTableName()
                        + ", column name = "
                        + hiveObject.getColumnName() + ", database is "
                        + tabObj.getDbName());
                  }
                }
              }
            }

          }
        }
      }

      if (persistentObjs.size() > 0) {
        pm.deletePersistentAll(persistentObjs);
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
    return committed;
  }

  @SuppressWarnings("unchecked")
  private List<MRoleMap> listRoleMembers(
      MRole mRol) {
    boolean success = false;
    List<MRoleMap> mRoleMemeberList = null;
    try {
      openTransaction();
      LOG.debug("Executing listMSecurityUserRoleMember");
      Query query = pm.newQuery(MRoleMap.class,
          "role.roleName == t1");
      query.declareParameters("java.lang.String t1");
      query.setUnique(false);
      mRoleMemeberList = (List<MRoleMap>) query.execute(
          mRol.getRoleName());
      LOG.debug("Done executing query for listMSecurityUserRoleMember");
      pm.retrieveAll(mRoleMemeberList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMSecurityUserRoleMember");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mRoleMemeberList;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<MGlobalPrivilege> listPrincipalGlobalGrants(String principalName, PrincipalType principalType) {
    boolean commited = false;
    List<MGlobalPrivilege> userNameDbPriv = null;
    try {
      openTransaction();
      if (principalName != null) {
        Query query = pm.newQuery(MGlobalPrivilege.class,
            "principalName == t1 && principalType == t2 ");
        query.declareParameters(
            "java.lang.String t1, java.lang.String t2");
        userNameDbPriv = (List<MGlobalPrivilege>) query
            .executeWithArray(principalName, principalType.toString());
        pm.retrieveAll(userNameDbPriv);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return userNameDbPriv;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<MDBPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String dbName) {
    boolean success = false;
    List<MDBPrivilege> mSecurityDBList = null;
    dbName = dbName.toLowerCase().trim();

    try {
      openTransaction();
      LOG.debug("Executing listPrincipalDBGrants");
      Query query = pm.newQuery(MDBPrivilege.class,
          "principalName == t1 && principalType == t2 && database.name == t3");
      query
          .declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      mSecurityDBList = (List<MDBPrivilege>) query.executeWithArray(principalName, principalType.toString(), dbName);
      LOG.debug("Done executing query for listPrincipalDBGrants");
      pm.retrieveAll(mSecurityDBList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalDBGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityDBList;
  }

  @SuppressWarnings("unchecked")
  private List<MDBPrivilege> listPrincipalAllDBGrant(
      String principalName, PrincipalType principalType) {
    boolean success = false;
    List<MDBPrivilege> mSecurityDBList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalAllDBGrant");
      Query query = pm.newQuery(MDBPrivilege.class,
          "principalName == t1 && principalType == t2");
      query
          .declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityDBList = (List<MDBPrivilege>) query.execute(principalName, principalType.toString());
      LOG.debug("Done executing query for listPrincipalAllDBGrant");
      pm.retrieveAll(mSecurityDBList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalAllDBGrant");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityDBList;
  }

  @SuppressWarnings("unchecked")
  public List<MTablePrivilege> listAllTableGrants(String dbName,
      String tableName) {
    boolean success = false;
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();
    List<MTablePrivilege> mSecurityTabList = null;
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();
    try {
      openTransaction();
      LOG.debug("Executing listAllTableGrants");
      String queryStr = "table.tableName == t1 && table.database.name == t2";
      Query query = pm.newQuery(
          MTablePrivilege.class, queryStr);
      query.declareParameters(
          "java.lang.String t1, java.lang.String t2");
      mSecurityTabList = (List<MTablePrivilege>) query
          .executeWithArray(tableName, dbName);
      LOG.debug("Done executing query for listAllTableGrants");
      pm.retrieveAll(mSecurityTabList);
      success = commitTransaction();
      LOG
          .debug("Done retrieving all objects for listAllTableGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityTabList;
  }

  @SuppressWarnings("unchecked")
  public List<MPartitionPrivilege> listTableAllPartitionGrants(String dbName,
      String tableName) {
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();
    boolean success = false;
    List<MPartitionPrivilege> mSecurityTabPartList = null;
    try {
      openTransaction();
      LOG.debug("Executing listTableAllPartitionGrants");
      String queryStr = "partition.table.tableName == t1 && partition.table.database.name == t2";
      Query query = pm.newQuery(
          MPartitionPrivilege.class, queryStr);
      query.declareParameters(
          "java.lang.String t1, java.lang.String t2");
      mSecurityTabPartList = (List<MPartitionPrivilege>) query
          .executeWithArray(tableName, dbName);
      LOG.debug("Done executing query for listTableAllPartitionGrants");
      pm.retrieveAll(mSecurityTabPartList);
      success = commitTransaction();
      LOG
          .debug("Done retrieving all objects for listTableAllPartitionGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityTabPartList;
  }

  @SuppressWarnings("unchecked")
  public List<MTableColumnPrivilege> listTableAllColumnGrants(String dbName,
      String tableName) {
    boolean success = false;
    List<MTableColumnPrivilege> mTblColPrivilegeList = null;
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();

    try {
      openTransaction();
      LOG.debug("Executing listTableAllColumnGrants");
      String queryStr = "table.tableName == t1 && table.database.name == t2";
      Query query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mTblColPrivilegeList = (List<MTableColumnPrivilege>) query
          .executeWithArray(tableName, dbName);
      LOG.debug("Done executing query for listTableAllColumnGrants");
      pm.retrieveAll(mTblColPrivilegeList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listTableAllColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mTblColPrivilegeList;
  }

  @SuppressWarnings("unchecked")
  public List<MPartitionColumnPrivilege> listTableAllPartitionColumnGrants(String dbName,
      String tableName) {
    boolean success = false;
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();

    List<MPartitionColumnPrivilege> mSecurityColList = null;
    try {
      openTransaction();
      LOG.debug("Executing listTableAllPartitionColumnGrants");
      String queryStr = "partition.table.tableName == t1 && partition.table.database.name == t2";
      Query query = pm.newQuery(MPartitionColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityColList = (List<MPartitionColumnPrivilege>) query
          .executeWithArray(tableName, dbName);
      LOG.debug("Done executing query for listTableAllPartitionColumnGrants");
      pm.retrieveAll(mSecurityColList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listTableAllPartitionColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityColList;
  }

  @SuppressWarnings("unchecked")
  public List<MPartitionColumnPrivilege> listPartitionAllColumnGrants(String dbName,
      String tableName, String partName) {
    boolean success = false;
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();

    List<MPartitionColumnPrivilege> mSecurityColList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPartitionAllColumnGrants");
      String queryStr = "partition.table.tableName == t1 && partition.table.database.name == t2 && partition.partitionName == t3";
      Query query = pm.newQuery(MPartitionColumnPrivilege.class, queryStr);
      query.declareParameters(
          "java.lang.String t1, java.lang.String t2, java.lang.String t3");
      mSecurityColList = (List<MPartitionColumnPrivilege>) query
          .executeWithArray(tableName, dbName, partName);
      LOG.debug("Done executing query for listPartitionAllColumnGrants");
      pm.retrieveAll(mSecurityColList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPartitionAllColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityColList;
  }

  @SuppressWarnings("unchecked")
  private List<MDBPrivilege> listDatabaseGrants(String dbName) {
    dbName = dbName.toLowerCase().trim();

    boolean success = false;
    try {
      openTransaction();
      LOG.debug("Executing listDatabaseGrants");
      Query query = pm.newQuery(MDBPrivilege.class,
          "database.name == t1");
      query.declareParameters("java.lang.String t1");
      List<MDBPrivilege> mSecurityDBList = (List<MDBPrivilege>) query
          .executeWithArray(dbName);
      LOG.debug("Done executing query for listDatabaseGrants");
      pm.retrieveAll(mSecurityDBList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listDatabaseGrants");
      return mSecurityDBList;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private List<MPartitionPrivilege> listPartitionGrants(String dbName, String tableName,
      String partName) {
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();

    boolean success = false;
    List<MPartitionPrivilege> mSecurityTabPartList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPartitionGrants");
      Query query = pm.newQuery(MPartitionPrivilege.class,
          "partition.table.tableName == t1 && partition.table.database.name == t2 && partition.partitionName == t3");
      query.declareParameters(
          "java.lang.String t1, java.lang.String t2, java.lang.String t3");
      mSecurityTabPartList = (List<MPartitionPrivilege>) query
          .executeWithArray(tableName, dbName, partName);
      LOG.debug("Done executing query for listPartitionGrants");
      pm.retrieveAll(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPartitionGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityTabPartList;
  }

  @SuppressWarnings("unchecked")
  public List<MTablePrivilege> listAllTableGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName) {
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();

    boolean success = false;
    List<MTablePrivilege> mSecurityTabPartList = null;
    try {
      openTransaction();
      LOG.debug("Executing listAllTableGrants");
      Query query = pm.newQuery(
          MTablePrivilege.class,
              "principalName == t1 && principalType == t2 && table.tableName == t3 && table.database.name == t4");
      query.declareParameters(
          "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4");
      mSecurityTabPartList = (List<MTablePrivilege>) query
          .executeWithArray(principalName, principalType.toString(), tableName, dbName);
      LOG.debug("Done executing query for listAllTableGrants");
      pm.retrieveAll(mSecurityTabPartList);
      success = commitTransaction();
      LOG
          .debug("Done retrieving all objects for listAllTableGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityTabPartList;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<MPartitionPrivilege> listPrincipalPartitionGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String partName) {
    boolean success = false;
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();

    List<MPartitionPrivilege> mSecurityTabPartList = null;
    try {
      openTransaction();
      LOG.debug("Executing listMSecurityPrincipalPartitionGrant");
      Query query = pm.newQuery(
          MPartitionPrivilege.class,
              "principalName == t1 && principalType == t2 && partition.table.tableName == t3 " +
              "&& partition.table.database.name == t4 && partition.partitionName == t5");
      query.declareParameters(
          "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4, " +
          "java.lang.String t5");
      mSecurityTabPartList = (List<MPartitionPrivilege>) query
          .executeWithArray(principalName, principalType.toString(), tableName, dbName, partName);
      LOG.debug("Done executing query for listMSecurityPrincipalPartitionGrant");

      pm.retrieveAll(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMSecurityPrincipalPartitionGrant");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityTabPartList;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<MTableColumnPrivilege> listPrincipalTableColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String columnName) {
    boolean success = false;
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();
    columnName = columnName.toLowerCase().trim();
    List<MTableColumnPrivilege> mSecurityColList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalTableColumnGrants");
      String queryStr = "principalName == t1 && principalType == t2 && " +
          "table.tableName == t3 && table.database.name == t4 &&  columnName == t5 ";
      Query query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
      query
          .declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, " +
              "java.lang.String t4, java.lang.String t5");
      mSecurityColList = (List<MTableColumnPrivilege>) query.executeWithArray(
          principalName, principalType.toString(), tableName, dbName, columnName);
      LOG.debug("Done executing query for listPrincipalTableColumnGrants");
      pm.retrieveAll(mSecurityColList);
      success = commitTransaction();
      LOG
          .debug("Done retrieving all objects for listPrincipalTableColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityColList;
  }

  @SuppressWarnings("unchecked")
  public List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String partitionName, String columnName) {
    boolean success = false;
    tableName = tableName.toLowerCase().trim();
    dbName = dbName.toLowerCase().trim();
    columnName = columnName.toLowerCase().trim();

    List<MPartitionColumnPrivilege> mSecurityColList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalPartitionColumnGrants");
      Query query = pm
          .newQuery(
              MPartitionColumnPrivilege.class,
              "principalName == t1 && principalType == t2 && partition.table.tableName == t3 " +
              "&& partition.table.database.name == t4 && partition.partitionName == t5 && columnName == t6");
      query
          .declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, " +
              "java.lang.String t4, java.lang.String t5, java.lang.String t6");

      mSecurityColList = (List<MPartitionColumnPrivilege>) query
          .executeWithArray(principalName, principalType.toString(), tableName,
              dbName, partitionName, columnName);
      LOG.debug("Done executing query for listPrincipalPartitionColumnGrants");
      pm.retrieveAll(mSecurityColList);

      success = commitTransaction();
      LOG
          .debug("Done retrieving all objects for listPrincipalPartitionColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityColList;
  }

  @SuppressWarnings("unchecked")
  private List<MTablePrivilege> listPrincipalAllTableGrants(
      String principalName, PrincipalType principalType) {
    boolean success = false;
    List<MTablePrivilege> mSecurityTabPartList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalAllTableGrants");
      Query query = pm.newQuery(MTablePrivilege.class,
          "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityTabPartList = (List<MTablePrivilege>) query.execute(
          principalName, principalType.toString());
      LOG
          .debug("Done executing query for listPrincipalAllTableGrants");
      pm.retrieveAll(mSecurityTabPartList);
      success = commitTransaction();
      LOG
          .debug("Done retrieving all objects for listPrincipalAllTableGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityTabPartList;
  }

  @SuppressWarnings("unchecked")
  private List<MPartitionPrivilege> listPrincipalAllPartitionGrants(
      String principalName, PrincipalType principalType) {
    boolean success = false;
    List<MPartitionPrivilege> mSecurityTabPartList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalAllPartitionGrants");
      Query query = pm.newQuery(MPartitionPrivilege.class,
          "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityTabPartList = (List<MPartitionPrivilege>) query.execute(
          principalName, principalType.toString());
      LOG
          .debug("Done executing query for listPrincipalAllPartitionGrants");
      pm.retrieveAll(mSecurityTabPartList);
      success = commitTransaction();
      LOG
          .debug("Done retrieving all objects for listPrincipalAllPartitionGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityTabPartList;
  }

  @SuppressWarnings("unchecked")
  private List<MTableColumnPrivilege> listPrincipalAllTableColumnGrants(
      String principalName, PrincipalType principalType) {
    boolean success = false;
    List<MTableColumnPrivilege> mSecurityColumnList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalAllTableColumnGrants");
      Query query = pm.newQuery(MTableColumnPrivilege.class,
          "principalName == t1 && principalType == t2");
      query
          .declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityColumnList = (List<MTableColumnPrivilege>) query.execute(
          principalName, principalType.toString());
      LOG.debug("Done executing query for listPrincipalAllTableColumnGrants");
      pm.retrieveAll(mSecurityColumnList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalAllTableColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityColumnList;
  }

  @SuppressWarnings("unchecked")
  private List<MPartitionColumnPrivilege> listPrincipalAllPartitionColumnGrants(
      String principalName, PrincipalType principalType) {
    boolean success = false;
    List<MPartitionColumnPrivilege> mSecurityColumnList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalAllTableColumnGrants");
      Query query = pm.newQuery(MPartitionColumnPrivilege.class,
          "principalName == t1 && principalType == t2");
      query
          .declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityColumnList = (List<MPartitionColumnPrivilege>) query.execute(
          principalName, principalType.toString());
      LOG.debug("Done executing query for listPrincipalAllTableColumnGrants");
      pm.retrieveAll(mSecurityColumnList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalAllTableColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityColumnList;
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName,
      Map<String, String> partName, PartitionEventType evtType) throws UnknownTableException,
      MetaException, InvalidPartitionException, UnknownPartitionException {

    Collection<MPartitionEvent> partEvents;
    boolean success = false;
    LOG.debug("Begin Executing isPartitionMarkedForEvent");
    try{
    openTransaction();
    Query query = pm.newQuery(MPartitionEvent.class, "dbName == t1 && tblName == t2 && partName == t3 && eventType == t4");
    query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, int t4");
    Table tbl = getTable(dbName, tblName); // Make sure dbName and tblName are valid.
    if(null == tbl) {
      throw new UnknownTableException("Table: "+ tblName + " is not found.");
    }
    partEvents = (Collection<MPartitionEvent>) query.executeWithArray(dbName, tblName, getPartitionStr(tbl, partName), evtType.getValue());
    pm.retrieveAll(partEvents);
    success = commitTransaction();
    LOG.debug("Done executing isPartitionMarkedForEvent");
    } finally{
      if (!success) {
        rollbackTransaction();
      }
    }
    return (partEvents != null  && !partEvents.isEmpty()) ? true : false;

  }

  @Override
  public Table markPartitionForEvent(String dbName, String tblName, Map<String,String> partName,
      PartitionEventType evtType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {

    LOG.debug("Begin executing markPartitionForEvent");
    boolean success = false;
    Table tbl = null;
    try{
    openTransaction();
    tbl = getTable(dbName, tblName); // Make sure dbName and tblName are valid.
    if(null == tbl) {
      throw new UnknownTableException("Table: "+ tblName + " is not found.");
    }
    pm.makePersistent(new MPartitionEvent(dbName,tblName,getPartitionStr(tbl, partName), evtType.getValue()));
    success = commitTransaction();
    LOG.debug("Done executing markPartitionForEvent");
    } finally {
      if(!success) {
        rollbackTransaction();
      }
    }
    return tbl;
  }

  private String getPartitionStr(Table tbl, Map<String,String> partName) throws InvalidPartitionException{
    if(tbl.getPartitionKeysSize() != partName.size()){
      throw new InvalidPartitionException("Number of partition columns in table: "+ tbl.getPartitionKeysSize() +
          " doesn't match with number of supplied partition values: "+partName.size());
    }
    final List<String> storedVals = new ArrayList<String>(tbl.getPartitionKeysSize());
    for(FieldSchema partKey : tbl.getPartitionKeys()){
      String partVal = partName.get(partKey.getName());
      if(null == partVal) {
        throw new InvalidPartitionException("No value found for partition column: "+partKey.getName());
      }
      storedVals.add(partVal);
    }
    return join(storedVals,',');
  }

  /** The following API
   *
   *  - executeJDOQLSelect
   *
   * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
   *
   */
  public Collection<?> executeJDOQLSelect(String query) {
    boolean committed = false;
    Collection<?> result = null;

    try {
      openTransaction();
      Query q = pm.newQuery(query);
      result = (Collection<?>) q.execute();
      committed = commitTransaction();
      if (committed) {
        return result;
      } else {
        return null;
      }
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  /** The following API
  *
  *  - executeJDOQLUpdate
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  public long executeJDOQLUpdate(String query) {
    boolean committed = false;
    long numUpdated = 0;

    try {
      openTransaction();
      Query q = pm.newQuery(query);
      numUpdated = (Long) q.execute();
      committed = commitTransaction();
      if (committed) {
        return numUpdated;
      } else {
        return -1;
      }
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  /** The following API
  *
  *  - listFSRoots
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  public Set<String> listFSRoots() {
    boolean committed = false;
    Set<String> fsRoots = new HashSet<String>();

    try {
      openTransaction();
      Query query = pm.newQuery(MDatabase.class);
      List<MDatabase> mDBs = (List<MDatabase>) query.execute();
      pm.retrieveAll(mDBs);

      for (MDatabase mDB:mDBs) {
        fsRoots.add(mDB.getLocationUri());
      }
      committed = commitTransaction();
      if (committed) {
        return fsRoots;
      } else {
        return null;
      }
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  private boolean shouldUpdateURI(URI onDiskUri, URI inputUri) {
    String onDiskHost = onDiskUri.getHost();
    String inputHost = inputUri.getHost();

    int onDiskPort = onDiskUri.getPort();
    int inputPort = inputUri.getPort();

    String onDiskScheme = onDiskUri.getScheme();
    String inputScheme = inputUri.getScheme();

    //compare ports
    if (inputPort != -1) {
      if (inputPort != onDiskPort) {
        return false;
      }
    }
    //compare schemes
    if (inputScheme != null) {
      if (onDiskScheme == null) {
        return false;
      }
      if (!inputScheme.equalsIgnoreCase(onDiskScheme)) {
        return false;
      }
    }
    //compare hosts
    if (onDiskHost != null) {
      if (!inputHost.equalsIgnoreCase(onDiskHost)) {
        return false;
      }
    } else {
      return false;
    }
    return true;
  }

  public class UpdateMDatabaseURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;

    UpdateMDatabaseURIRetVal(List<String> badRecords, Map<String, String> updateLocations) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }
  }

  /** The following APIs
  *
  *  - updateMDatabaseURI
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  public UpdateMDatabaseURIRetVal updateMDatabaseURI(URI oldLoc, URI newLoc, boolean dryRun) {
    boolean committed = false;
    Map<String, String> updateLocations = new HashMap<String, String>();
    List<String> badRecords = new ArrayList<String>();
    UpdateMDatabaseURIRetVal retVal = null;

    try {
      openTransaction();
      Query query = pm.newQuery(MDatabase.class);
      List<MDatabase> mDBs = (List<MDatabase>) query.execute();
      pm.retrieveAll(mDBs);

      for(MDatabase mDB:mDBs) {
        URI locationURI = null;
        String location = mDB.getLocationUri();
        try {
          locationURI = new URI(location);
        } catch(URISyntaxException e) {
          badRecords.add(location);
        } catch (NullPointerException e) {
          badRecords.add(location);
        }
        if (locationURI == null) {
          badRecords.add(location);
        } else {
          if (shouldUpdateURI(locationURI, oldLoc)) {
            String dbLoc = mDB.getLocationUri().replaceAll(oldLoc.toString(), newLoc.toString());
            updateLocations.put(locationURI.toString(), dbLoc);
            if (!dryRun) {
              mDB.setLocationUri(dbLoc);
            }
          }
        }
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdateMDatabaseURIRetVal(badRecords, updateLocations);
      }
      return retVal;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  public class UpdateMStorageDescriptorTblPropURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;

    UpdateMStorageDescriptorTblPropURIRetVal(List<String> badRecords,
      Map<String, String> updateLocations) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }
  }

  /** The following APIs
  *
  *  - updateMStorageDescriptorTblPropURI
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  public UpdateMStorageDescriptorTblPropURIRetVal updateMStorageDescriptorTblPropURI(URI oldLoc,
      URI newLoc, String tblPropKey, boolean isDryRun) {
    boolean committed = false;
    Map<String, String> updateLocations = new HashMap<String, String>();
    List<String> badRecords = new ArrayList<String>();
    UpdateMStorageDescriptorTblPropURIRetVal retVal = null;

    try {
      openTransaction();
      Query query = pm.newQuery(MStorageDescriptor.class);
      List<MStorageDescriptor> mSDSs = (List<MStorageDescriptor>) query.execute();
      pm.retrieveAll(mSDSs);

      for(MStorageDescriptor mSDS:mSDSs) {
        URI tablePropLocationURI = null;
        if (mSDS.getParameters().containsKey(tblPropKey)) {
          String tablePropLocation = mSDS.getParameters().get(tblPropKey);
          try {
              tablePropLocationURI = new URI(tablePropLocation);
            } catch (URISyntaxException e) {
              badRecords.add(tablePropLocation);
            } catch (NullPointerException e) {
              badRecords.add(tablePropLocation);
            }
            // if tablePropKey that was passed in lead to a valid URI resolution, update it if
            //parts of it match the old-NN-loc, else add to badRecords
            if (tablePropLocationURI == null) {
              badRecords.add(tablePropLocation);
            } else {
              if (shouldUpdateURI(tablePropLocationURI, oldLoc)) {
                String tblPropLoc = mSDS.getParameters().get(tblPropKey).replaceAll(oldLoc.toString(),
                    newLoc.toString());
                updateLocations.put(tablePropLocationURI.toString(), tblPropLoc);
                if (!isDryRun) {
                  mSDS.getParameters().put(tblPropKey, tblPropLoc);
                }
             }
           }
         }
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdateMStorageDescriptorTblPropURIRetVal(badRecords, updateLocations);
      }
      return retVal;
     } finally {
        if (!committed) {
          rollbackTransaction();
        }
     }
  }

  public class UpdateMStorageDescriptorTblURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;

    UpdateMStorageDescriptorTblURIRetVal(List<String> badRecords,
      Map<String, String> updateLocations) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }
  }

  /** The following APIs
  *
  *  - updateMStorageDescriptorTblURI
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  public UpdateMStorageDescriptorTblURIRetVal updateMStorageDescriptorTblURI(URI oldLoc, URI newLoc,
    boolean isDryRun) {
    boolean committed = false;
    Map<String, String> updateLocations = new HashMap<String, String>();
    List<String> badRecords = new ArrayList<String>();
    UpdateMStorageDescriptorTblURIRetVal retVal = null;

    try {
      openTransaction();
      Query query = pm.newQuery(MStorageDescriptor.class);
      List<MStorageDescriptor> mSDSs = (List<MStorageDescriptor>) query.execute();
      pm.retrieveAll(mSDSs);

      for(MStorageDescriptor mSDS:mSDSs) {
        URI locationURI = null;
        String location = mSDS.getLocation();
        try {
          locationURI = new URI(location);
        } catch (URISyntaxException e) {
          badRecords.add(location);
        } catch (NullPointerException e) {
          badRecords.add(location);
        }
        if (locationURI == null) {
          badRecords.add(location);
        } else {
          if (shouldUpdateURI(locationURI, oldLoc)) {
            String tblLoc = mSDS.getLocation().replaceAll(oldLoc.toString(), newLoc.toString());
            updateLocations.put(locationURI.toString(), tblLoc);
            if (!isDryRun) {
              mSDS.setLocation(tblLoc);
            }
          }
        }
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdateMStorageDescriptorTblURIRetVal(badRecords, updateLocations);
      }
      return retVal;
    } finally {
        if (!committed) {
          rollbackTransaction();
        }
     }
  }

  public class UpdateSerdeURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;

    UpdateSerdeURIRetVal(List<String> badRecords, Map<String, String> updateLocations) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }
  }

  /** The following APIs
  *
  *  - updateSerdeURI
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  public UpdateSerdeURIRetVal updateSerdeURI(URI oldLoc, URI newLoc, String serdeProp,
    boolean isDryRun) {
    boolean committed = false;
    Map<String, String> updateLocations = new HashMap<String, String>();
    List<String> badRecords = new ArrayList<String>();
    UpdateSerdeURIRetVal retVal = null;

    try {
      openTransaction();
      Query query = pm.newQuery(MSerDeInfo.class);
      List<MSerDeInfo> mSerdes = (List<MSerDeInfo>) query.execute();
      pm.retrieveAll(mSerdes);

      for(MSerDeInfo mSerde:mSerdes) {
        if (mSerde.getParameters().containsKey(serdeProp)) {
          String schemaLoc = mSerde.getParameters().get(serdeProp);
          URI schemaLocURI = null;
          try {
            schemaLocURI = new URI(schemaLoc);
          } catch (URISyntaxException e) {
            badRecords.add(schemaLoc);
          } catch (NullPointerException e) {
            badRecords.add(schemaLoc);
          }
          if (schemaLocURI == null) {
            badRecords.add(schemaLoc);
          } else {
            if (shouldUpdateURI(schemaLocURI, oldLoc)) {
              String newSchemaLoc = schemaLoc.replaceAll(oldLoc.toString(), newLoc.toString());
              updateLocations.put(schemaLocURI.toString(), newSchemaLoc);
              if (!isDryRun) {
                mSerde.getParameters().put(serdeProp, newSchemaLoc);
              }
            }
          }
        }
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdateSerdeURIRetVal(badRecords, updateLocations);
      }
      return retVal;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  // Methods to persist, maintain and retrieve Column Statistics
  private MTableColumnStatistics convertToMTableColumnStatistics(ColumnStatisticsDesc statsDesc,
      ColumnStatisticsObj statsObj) throws NoSuchObjectException,
      MetaException, InvalidObjectException
  {
     if (statsObj == null || statsDesc == null) {
       throw new InvalidObjectException("Invalid column stats object");
     }

     String dbName = statsDesc.getDbName();
     String tableName = statsDesc.getTableName();
     MTable table = getMTable(dbName, tableName);

     if (table == null) {
       throw new NoSuchObjectException("Table " + tableName +
       " for which stats is gathered doesn't exist.");
     }

     MTableColumnStatistics mColStats = new MTableColumnStatistics();
     mColStats.setTable(table);
     mColStats.setDbName(statsDesc.getDbName());
     mColStats.setTableName(statsDesc.getTableName());
     mColStats.setLastAnalyzed(statsDesc.getLastAnalyzed());
     mColStats.setColName(statsObj.getColName());
     mColStats.setColType(statsObj.getColType());

     if (statsObj.getStatsData().isSetBooleanStats()) {
       BooleanColumnStatsData boolStats = statsObj.getStatsData().getBooleanStats();
       mColStats.setBooleanStats(boolStats.getNumTrues(), boolStats.getNumFalses(),
           boolStats.getNumNulls());
     } else if (statsObj.getStatsData().isSetLongStats()) {
       LongColumnStatsData longStats = statsObj.getStatsData().getLongStats();
       mColStats.setLongStats(longStats.getNumNulls(), longStats.getNumDVs(),
           longStats.getLowValue(), longStats.getHighValue());
     } else if (statsObj.getStatsData().isSetDoubleStats()) {
       DoubleColumnStatsData doubleStats = statsObj.getStatsData().getDoubleStats();
       mColStats.setDoubleStats(doubleStats.getNumNulls(), doubleStats.getNumDVs(),
           doubleStats.getLowValue(), doubleStats.getHighValue());
     } else if (statsObj.getStatsData().isSetStringStats()) {
       StringColumnStatsData stringStats = statsObj.getStatsData().getStringStats();
       mColStats.setStringStats(stringStats.getNumNulls(), stringStats.getNumDVs(),
         stringStats.getMaxColLen(), stringStats.getAvgColLen());
     } else if (statsObj.getStatsData().isSetBinaryStats()) {
       BinaryColumnStatsData binaryStats = statsObj.getStatsData().getBinaryStats();
       mColStats.setBinaryStats(binaryStats.getNumNulls(), binaryStats.getMaxColLen(),
         binaryStats.getAvgColLen());
     }
     return mColStats;
  }

  private ColumnStatisticsObj getTableColumnStatisticsObj(MTableColumnStatistics mStatsObj) {
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColType(mStatsObj.getColType());
    statsObj.setColName(mStatsObj.getColName());
    String colType = mStatsObj.getColType().toLowerCase();
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();

    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(mStatsObj.getNumFalses());
      boolStats.setNumTrues(mStatsObj.getNumTrues());
      boolStats.setNumNulls(mStatsObj.getNumNulls());
      colStatsData.setBooleanStats(boolStats);
    } else if (colType.equals("string") ||
        colType.startsWith("varchar") || colType.startsWith("char")) {
      StringColumnStatsData stringStats = new StringColumnStatsData();
      stringStats.setNumNulls(mStatsObj.getNumNulls());
      stringStats.setAvgColLen(mStatsObj.getAvgColLen());
      stringStats.setMaxColLen(mStatsObj.getMaxColLen());
      stringStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls(mStatsObj.getNumNulls());
      binaryStats.setAvgColLen(mStatsObj.getAvgColLen());
      binaryStats.setMaxColLen(mStatsObj.getMaxColLen());
      colStatsData.setBinaryStats(binaryStats);
    } else if (colType.equals("bigint") || colType.equals("int") ||
        colType.equals("smallint") || colType.equals("tinyint") ||
        colType.equals("timestamp")) {
      LongColumnStatsData longStats = new LongColumnStatsData();
      longStats.setNumNulls(mStatsObj.getNumNulls());
      longStats.setHighValue(mStatsObj.getLongHighValue());
      longStats.setLowValue(mStatsObj.getLongLowValue());
      longStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setLongStats(longStats);
   } else if (colType.equals("double") || colType.equals("float")) {
     DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
     doubleStats.setNumNulls(mStatsObj.getNumNulls());
     doubleStats.setHighValue(mStatsObj.getDoubleHighValue());
     doubleStats.setLowValue(mStatsObj.getDoubleLowValue());
     doubleStats.setNumDVs(mStatsObj.getNumDVs());
     colStatsData.setDoubleStats(doubleStats);
   }
   statsObj.setStatsData(colStatsData);
   return statsObj;
  }

  private ColumnStatisticsDesc getTableColumnStatisticsDesc(MTableColumnStatistics mStatsObj) {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setIsTblLevel(true);
    statsDesc.setDbName(mStatsObj.getDbName());
    statsDesc.setTableName(mStatsObj.getTableName());
    statsDesc.setLastAnalyzed(mStatsObj.getLastAnalyzed());
    return statsDesc;
  }

  private ColumnStatistics convertToTableColumnStatistics(MTableColumnStatistics mStatsObj)
    throws MetaException
  {
    if (mStatsObj == null) {
      return null;
    }

    ColumnStatisticsDesc statsDesc = getTableColumnStatisticsDesc(mStatsObj);
    ColumnStatisticsObj statsObj = getTableColumnStatisticsObj(mStatsObj);
    List<ColumnStatisticsObj> statsObjs = new ArrayList<ColumnStatisticsObj>();
    statsObjs.add(statsObj);

    ColumnStatistics colStats = new ColumnStatistics();
    colStats.setStatsDesc(statsDesc);
    colStats.setStatsObj(statsObjs);
    return colStats;
  }

  private MPartitionColumnStatistics convertToMPartitionColumnStatistics(ColumnStatisticsDesc
    statsDesc, ColumnStatisticsObj statsObj, List<String> partVal)
    throws MetaException, NoSuchObjectException
  {
    if (statsDesc == null || statsObj == null || partVal == null) {
      return null;
    }

    MPartition partition  = getMPartition(statsDesc.getDbName(), statsDesc.getTableName(), partVal);

    if (partition == null) {
      throw new NoSuchObjectException("Partition for which stats is gathered doesn't exist.");
    }

    MPartitionColumnStatistics mColStats = new MPartitionColumnStatistics();
    mColStats.setPartition(partition);
    mColStats.setDbName(statsDesc.getDbName());
    mColStats.setTableName(statsDesc.getTableName());
    mColStats.setPartitionName(statsDesc.getPartName());
    mColStats.setLastAnalyzed(statsDesc.getLastAnalyzed());
    mColStats.setColName(statsObj.getColName());
    mColStats.setColType(statsObj.getColType());

    if (statsObj.getStatsData().isSetBooleanStats()) {
      BooleanColumnStatsData boolStats = statsObj.getStatsData().getBooleanStats();
      mColStats.setBooleanStats(boolStats.getNumTrues(), boolStats.getNumFalses(),
          boolStats.getNumNulls());
    } else if (statsObj.getStatsData().isSetLongStats()) {
      LongColumnStatsData longStats = statsObj.getStatsData().getLongStats();
      mColStats.setLongStats(longStats.getNumNulls(), longStats.getNumDVs(),
          longStats.getLowValue(), longStats.getHighValue());
    } else if (statsObj.getStatsData().isSetDoubleStats()) {
      DoubleColumnStatsData doubleStats = statsObj.getStatsData().getDoubleStats();
      mColStats.setDoubleStats(doubleStats.getNumNulls(), doubleStats.getNumDVs(),
          doubleStats.getLowValue(), doubleStats.getHighValue());
    } else if (statsObj.getStatsData().isSetStringStats()) {
      StringColumnStatsData stringStats = statsObj.getStatsData().getStringStats();
      mColStats.setStringStats(stringStats.getNumNulls(), stringStats.getNumDVs(),
        stringStats.getMaxColLen(), stringStats.getAvgColLen());
    } else if (statsObj.getStatsData().isSetBinaryStats()) {
      BinaryColumnStatsData binaryStats = statsObj.getStatsData().getBinaryStats();
      mColStats.setBinaryStats(binaryStats.getNumNulls(), binaryStats.getMaxColLen(),
        binaryStats.getAvgColLen());
    }
    return mColStats;
  }

  private void writeMTableColumnStatistics(MTableColumnStatistics mStatsObj)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException
  {
     String dbName = mStatsObj.getDbName();
     String tableName = mStatsObj.getTableName();
     String colName = mStatsObj.getColName();

     LOG.info("Updating table level column statistics for db=" + dbName + " tableName=" + tableName
       + " colName=" + colName);

     MTable mTable = getMTable(mStatsObj.getDbName(), mStatsObj.getTableName());
     boolean foundCol = false;

     if (mTable == null) {
        throw new
          NoSuchObjectException("Table " + tableName +
          " for which stats gathering is requested doesn't exist.");
      }

      MStorageDescriptor mSDS = mTable.getSd();
      List<MFieldSchema> colList = mSDS.getCD().getCols();

      for(MFieldSchema mCol:colList) {
        if (mCol.getName().equals(mStatsObj.getColName().trim())) {
          foundCol = true;
          break;
        }
      }

      if (!foundCol) {
        throw new
          NoSuchObjectException("Column " + colName +
          " for which stats gathering is requested doesn't exist.");
      }

      MTableColumnStatistics oldStatsObj = getMTableColumnStatistics(dbName, tableName, colName);

      if (oldStatsObj != null) {
       oldStatsObj.setAvgColLen(mStatsObj.getAvgColLen());
       oldStatsObj.setLongHighValue(mStatsObj.getLongHighValue());
       oldStatsObj.setDoubleHighValue(mStatsObj.getDoubleHighValue());
       oldStatsObj.setLastAnalyzed(mStatsObj.getLastAnalyzed());
       oldStatsObj.setLongLowValue(mStatsObj.getLongLowValue());
       oldStatsObj.setDoubleLowValue(mStatsObj.getDoubleLowValue());
       oldStatsObj.setMaxColLen(mStatsObj.getMaxColLen());
       oldStatsObj.setNumDVs(mStatsObj.getNumDVs());
       oldStatsObj.setNumFalses(mStatsObj.getNumFalses());
       oldStatsObj.setNumTrues(mStatsObj.getNumTrues());
       oldStatsObj.setNumNulls(mStatsObj.getNumNulls());
      } else {
        pm.makePersistent(mStatsObj);
      }
   }

  private ColumnStatisticsObj getPartitionColumnStatisticsObj(MPartitionColumnStatistics mStatsObj)
  {
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColType(mStatsObj.getColType());
    statsObj.setColName(mStatsObj.getColName());
    String colType = mStatsObj.getColType().toLowerCase();
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();

    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(mStatsObj.getNumFalses());
      boolStats.setNumTrues(mStatsObj.getNumTrues());
      boolStats.setNumNulls(mStatsObj.getNumNulls());
      colStatsData.setBooleanStats(boolStats);
    } else if (colType.equals("string") ||
        colType.startsWith("varchar") || colType.startsWith("char")) {
      StringColumnStatsData stringStats = new StringColumnStatsData();
      stringStats.setNumNulls(mStatsObj.getNumNulls());
      stringStats.setAvgColLen(mStatsObj.getAvgColLen());
      stringStats.setMaxColLen(mStatsObj.getMaxColLen());
      stringStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls(mStatsObj.getNumNulls());
      binaryStats.setAvgColLen(mStatsObj.getAvgColLen());
      binaryStats.setMaxColLen(mStatsObj.getMaxColLen());
      colStatsData.setBinaryStats(binaryStats);
    } else if (colType.equals("tinyint") || colType.equals("smallint") ||
        colType.equals("int") || colType.equals("bigint") ||
        colType.equals("timestamp")) {
      LongColumnStatsData longStats = new LongColumnStatsData();
      longStats.setNumNulls(mStatsObj.getNumNulls());
      longStats.setHighValue(mStatsObj.getLongHighValue());
      longStats.setLowValue(mStatsObj.getLongLowValue());
      longStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setLongStats(longStats);
   } else if (colType.equals("double") || colType.equals("float")) {
     DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
     doubleStats.setNumNulls(mStatsObj.getNumNulls());
     doubleStats.setHighValue(mStatsObj.getDoubleHighValue());
     doubleStats.setLowValue(mStatsObj.getDoubleLowValue());
     doubleStats.setNumDVs(mStatsObj.getNumDVs());
     colStatsData.setDoubleStats(doubleStats);
   }
   statsObj.setStatsData(colStatsData);
   return statsObj;
  }

  private ColumnStatisticsDesc getPartitionColumnStatisticsDesc(
    MPartitionColumnStatistics mStatsObj) {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setIsTblLevel(false);
    statsDesc.setDbName(mStatsObj.getDbName());
    statsDesc.setTableName(mStatsObj.getTableName());
    statsDesc.setPartName(mStatsObj.getPartitionName());
    statsDesc.setLastAnalyzed(mStatsObj.getLastAnalyzed());
    return statsDesc;
  }

  private void writeMPartitionColumnStatistics(MPartitionColumnStatistics mStatsObj,
    List<String> partVal) throws NoSuchObjectException, MetaException, InvalidObjectException,
    InvalidInputException
  {
    String dbName = mStatsObj.getDbName();
    String tableName = mStatsObj.getTableName();
    String partName = mStatsObj.getPartitionName();
    String colName = mStatsObj.getColName();

    LOG.info("Updating partition level column statistics for db=" + dbName + " tableName=" +
      tableName + " partName=" + partName + " colName=" + colName);

    MTable mTable = getMTable(mStatsObj.getDbName(), mStatsObj.getTableName());
    boolean foundCol = false;

    if (mTable == null) {
      throw new
        NoSuchObjectException("Table " + tableName +
        " for which stats gathering is requested doesn't exist.");
    }

    MPartition mPartition =
                 getMPartition(mStatsObj.getDbName(), mStatsObj.getTableName(), partVal);

    if (mPartition == null) {
      throw new
        NoSuchObjectException("Partition " + partName +
        " for which stats gathering is requested doesn't exist");
    }

    MStorageDescriptor mSDS = mPartition.getSd();
    List<MFieldSchema> colList = mSDS.getCD().getCols();

    for(MFieldSchema mCol:colList) {
      if (mCol.getName().equals(mStatsObj.getColName().trim())) {
        foundCol = true;
        break;
      }
    }

    if (!foundCol) {
      throw new
        NoSuchObjectException("Column " + colName +
        " for which stats gathering is requested doesn't exist.");
    }

    MPartitionColumnStatistics oldStatsObj = getMPartitionColumnStatistics(dbName, tableName,
                                                               partName, partVal, colName);
    if (oldStatsObj != null) {
      oldStatsObj.setAvgColLen(mStatsObj.getAvgColLen());
      oldStatsObj.setLongHighValue(mStatsObj.getLongHighValue());
      oldStatsObj.setDoubleHighValue(mStatsObj.getDoubleHighValue());
      oldStatsObj.setLastAnalyzed(mStatsObj.getLastAnalyzed());
      oldStatsObj.setLongLowValue(mStatsObj.getLongLowValue());
      oldStatsObj.setDoubleLowValue(mStatsObj.getDoubleLowValue());
      oldStatsObj.setMaxColLen(mStatsObj.getMaxColLen());
      oldStatsObj.setNumDVs(mStatsObj.getNumDVs());
      oldStatsObj.setNumFalses(mStatsObj.getNumFalses());
      oldStatsObj.setNumTrues(mStatsObj.getNumTrues());
      oldStatsObj.setNumNulls(mStatsObj.getNumNulls());
    } else {
      pm.makePersistent(mStatsObj);
    }
 }

  public boolean updateTableColumnStatistics(ColumnStatistics colStats)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException
  {
    boolean committed = false;

    try {
      openTransaction();
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();

      for (ColumnStatisticsObj statsObj:statsObjs) {
          MTableColumnStatistics mStatsObj = convertToMTableColumnStatistics(statsDesc, statsObj);
          writeMTableColumnStatistics(mStatsObj);
      }
      committed = commitTransaction();
      return committed;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
 }

  public boolean updatePartitionColumnStatistics(ColumnStatistics colStats, List<String> partVals)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException
  {
    boolean committed = false;

    try {
    openTransaction();
    List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
    ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();

    for (ColumnStatisticsObj statsObj:statsObjs) {
        MPartitionColumnStatistics mStatsObj =
            convertToMPartitionColumnStatistics(statsDesc, statsObj, partVals);
        writeMPartitionColumnStatistics(mStatsObj, partVals);
    }
    committed = commitTransaction();
    return committed;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  private MTableColumnStatistics getMTableColumnStatistics(String dbName, String tableName,
    String colName) throws NoSuchObjectException, InvalidInputException, MetaException
  {
    boolean committed = false;

    if (dbName == null) {
      dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    }

    if (tableName == null || colName == null) {
      throw new InvalidInputException("TableName/ColName passed to get_table_column_statistics " +
      "is null");
    }

    try {
      openTransaction();
      MTableColumnStatistics mStatsObj = null;
      MTable mTable = getMTable(dbName.trim(), tableName.trim());
      boolean foundCol = false;

      if (mTable == null) {
        throw new NoSuchObjectException("Table " + tableName +
        " for which stats is requested doesn't exist.");
      }

      MStorageDescriptor mSDS = mTable.getSd();
      List<MFieldSchema> colList = mSDS.getCD().getCols();

      for(MFieldSchema mCol:colList) {
        if (mCol.getName().equals(colName.trim())) {
          foundCol = true;
          break;
        }
      }

      if (!foundCol) {
        throw new NoSuchObjectException("Column " + colName +
        " for which stats is requested doesn't exist.");
      }

      Query query = pm.newQuery(MTableColumnStatistics.class);
      query.setFilter("table.tableName == t1 && " +
        "dbName == t2 && " + "colName == t3");
      query
      .declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      query.setUnique(true);

      mStatsObj = (MTableColumnStatistics) query.execute(tableName.trim(),
                                                        dbName.trim(), colName.trim());
      pm.retrieve(mStatsObj);
      committed = commitTransaction();
      return mStatsObj;
    } finally {
      if (!committed) {
        rollbackTransaction();
        return null;
      }
    }
  }

 public ColumnStatistics getTableColumnStatistics(String dbName, String tableName, String colName)
   throws MetaException, NoSuchObjectException, InvalidInputException
  {
    ColumnStatistics statsObj;
    MTableColumnStatistics mStatsObj = getMTableColumnStatistics(dbName, tableName, colName);

    if (mStatsObj == null) {
      throw new NoSuchObjectException("Statistics for dbName=" + dbName + " tableName=" + tableName
        + " columnName=" + colName + " doesn't exist.");
    }

    statsObj = convertToTableColumnStatistics(mStatsObj);
    return statsObj;
  }

  public ColumnStatistics getPartitionColumnStatistics(String dbName, String tableName,
    String partName, List<String> partVal, String colName)
    throws MetaException, NoSuchObjectException, InvalidInputException
  {
    ColumnStatistics statsObj;
    MPartitionColumnStatistics mStatsObj =
          getMPartitionColumnStatistics(dbName, tableName, partName, partVal, colName);

    if (mStatsObj == null) {
      throw new NoSuchObjectException("Statistics for dbName=" + dbName + " tableName=" + tableName
          + " partName= " + partName + " columnName=" + colName + " doesn't exist.");
    }
    statsObj = convertToPartColumnStatistics(mStatsObj);
    return statsObj;
  }

  private ColumnStatistics convertToPartColumnStatistics(MPartitionColumnStatistics mStatsObj)
  {
    if (mStatsObj == null) {
      return null;
    }

    ColumnStatisticsDesc statsDesc = getPartitionColumnStatisticsDesc(mStatsObj);
    ColumnStatisticsObj statsObj = getPartitionColumnStatisticsObj(mStatsObj);
    List<ColumnStatisticsObj> statsObjs = new ArrayList<ColumnStatisticsObj>();
    statsObjs.add(statsObj);

    ColumnStatistics colStats = new ColumnStatistics();
    colStats.setStatsDesc(statsDesc);
    colStats.setStatsObj(statsObjs);
    return colStats;
  }

  private MPartitionColumnStatistics getMPartitionColumnStatistics(String dbName, String tableName,
    String partName, List<String> partVal, String colName) throws NoSuchObjectException,
    InvalidInputException, MetaException
  {
    boolean committed = false;
    MPartitionColumnStatistics mStatsObj = null;

    if (dbName == null) {
      dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    }

    if (tableName == null || partVal == null || colName == null) {
      throw new InvalidInputException("TableName/PartName/ColName passed to " +
        " get_partition_column_statistics is null");
    }

    try {
      openTransaction();
      MTable mTable = getMTable(dbName.trim(), tableName.trim());
      boolean foundCol = false;

      if (mTable == null) {
        throw new NoSuchObjectException("Table "  + tableName +
          " for which stats is requested doesn't exist.");
      }

      MPartition mPartition =
                  getMPartition(dbName, tableName, partVal);

      if (mPartition == null) {
        throw new
          NoSuchObjectException("Partition " + partName +
          " for which stats is requested doesn't exist");
      }

      MStorageDescriptor mSDS = mPartition.getSd();
      List<MFieldSchema> colList = mSDS.getCD().getCols();

      for(MFieldSchema mCol:colList) {
        if (mCol.getName().equals(colName.trim())) {
          foundCol = true;
          break;
        }
      }

      if (!foundCol) {
        throw new NoSuchObjectException("Column " + colName +
        " for which stats is requested doesn't exist.");
      }

      Query query = pm.newQuery(MPartitionColumnStatistics.class);
      query.setFilter("partition.partitionName == t1 && " +
        "dbName == t2 && " + "tableName == t3 && " + "colName == t4");
      query
      .declareParameters("java.lang.String t1, java.lang.String t2, " +
         "java.lang.String t3, java.lang.String t4");
      query.setUnique(true);

      mStatsObj = (MPartitionColumnStatistics) query.executeWithArray(partName.trim(),
                                                        dbName.trim(), tableName.trim(),
                                                        colName.trim());
      pm.retrieve(mStatsObj);
      committed = commitTransaction();
      return mStatsObj;

    } finally {
      if (!committed) {
        rollbackTransaction();
       }
    }
  }

  public boolean deletePartitionColumnStatistics(String dbName, String tableName,
    String partName, List<String> partVals,String colName)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException
  {
    boolean ret = false;

    if (dbName == null) {
      dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    }

    if (tableName == null) {
      throw new InvalidInputException("Table name is null.");
    }

    try {
      openTransaction();
      MTable mTable = getMTable(dbName, tableName);
      MPartitionColumnStatistics mStatsObj;
      List<MPartitionColumnStatistics> mStatsObjColl;

      if (mTable == null) {
        throw new
          NoSuchObjectException("Table " + tableName +
          "  for which stats deletion is requested doesn't exist");
      }

      MPartition mPartition =
          getMPartition(dbName, tableName, partVals);

      if (mPartition == null) {
        throw new
          NoSuchObjectException("Partition " + partName +
          " for which stats deletion is requested doesn't exist");
      }

      Query query = pm.newQuery(MPartitionColumnStatistics.class);
      String filter;
      String parameters;

      if (colName != null) {
        filter = "partition.partitionName == t1 && dbName == t2 && tableName == t3 && " +
                    "colName == t4";
        parameters = "java.lang.String t1, java.lang.String t2, " +
                        "java.lang.String t3, java.lang.String t4";
      } else {
        filter = "partition.partitionName == t1 && dbName == t2 && tableName == t3";
        parameters = "java.lang.String t1, java.lang.String t2, java.lang.String t3";
      }

      query.setFilter(filter);
      query
        .declareParameters(parameters);

      if (colName != null) {
        query.setUnique(true);
        mStatsObj = (MPartitionColumnStatistics)query.executeWithArray(partName.trim(),
                                                dbName.trim(), tableName.trim(), colName.trim());
        pm.retrieve(mStatsObj);

        if (mStatsObj != null) {
          pm.deletePersistent(mStatsObj);
        } else {
          throw new NoSuchObjectException("Column stats doesn't exist for db=" +dbName + " table="
              + tableName + " partition=" + partName + " col=" + colName);
        }
      } else {
        mStatsObjColl= (List<MPartitionColumnStatistics>)query.execute(partName.trim(),
                                  dbName.trim(), tableName.trim());
        pm.retrieveAll(mStatsObjColl);

        if (mStatsObjColl != null) {
          pm.deletePersistentAll(mStatsObjColl);
        } else {
          throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName +
            " table=" + tableName + " partition" + partName);
        }
      }
      ret = commitTransaction();
    } catch(NoSuchObjectException e) {
       rollbackTransaction();
       throw e;
    } finally {
      if (!ret) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException
  {
    boolean ret = false;

    if (dbName == null) {
      dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    }

    if (tableName == null) {
      throw new InvalidInputException("Table name is null.");
    }

    try {
      openTransaction();
      MTable mTable = getMTable(dbName, tableName);
      MTableColumnStatistics mStatsObj;
        List<MTableColumnStatistics> mStatsObjColl;

      if (mTable == null) {
        throw new
          NoSuchObjectException("Table " + tableName +
          "  for which stats deletion is requested doesn't exist");
      }

      Query query = pm.newQuery(MTableColumnStatistics.class);
      String filter;
      String parameters;

      if (colName != null) {
        filter = "table.tableName == t1 && dbName == t2 && colName == t3";
        parameters = "java.lang.String t1, java.lang.String t2, java.lang.String t3";
      } else {
        filter = "table.tableName == t1 && dbName == t2";
        parameters = "java.lang.String t1, java.lang.String t2";
      }

      query.setFilter(filter);
      query
        .declareParameters(parameters);

      if (colName != null) {
        query.setUnique(true);
        mStatsObj = (MTableColumnStatistics)query.execute(tableName.trim(),
                                                    dbName.trim(), colName.trim());
        pm.retrieve(mStatsObj);

        if (mStatsObj != null) {
          pm.deletePersistent(mStatsObj);
        } else {
          throw new NoSuchObjectException("Column stats doesn't exist for db=" +dbName + " table="
              + tableName + " col=" + colName);
        }
      } else {
        mStatsObjColl= (List<MTableColumnStatistics>)query.execute(tableName.trim(), dbName.trim());
        pm.retrieveAll(mStatsObjColl);

        if (mStatsObjColl != null) {
          pm.deletePersistentAll(mStatsObjColl);
        } else {
          throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName +
            " table=" + tableName);
        }
      }
      ret = commitTransaction();
    } catch(NoSuchObjectException e) {
       rollbackTransaction();
       throw e;
    } finally {
      if (!ret) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  @Override
  public long cleanupEvents() {
    boolean commited = false;
    long delCnt;
    LOG.debug("Begin executing cleanupEvents");
    Long expiryTime = HiveConf.getLongVar(getConf(), ConfVars.METASTORE_EVENT_EXPIRY_DURATION) * 1000L;
    Long curTime = System.currentTimeMillis();
    try {
      openTransaction();
      Query query = pm.newQuery(MPartitionEvent.class,"curTime - eventTime > expiryTime");
      query.declareParameters("java.lang.Long curTime, java.lang.Long expiryTime");
      delCnt = query.deletePersistentAll(curTime, expiryTime);
      commited = commitTransaction();
    }
    finally {
      if (!commited) {
        rollbackTransaction();
      }
      LOG.debug("Done executing cleanupEvents");
    }
    return delCnt;
  }

  private MDelegationToken getTokenFrom(String tokenId) {
    Query query = pm.newQuery(MDelegationToken.class, "tokenIdentifier == tokenId");
    query.declareParameters("java.lang.String tokenId");
    query.setUnique(true);
    return (MDelegationToken)query.execute(tokenId);
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
      if(!committed) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing addToken with status : " + committed);
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
      if(!committed) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing removeToken with status : " + committed);
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
      if(!committed) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing getToken with status : " + committed);
    return (null == token) ? null : token.getTokenStr();
  }

  @Override
  public List<String> getAllTokenIdentifiers() {

    LOG.debug("Begin executing getAllTokenIdentifiers");
    boolean committed = false;
    List<MDelegationToken> tokens;
    try{
      openTransaction();
      Query query = pm.newQuery(MDelegationToken.class);
      tokens = (List<MDelegationToken>) query.execute();
      pm.retrieveAll(tokens);
      committed = commitTransaction();
    } finally {
      if(!committed) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing getAllTokenIdentifers with status : " + committed);
    List<String> tokenIdents = new ArrayList<String>(tokens.size());

    for (MDelegationToken token : tokens) {
      tokenIdents.add(token.getTokenIdentifier());
    }
    return tokenIdents;
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
      if(!committed) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing addMasterKey with status : " + committed);
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
    MMasterKey masterKey;
    try{
    openTransaction();
    Query query = pm.newQuery(MMasterKey.class, "keyId == id");
    query.declareParameters("java.lang.Integer id");
    query.setUnique(true);
    masterKey = (MMasterKey)query.execute(id);
    if (null != masterKey) {
      masterKey.setMasterKey(key);
    }
    committed = commitTransaction();
    } finally {
      if(!committed) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing updateMasterKey with status : " + committed);
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
    MMasterKey masterKey;
    try{
    openTransaction();
    Query query = pm.newQuery(MMasterKey.class, "keyId == id");
    query.declareParameters("java.lang.Integer id");
    query.setUnique(true);
    masterKey = (MMasterKey)query.execute(id);
    if (null != masterKey) {
      pm.deletePersistent(masterKey);
    }
    success = commitTransaction();
    } finally {
      if(!success) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing removeMasterKey with status : " + success);
    return (null != masterKey) && success;
  }

  @Override
  public String[] getMasterKeys() {
    LOG.debug("Begin executing getMasterKeys");
    boolean committed = false;
    List<MMasterKey> keys;
    try{
      openTransaction();
      Query query = pm.newQuery(MMasterKey.class);
      keys = (List<MMasterKey>) query.execute();
      pm.retrieveAll(keys);
      committed = commitTransaction();
    } finally {
      if(!committed) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing getMasterKeys with status : " + committed);
    String[] masterKeys = new String[keys.size()];

    for (int i = 0; i < keys.size(); i++) {
      masterKeys[i] = keys.get(i).getMasterKey();
    }
    return masterKeys;
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

  private synchronized void checkSchema() throws MetaException {
    // recheck if it got verified by another thread while we were waiting
    if (isSchemaVerified.get()) {
      return;
    }

    boolean strictValidation =
      HiveConf.getBoolVar(getConf(), HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION);
    // read the schema version stored in metastore db
    String schemaVer = getMetaStoreSchemaVersion();
    if (schemaVer == null) {
      // metastore has no schema version information
      if (strictValidation) {
            throw new MetaException("Version information not found in metastore. ");
          } else {
            LOG.warn("Version information not found in metastore. "
                + HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString() +
                " is not enabled so recording the schema version " +
                MetaStoreSchemaInfo.getHiveSchemaVersion());
            setMetaStoreSchemaVersion(MetaStoreSchemaInfo.getHiveSchemaVersion(),
                "Set by MetaStore");
        }
    } else {
      // metastore schema version is different than Hive distribution needs
      if (strictValidation) {
        if (!schemaVer.equalsIgnoreCase(MetaStoreSchemaInfo.getHiveSchemaVersion())) {
          throw new MetaException("Hive Schema version "
              + MetaStoreSchemaInfo.getHiveSchemaVersion() +
              " does not match metastore's schema version " + schemaVer +
              " Metastore is not upgraded or corrupt");
        } else {
          LOG.warn("Metastore version was " + schemaVer + " " +
              HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString() +
              " is not enabled so recording the new schema version " +
              MetaStoreSchemaInfo.getHiveSchemaVersion());
          setMetaStoreSchemaVersion(MetaStoreSchemaInfo.getHiveSchemaVersion(),
              "Set by MetaStore");
        }
      }
    }
    isSchemaVerified.set(true);
    return;
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

  @SuppressWarnings("unchecked")
  private MVersionTable getMSchemaVersion()
      throws NoSuchObjectException, MetaException {
    boolean committed = false;
    List<MVersionTable> mVerTables = new ArrayList<MVersionTable>();

    try {
      openTransaction();
      Query query = pm.newQuery(MVersionTable.class);

      try {
        mVerTables = (List<MVersionTable>)query.execute();
        pm.retrieveAll(mVerTables);
      } catch (JDODataStoreException e) {
        if (e.getCause() instanceof MissingTableException) {
          throw new MetaException("Version table not found. " +
              "The metastore is not upgraded to " + MetaStoreSchemaInfo.getHiveSchemaVersion());
        } else {
          throw e;
        }
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
    if (mVerTables.isEmpty()) {
      throw new NoSuchObjectException("No matching version found");
    }
    if (mVerTables.size() > 1) {
      throw new MetaException("Metastore contains multiple versions");
    }
    return mVerTables.get(0);
  }

  @Override
  public void setMetaStoreSchemaVersion(String schemaVersion, String comment) throws MetaException {
    MVersionTable mSchemaVer;
    boolean commited = false;

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
      if (!commited) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName, List<String> partVals)
      throws MetaException {
    boolean success = false;
    try {
      openTransaction();
      dbName = dbName.toLowerCase().trim();
      tableName = tableName.toLowerCase().trim();

      // TODO: this could also be passed from upper layer; or this method should filter the list.
      MTable mtbl = getMTable(dbName, tableName);
      if (mtbl == null) {
        success = commitTransaction();
        return false;
      }

      Query query = pm.newQuery(
          "select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
          + "where table.tableName == t1 && table.database.name == t2 && partitionName == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      query.setUnique(true);
      query.setResult("partitionName");
      String name = Warehouse.makePartName(
          convertToFieldSchemas(mtbl.getPartitionKeys()), partVals);
      String result = (String)query.execute(tableName, dbName, name);
      success = commitTransaction();
      return result != null;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  /** Add this to code to debug lexer if needed. DebugTokenStream may also be added here. */
  private void debugLexer(CommonTokenStream stream, FilterLexer lexer) {
    try {
      stream.fill();
      List<?> tokens = stream.getTokens();
      String report = "LEXER: tokens (" + ((tokens == null) ? "null" : tokens.size()) + "): ";
      if (tokens != null) {
        for (Object o : tokens) {
          if (o == null || !(o instanceof Token)) {
            report += "[not a token: " + o + "], ";
          } else {
            Token t = (Token)o;
            report += "[at " + t.getCharPositionInLine() + ": "
                + t.getType() + " " + t.getText() + "], ";
          }
        }
      }
      report += "; lexer error: " + lexer.errorMsg;
      LOG.error(report);
    } catch (Throwable t) {
      LOG.error("LEXER: tokens (error)", t);
    }
  }
}
