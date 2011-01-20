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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.DataStoreCache;

import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MIndex;
import org.apache.hadoop.hive.metastore.model.MOrder;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MRole;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MRoleMap;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.hadoop.hive.metastore.model.MType;
import org.apache.hadoop.hive.metastore.parser.FilterLexer;
import org.apache.hadoop.hive.metastore.parser.FilterParser;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.ANTLRNoCaseStringStream;
import org.apache.hadoop.util.StringUtils;

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

  private boolean isInitialized = false;
  private PersistenceManager pm = null;
  private Configuration hiveConf;
  int openTrasactionCalls = 0;
  private Transaction currentTransaction = null;
  private TXN_STATUS transactionStatus = TXN_STATUS.NO_STATE;

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
    return;
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
        Object prevVal = prop.setProperty(e.getKey(), e.getValue());
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

  private static PersistenceManagerFactory getPMF() {
    if (pmf == null) {
      pmf = JDOHelper.getPersistenceManagerFactory(prop);
      DataStoreCache dsc = pmf.getDataStoreCache();
      if (dsc != null) {
        dsc.pinAll(true, MTable.class);
        dsc.pinAll(true, MStorageDescriptor.class);
        dsc.pinAll(true, MSerDeInfo.class);
        dsc.pinAll(true, MPartition.class);
        dsc.pinAll(true, MDatabase.class);
        dsc.pinAll(true, MType.class);
        dsc.pinAll(true, MFieldSchema.class);
        dsc.pinAll(true, MOrder.class);
      }
    }
    return pmf;
  }

  private PersistenceManager getPersistenceManager() {
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

      // first drop tables
      for (String tableName : getAllTables(dbname)) {
        dropTable(dbname, tableName);
      }

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
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MType.class, "name == typeName");
      query.declareParameters("java.lang.String typeName");
      query.setUnique(true);
      MType type = (MType) query.execute(typeName.trim());
      pm.retrieve(type);
      pm.deletePersistent(type);
      commited = commitTransaction();
      success = true;
    } catch (JDOObjectNotFoundException e) {
      commited = commitTransaction();
      LOG.debug("type not found " + typeName, e);
    } finally {
      if (!commited) {
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

  public boolean dropTable(String dbName, String tableName) throws MetaException {
    boolean success = false;
    try {
      openTransaction();
      MTable tbl = getMTable(dbName, tableName);
      pm.retrieve(tbl);
      if (tbl != null) {
        // first remove all the partitions
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
        pm.deletePersistentAll(listMPartitions(dbName, tableName, -1));
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
      keys = new ArrayList<Order>();
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

  // MSD and SD should be same objects. Not sure how to make then same right now
  // MSerdeInfo *& SerdeInfo should be same as well
  private StorageDescriptor convertToStorageDescriptor(MStorageDescriptor msd)
      throws MetaException {
    if (msd == null) {
      return null;
    }
    return new StorageDescriptor(convertToFieldSchemas(msd.getCols()), msd
        .getLocation(), msd.getInputFormat(), msd.getOutputFormat(), msd
        .isCompressed(), msd.getNumBuckets(), converToSerDeInfo(msd
        .getSerDeInfo()), msd.getBucketCols(), convertToOrders(msd
        .getSortCols()), msd.getParameters());
  }

  private MStorageDescriptor convertToMStorageDescriptor(StorageDescriptor sd)
      throws MetaException {
    if (sd == null) {
      return null;
    }
    return new MStorageDescriptor(convertToMFieldSchemas(sd.getCols()), sd
        .getLocation(), sd.getInputFormat(), sd.getOutputFormat(), sd
        .isCompressed(), sd.getNumBuckets(), converToMSerDeInfo(sd
        .getSerdeInfo()), sd.getBucketCols(),
        convertToMOrders(sd.getSortCols()), sd.getParameters());
  }

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
      MPartition mpart = convertToMPart(part);
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

  private MPartition convertToMPart(Partition part)
      throws InvalidObjectException, MetaException {
    if (part == null) {
      return null;
    }
    MTable mt = getMTable(part.getDbName(), part.getTableName());
    if (mt == null) {
      throw new InvalidObjectException(
          "Partition doesn't have a valid table or database name");
    }
    return new MPartition(Warehouse.makePartName(convertToFieldSchemas(mt
        .getPartitionKeys()), part.getValues()), mt, part.getValues(), part
        .getCreateTime(), part.getLastAccessTime(),
        convertToMStorageDescriptor(part.getSd()), part.getParameters());
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

  public boolean dropPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException {
    boolean success = false;
    try {
      openTransaction();
      MPartition part = getMPartition(dbName, tableName, part_vals);
      if (part != null) {
        List<MFieldSchema> schemas = part.getTable().getPartitionKeys();
        List<String> colNames = new ArrayList<String>();
        for (MFieldSchema col: schemas) {
          colNames.add(col.getName());
        }
        String partName = FileUtils.makePartName(colNames, part_vals);
        
        List<MPartitionPrivilege> partGrants = listPartitionGrants(
            dbName, tableName, partName);

        if (partGrants != null && partGrants.size() > 0) {
          pm.deletePersistentAll(partGrants);
        }

        List<MPartitionColumnPrivilege> partColumnGrants = listPartitionAllColumnGrants(
            dbName, tableName, partName);
        if (partColumnGrants != null && partColumnGrants.size() > 0) {
          pm.deletePersistentAll(partColumnGrants);
        }

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

  public List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException {
    openTransaction();
    List<Partition> parts = convertToParts(listMPartitions(dbName, tableName,
        max));
    commitTransaction();
    return parts;
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName,
      short maxParts, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    boolean success = false;
    try {
      openTransaction();
      List<MPartition> mparts = listMPartitions(dbName, tblName, maxParts);
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


  private List<Partition> convertToParts(List<MPartition> mparts)
      throws MetaException {
    List<Partition> parts = new ArrayList<Partition>(mparts.size());
    for (MPartition mp : mparts) {
      parts.add(convertToPart(mp));
    }
    return parts;
  }

  // TODO:pc implement max
  public List<String> listPartitionNames(String dbName, String tableName,
      short max) throws MetaException {
    List<String> pns = new ArrayList<String>();
    boolean success = false;
    try {
      openTransaction();
      LOG.debug("Executing getPartitionNames");
      dbName = dbName.toLowerCase().trim();
      tableName = tableName.toLowerCase().trim();
      Query q = pm.newQuery(
          "select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
          + "where table.database.name == t1 && table.tableName == t2 "
          + "order by partitionName asc");
      q.declareParameters("java.lang.String t1, java.lang.String t2");
      q.setResult("partitionName");
      Collection names = (Collection) q.execute(dbName, tableName);
      pns = new ArrayList<String>();
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
      mparts = (List<MPartition>) query.execute(tableName, dbName);
      LOG.debug("Done executing query for listMPartitions");
      pm.retrieveAll(mparts);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMPartitions");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mparts;
  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName, String tblName,
      String filter, short maxParts) throws MetaException, NoSuchObjectException {
    openTransaction();
    List<Partition> parts = convertToParts(listMPartitionsByFilter(dbName,
        tblName, filter, maxParts));
    commitTransaction();
    return parts;
  }

  private String makeQueryFilterString(MTable mtable, String filter,
      Map<String, String> params)
      throws MetaException {
    StringBuilder queryBuilder = new StringBuilder(
        "table.tableName == t1 && table.database.name == t2");

    if( filter != null && filter.length() > 0) {

      Table table = convertToTable(mtable);

      CharStream cs = new ANTLRNoCaseStringStream(filter);
      FilterLexer lexer = new FilterLexer(cs);

      CommonTokenStream tokens = new CommonTokenStream();
      tokens.setTokenSource (lexer);

      FilterParser parser = new FilterParser(tokens);

      try {
        parser.filter();
      } catch(RecognitionException re) {
        throw new MetaException("Error parsing partition filter : " + re);
      }

      String jdoFilter = parser.tree.generateJDOFilter(table, params);

      if( jdoFilter.trim().length() > 0 ) {
        queryBuilder.append(" && ( ");
        queryBuilder.append(jdoFilter.trim());
        queryBuilder.append(" )");
      }
    }

    return queryBuilder.toString();
  }

  private String makeParameterDeclarationString(Map<String, String> params) {
    //Create the parameter declaration string
    StringBuilder paramDecl = new StringBuilder();
    for(String key : params.keySet() ) {
      paramDecl.append(", java.lang.String  " + key);
    }
    return paramDecl.toString();
  }

  private List<MPartition> listMPartitionsByFilter(String dbName, String tableName,
      String filter, short maxParts) throws MetaException, NoSuchObjectException{
    boolean success = false;
    List<MPartition> mparts = null;
    try {
      openTransaction();
      LOG.debug("Executing listMPartitionsByFilter");
      dbName = dbName.toLowerCase();
      tableName = tableName.toLowerCase();

      MTable mtable = getMTable(dbName, tableName);
      if( mtable == null ) {
        throw new NoSuchObjectException("Specified database/table does not exist : "
            + dbName + "." + tableName);
      }
      Map<String, String> params = new HashMap<String, String>();
      String queryFilterString =
        makeQueryFilterString(mtable, filter, params);

      Query query = pm.newQuery(MPartition.class,
          queryFilterString);

      if( maxParts >= 0 ) {
        //User specified a row limit, set it on the Query
        query.setRange(0, maxParts);
      }

      LOG.debug("Filter specified is " + filter + "," +
             " JDOQL filter is " + queryFilterString);

      params.put("t1", tableName.trim());
      params.put("t2", dbName.trim());

      String parameterDeclaration = makeParameterDeclarationString(params);
      query.declareParameters(parameterDeclaration);
      query.setOrdering("partitionName ascending");

      mparts = (List<MPartition>) query.executeWithMap(params);

      LOG.debug("Done executing query for listMPartitionsByFilter");
      pm.retrieveAll(mparts);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMPartitionsByFilter");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mparts;
  }

  @Override
  public List<String> listPartitionNamesByFilter(String dbName, String tableName,
      String filter, short maxParts) throws MetaException {
    boolean success = false;
    List<String> partNames = new ArrayList<String>();
    try {
      openTransaction();
      LOG.debug("Executing listMPartitionsByFilter");
      dbName = dbName.toLowerCase();
      tableName = tableName.toLowerCase();

      MTable mtable = getMTable(dbName, tableName);
      if( mtable == null ) {
        // To be consistent with the behavior of listPartitionNames, if the
        // table or db does not exist, we return an empty list
        return partNames;
      }
      Map<String, String> params = new HashMap<String, String>();
      String queryFilterString =
        makeQueryFilterString(mtable, filter, params);

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

      params.put("t1", tableName.trim());
      params.put("t2", dbName.trim());

      String parameterDeclaration = makeParameterDeclarationString(params);
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
      oldt.setSd(newt.getSd());
      oldt.setDatabase(newt.getDatabase());
      oldt.setRetention(newt.getRetention());
      oldt.setPartitionKeys(newt.getPartitionKeys());
      oldt.setTableType(newt.getTableType());
      oldt.setLastAccessTime(newt.getLastAccessTime());

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

  public void alterPartition(String dbname, String name, Partition newPart)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      name = name.toLowerCase();
      dbname = dbname.toLowerCase();
      MPartition oldp = getMPartition(dbname, name, newPart.getValues());
      MPartition newp = convertToMPart(newPart);
      if (oldp == null || newp == null) {
        throw new InvalidObjectException("partition does not exist.");
      }
      oldp.setParameters(newPart.getParameters());
      copyMSD(newp.getSd(), oldp.getSd());
      if (newp.getCreateTime() != oldp.getCreateTime()) {
        oldp.setCreateTime(newp.getCreateTime());
      }
      if (newp.getLastAccessTime() != oldp.getLastAccessTime()) {
        oldp.setLastAccessTime(newp.getLastAccessTime());
      }
      // commit the changes
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  private void copyMSD(MStorageDescriptor newSd, MStorageDescriptor oldSd) {
    oldSd.setLocation(newSd.getLocation());
    oldSd.setCols(newSd.getCols());
    oldSd.setBucketCols(newSd.getBucketCols());
    oldSd.setCompressed(newSd.isCompressed());
    oldSd.setInputFormat(newSd.getInputFormat());
    oldSd.setOutputFormat(newSd.getOutputFormat());
    oldSd.setNumBuckets(newSd.getNumBuckets());
    oldSd.getSerDeInfo().setName(newSd.getSerDeInfo().getName());
    oldSd.getSerDeInfo().setSerializationLib(
        newSd.getSerDeInfo().getSerializationLib());
    oldSd.getSerDeInfo().setParameters(newSd.getSerDeInfo().getParameters());
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
      pns = new ArrayList<String>();
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

}
