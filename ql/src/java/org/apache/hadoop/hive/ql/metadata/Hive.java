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

package org.apache.hadoop.hive.ql.metadata;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.serde.serdeConstants.COLLECTION_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.ESCAPE_CHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.LINE_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.MAPKEY_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.HiveIndexHandler;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPrunerUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.CreateTableAutomaticGrant;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

import com.google.common.collect.Sets;

/**
 * This class has functions that implement meta data/DDL operations using calls
 * to the metastore.
 * It has a metastore client instance it uses to communicate with the metastore.
 *
 * It is a thread local variable, and the instances is accessed using static
 * get methods in this class.
 */

public class Hive {

  static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Hive");

  private HiveConf conf = null;
  private IMetaStoreClient metaStoreClient;

  private static ThreadLocal<Hive> hiveDB = new ThreadLocal<Hive>() {
    @Override
    protected synchronized Hive initialValue() {
      return null;
    }

    @Override
    public synchronized void remove() {
      if (this.get() != null) {
        this.get().close();
      }
      super.remove();
    }
  };

  /**
   * Gets hive object for the current thread. If one is not initialized then a
   * new one is created If the new configuration is different in metadata conf
   * vars then a new one is created.
   *
   * @param c
   *          new Hive Configuration
   * @return Hive object for current thread
   * @throws HiveException
   *
   */
  public static Hive get(HiveConf c) throws HiveException {
    boolean needsRefresh = false;
    Hive db = hiveDB.get();
    if (db != null) {
      for (HiveConf.ConfVars oneVar : HiveConf.metaVars) {
        // Since metaVars are all of different types, use string for comparison
        String oldVar = db.getConf().get(oneVar.varname, "");
        String newVar = c.get(oneVar.varname, "");
        if (oldVar.compareToIgnoreCase(newVar) != 0) {
          needsRefresh = true;
          break;
        }
      }
    }
    return get(c, needsRefresh);
  }

  /**
   * get a connection to metastore. see get(HiveConf) function for comments
   *
   * @param c
   *          new conf
   * @param needsRefresh
   *          if true then creates a new one
   * @return The connection to the metastore
   * @throws HiveException
   */
  public static Hive get(HiveConf c, boolean needsRefresh) throws HiveException {
    Hive db = hiveDB.get();
    if (db == null || needsRefresh) {
      closeCurrent();
      c.set("fs.scheme.class", "dfs");
      Hive newdb = new Hive(c);
      hiveDB.set(newdb);
      return newdb;
    }
    db.conf = c;
    return db;
  }

  public static Hive get() throws HiveException {
    Hive db = hiveDB.get();
    if (db == null) {
      db = new Hive(new HiveConf(Hive.class));
      hiveDB.set(db);
    }
    return db;
  }

  public static void set(Hive hive) {
    hiveDB.set(hive);
  }

  public static void closeCurrent() {
    hiveDB.remove();
  }

  /**
   * Hive
   *
   * @param argFsRoot
   * @param c
   *
   */
  private Hive(HiveConf c) throws HiveException {
    conf = c;
  }

  /**
   * closes the connection to metastore for the calling thread
   */
  private void close() {
    LOG.debug("Closing current thread's connection to Hive Metastore.");
    if (metaStoreClient != null) {
      metaStoreClient.close();
      metaStoreClient = null;
    }
  }

  /**
   * Create a database
   * @param db
   * @param ifNotExist if true, will ignore AlreadyExistsException exception
   * @throws AlreadyExistsException
   * @throws HiveException
   */
  public void createDatabase(Database db, boolean ifNotExist)
      throws AlreadyExistsException, HiveException {
    try {
      getMSC().createDatabase(db);
    } catch (AlreadyExistsException e) {
      if (!ifNotExist) {
        throw e;
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Create a Database. Raise an error if a database with the same name already exists.
   * @param db
   * @throws AlreadyExistsException
   * @throws HiveException
   */
  public void createDatabase(Database db) throws AlreadyExistsException, HiveException {
    createDatabase(db, false);
  }

  /**
   * Drop a database.
   * @param name
   * @throws NoSuchObjectException
   * @throws HiveException
   * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#dropDatabase(java.lang.String)
   */
  public void dropDatabase(String name) throws HiveException, NoSuchObjectException {
    dropDatabase(name, true, false, false);
  }

  /**
   * Drop a database
   * @param name
   * @param deleteData
   * @param ignoreUnknownDb if true, will ignore NoSuchObjectException
   * @throws HiveException
   * @throws NoSuchObjectException
   */
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws HiveException, NoSuchObjectException {
    dropDatabase(name, deleteData, ignoreUnknownDb, false);
  }

  /**
   * Drop a database
   * @param name
   * @param deleteData
   * @param ignoreUnknownDb if true, will ignore NoSuchObjectException
   * @param cascade           if true, delete all tables on the DB if exists. Othewise, the query
   *                        will fail if table still exists.
   * @throws HiveException
   * @throws NoSuchObjectException
   */
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws HiveException, NoSuchObjectException {
    try {
      getMSC().dropDatabase(name, deleteData, ignoreUnknownDb, cascade);
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  /**
   * Creates a table metdata and the directory for the table data
   *
   * @param tableName
   *          name of the table
   * @param columns
   *          list of fields of the table
   * @param partCols
   *          partition keys of the table
   * @param fileInputFormat
   *          Class of the input format of the table data file
   * @param fileOutputFormat
   *          Class of the output format of the table data file
   * @throws HiveException
   *           thrown if the args are invalid or if the metadata or the data
   *           directory couldn't be created
   */
  public void createTable(String tableName, List<String> columns,
      List<String> partCols, Class<? extends InputFormat> fileInputFormat,
      Class<?> fileOutputFormat) throws HiveException {
    this.createTable(tableName, columns, partCols, fileInputFormat,
        fileOutputFormat, -1, null);
  }

  /**
   * Creates a table metdata and the directory for the table data
   *
   * @param tableName
   *          name of the table
   * @param columns
   *          list of fields of the table
   * @param partCols
   *          partition keys of the table
   * @param fileInputFormat
   *          Class of the input format of the table data file
   * @param fileOutputFormat
   *          Class of the output format of the table data file
   * @param bucketCount
   *          number of buckets that each partition (or the table itself) should
   *          be divided into
   * @throws HiveException
   *           thrown if the args are invalid or if the metadata or the data
   *           directory couldn't be created
   */
  public void createTable(String tableName, List<String> columns,
      List<String> partCols, Class<? extends InputFormat> fileInputFormat,
      Class<?> fileOutputFormat, int bucketCount, List<String> bucketCols)
      throws HiveException {
    if (columns == null) {
      throw new HiveException("columns not specified for table " + tableName);
    }

    Table tbl = newTable(tableName);
    tbl.setInputFormatClass(fileInputFormat.getName());
    tbl.setOutputFormatClass(fileOutputFormat.getName());

    for (String col : columns) {
      FieldSchema field = new FieldSchema(col, STRING_TYPE_NAME, "default");
      tbl.getCols().add(field);
    }

    if (partCols != null) {
      for (String partCol : partCols) {
        FieldSchema part = new FieldSchema();
        part.setName(partCol);
        part.setType(STRING_TYPE_NAME); // default partition key
        tbl.getPartCols().add(part);
      }
    }
    tbl.setSerializationLib(LazySimpleSerDe.class.getName());
    tbl.setNumBuckets(bucketCount);
    tbl.setBucketCols(bucketCols);
    createTable(tbl);
  }

  /**
   * Updates the existing table metadata with the new metadata.
   *
   * @param tblName
   *          name of the existing table
   * @param newTbl
   *          new name of the table. could be the old name
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterTable(String tblName, Table newTbl)
      throws InvalidOperationException, HiveException {
    Table t = newTable(tblName);
    try {
      // Remove the DDL_TIME so it gets refreshed
      if (newTbl.getParameters() != null) {
        newTbl.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      getMSC().alter_table(t.getDbName(), t.getTableName(), newTbl.getTTable());
    } catch (MetaException e) {
      throw new HiveException("Unable to alter table.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter table.", e);
    }
  }

  /**
   * Updates the existing index metadata with the new metadata.
   *
   * @param idxName
   *          name of the existing index
   * @param newIdx
   *          new name of the index. could be the old name
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterIndex(String dbName, String baseTblName, String idxName, Index newIdx)
      throws InvalidOperationException, HiveException {
    try {
      getMSC().alter_index(dbName, baseTblName, idxName, newIdx);
    } catch (MetaException e) {
      throw new HiveException("Unable to alter index.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter index.", e);
    }
  }

  /**
   * Updates the existing partition metadata with the new metadata.
   *
   * @param tblName
   *          name of the existing table
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterPartition(String tblName, Partition newPart)
      throws InvalidOperationException, HiveException {
    Table t = newTable(tblName);
    alterPartition(t.getDbName(), t.getTableName(), newPart);
  }

  /**
   * Updates the existing partition metadata with the new metadata.
   *
   * @param dbName
   *          name of the exiting table's database
   * @param tblName
   *          name of the existing table
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterPartition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, HiveException {
    try {
      // Remove the DDL time so that it gets refreshed
      if (newPart.getParameters() != null) {
        newPart.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      getMSC().alter_partition(dbName, tblName, newPart.getTPartition());

    } catch (MetaException e) {
      throw new HiveException("Unable to alter partition.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter partition.", e);
    }
  }

  /**
   * Updates the existing table metadata with the new metadata.
   *
   * @param tblName
   *          name of the existing table
   * @param newParts
   *          new partitions
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterPartitions(String tblName, List<Partition> newParts)
      throws InvalidOperationException, HiveException {
    Table t = newTable(tblName);
    List<org.apache.hadoop.hive.metastore.api.Partition> newTParts =
      new ArrayList<org.apache.hadoop.hive.metastore.api.Partition>();
    try {
      // Remove the DDL time so that it gets refreshed
      for (Partition tmpPart: newParts) {
        if (tmpPart.getParameters() != null) {
          tmpPart.getParameters().remove(hive_metastoreConstants.DDL_TIME);
        }
        newTParts.add(tmpPart.getTPartition());
      }
      getMSC().alter_partitions(t.getDbName(), t.getTableName(), newTParts);
    } catch (MetaException e) {
      throw new HiveException("Unable to alter partition.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter partition.", e);
    }
  }
  /**
   * Rename a old partition to new partition
   *
   * @param tbl
   *          existing table
   * @param oldPartSpec
   *          spec of old partition
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void renamePartition(Table tbl, Map<String, String> oldPartSpec, Partition newPart)
      throws HiveException {
    try {
      Map<String, String> newPartSpec = newPart.getSpec();
      if (oldPartSpec.keySet().size() != tbl.getPartCols().size()
          || newPartSpec.keySet().size() != tbl.getPartCols().size()) {
        throw new HiveException("Unable to rename partition to the same name: number of partition cols don't match. ");
      }
      if (!oldPartSpec.keySet().equals(newPartSpec.keySet())){
        throw new HiveException("Unable to rename partition to the same name: old and new partition cols don't match. ");
      }
      List<String> pvals = new ArrayList<String>();

      for (FieldSchema field : tbl.getPartCols()) {
        String val = oldPartSpec.get(field.getName());
        if (val == null || val.length() == 0) {
          throw new HiveException("get partition: Value for key "
              + field.getName() + " is null or empty");
        } else if (val != null){
          pvals.add(val);
        }
      }
      getMSC().renamePartition(tbl.getDbName(), tbl.getTableName(), pvals,
          newPart.getTPartition());

    } catch (InvalidOperationException e){
      throw new HiveException("Unable to rename partition.", e);
    } catch (MetaException e) {
      throw new HiveException("Unable to rename partition.", e);
    } catch (TException e) {
      throw new HiveException("Unable to rename partition.", e);
    }
  }

  public void alterDatabase(String dbName, Database db)
      throws HiveException {
    try {
      getMSC().alterDatabase(dbName, db);
    } catch (MetaException e) {
      throw new HiveException("Unable to alter database " + dbName, e);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Database " + dbName + " does not exists.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter database " + dbName, e);
    }
  }
  /**
   * Creates the table with the give objects
   *
   * @param tbl
   *          a table object
   * @throws HiveException
   */
  public void createTable(Table tbl) throws HiveException {
    createTable(tbl, false);
  }

  /**
   * Creates the table with the give objects
   *
   * @param tbl
   *          a table object
   * @param ifNotExists
   *          if true, ignore AlreadyExistsException
   * @throws HiveException
   */
  public void createTable(Table tbl, boolean ifNotExists) throws HiveException {
    try {
      if (tbl.getDbName() == null || "".equals(tbl.getDbName().trim())) {
        tbl.setDbName(SessionState.get().getCurrentDatabase());
      }
      if (tbl.getCols().size() == 0) {
        tbl.setFields(MetaStoreUtils.getFieldsFromDeserializer(tbl.getTableName(),
            tbl.getDeserializer()));
      }
      tbl.checkValidity();
      if (tbl.getParameters() != null) {
        tbl.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      org.apache.hadoop.hive.metastore.api.Table tTbl = tbl.getTTable();
      PrincipalPrivilegeSet principalPrivs = new PrincipalPrivilegeSet();
      SessionState ss = SessionState.get();
      if (ss != null) {
        CreateTableAutomaticGrant grants = ss.getCreateTableGrants();
        if (grants != null) {
          principalPrivs.setUserPrivileges(grants.getUserGrants());
          principalPrivs.setGroupPrivileges(grants.getGroupGrants());
          principalPrivs.setRolePrivileges(grants.getRoleGrants());
          tTbl.setPrivileges(principalPrivs);
        }
      }
      getMSC().createTable(tTbl);
    } catch (AlreadyExistsException e) {
      if (!ifNotExists) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   *
   * @param tableName
   *          table name
   * @param indexName
   *          index name
   * @param indexHandlerClass
   *          index handler class
   * @param indexedCols
   *          index columns
   * @param indexTblName
   *          index table's name
   * @param deferredRebuild
   *          referred build index table's data
   * @param inputFormat
   *          input format
   * @param outputFormat
   *          output format
   * @param serde
   * @param storageHandler
   *          index table's storage handler
   * @param location
   *          location
   * @param idxProps
   *          idx
   * @param serdeProps
   *          serde properties
   * @param collItemDelim
   * @param fieldDelim
   * @param fieldEscape
   * @param lineDelim
   * @param mapKeyDelim
   * @throws HiveException
   */
  public void createIndex(String tableName, String indexName, String indexHandlerClass,
      List<String> indexedCols, String indexTblName, boolean deferredRebuild,
      String inputFormat, String outputFormat, String serde,
      String storageHandler, String location,
      Map<String, String> idxProps, Map<String, String> tblProps, Map<String, String> serdeProps,
      String collItemDelim, String fieldDelim, String fieldEscape,
      String lineDelim, String mapKeyDelim, String indexComment)
      throws HiveException {

    try {
      String dbName = SessionState.get().getCurrentDatabase();
      Index old_index = null;
      try {
        old_index = getIndex(dbName, tableName, indexName);
      } catch (Exception e) {
      }
      if (old_index != null) {
        throw new HiveException("Index " + indexName + " already exists on table " + tableName + ", db=" + dbName);
      }

      org.apache.hadoop.hive.metastore.api.Table baseTbl = getMSC().getTable(dbName, tableName);
      if (baseTbl.getTableType() == TableType.VIRTUAL_VIEW.toString()) {
        throw new HiveException("tableName="+ tableName +" is a VIRTUAL VIEW. Index on VIRTUAL VIEW is not supported.");
      }

      if (indexTblName == null) {
        indexTblName = MetaStoreUtils.getIndexTableName(dbName, tableName, indexName);
      } else {
        org.apache.hadoop.hive.metastore.api.Table temp = null;
        try {
          temp = getMSC().getTable(dbName, indexTblName);
        } catch (Exception e) {
        }
        if (temp != null) {
          throw new HiveException("Table name " + indexTblName + " already exists. Choose another name.");
        }
      }

      org.apache.hadoop.hive.metastore.api.StorageDescriptor storageDescriptor = baseTbl.getSd().deepCopy();
      SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
      if(serde != null) {
        serdeInfo.setSerializationLib(serde);
      } else {
        if (storageHandler == null) {
          serdeInfo.setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
        } else {
          HiveStorageHandler sh = HiveUtils.getStorageHandler(getConf(), storageHandler);
          String serDeClassName = sh.getSerDeClass().getName();
          serdeInfo.setSerializationLib(serDeClassName);
        }
      }

      if (fieldDelim != null) {
        serdeInfo.getParameters().put(FIELD_DELIM, fieldDelim);
        serdeInfo.getParameters().put(SERIALIZATION_FORMAT, fieldDelim);
      }
      if (fieldEscape != null) {
        serdeInfo.getParameters().put(ESCAPE_CHAR, fieldEscape);
      }
      if (collItemDelim != null) {
        serdeInfo.getParameters().put(COLLECTION_DELIM, collItemDelim);
      }
      if (mapKeyDelim != null) {
        serdeInfo.getParameters().put(MAPKEY_DELIM, mapKeyDelim);
      }
      if (lineDelim != null) {
        serdeInfo.getParameters().put(LINE_DELIM, lineDelim);
      }

      if (serdeProps != null) {
        Iterator<Entry<String, String>> iter = serdeProps.entrySet()
          .iterator();
        while (iter.hasNext()) {
          Entry<String, String> m = iter.next();
          serdeInfo.getParameters().put(m.getKey(), m.getValue());
        }
      }

      storageDescriptor.setLocation(null);
      if (location != null) {
        storageDescriptor.setLocation(location);
      }
      storageDescriptor.setInputFormat(inputFormat);
      storageDescriptor.setOutputFormat(outputFormat);

      Map<String, String> params = new HashMap<String,String>();

      List<FieldSchema> indexTblCols = new ArrayList<FieldSchema>();
      List<Order> sortCols = new ArrayList<Order>();
      storageDescriptor.setBucketCols(null);
      int k = 0;
      Table metaBaseTbl = new Table(baseTbl);
      for (int i = 0; i < metaBaseTbl.getCols().size(); i++) {
        FieldSchema col = metaBaseTbl.getCols().get(i);
        if (indexedCols.contains(col.getName())) {
          indexTblCols.add(col);
          sortCols.add(new Order(col.getName(), 1));
          k++;
        }
      }
      if (k != indexedCols.size()) {
        throw new RuntimeException(
            "Check the index columns, they should appear in the table being indexed.");
      }

      storageDescriptor.setCols(indexTblCols);
      storageDescriptor.setSortCols(sortCols);

      int time = (int) (System.currentTimeMillis() / 1000);
      org.apache.hadoop.hive.metastore.api.Table tt = null;
      HiveIndexHandler indexHandler = HiveUtils.getIndexHandler(this.getConf(), indexHandlerClass);

      if (indexHandler.usesIndexTable()) {
        tt = new org.apache.hadoop.hive.ql.metadata.Table(dbName, indexTblName).getTTable();
        List<FieldSchema> partKeys = baseTbl.getPartitionKeys();
        tt.setPartitionKeys(partKeys);
        tt.setTableType(TableType.INDEX_TABLE.toString());
        if (tblProps != null) {
          for (Entry<String, String> prop : tblProps.entrySet()) {
            tt.putToParameters(prop.getKey(), prop.getValue());
          }
        }
      }

      if(!deferredRebuild) {
        throw new RuntimeException("Please specify deferred rebuild using \" WITH DEFERRED REBUILD \".");
      }

      Index indexDesc = new Index(indexName, indexHandlerClass, dbName, tableName, time, time, indexTblName,
          storageDescriptor, params, deferredRebuild);
      if (indexComment != null) {
        indexDesc.getParameters().put("comment", indexComment);
      }

      if (idxProps != null)
      {
        indexDesc.getParameters().putAll(idxProps);
      }

      indexHandler.analyzeIndexDefinition(baseTbl, indexDesc, tt);

      this.getMSC().createIndex(indexDesc, tt);

    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public Index getIndex(String qualifiedIndexName) throws HiveException {
    String[] names = getQualifiedNames(qualifiedIndexName);
    switch (names.length) {
    case 3:
      return getIndex(names[0], names[1], names[2]);
    case 2:
      return getIndex(SessionState.get().getCurrentDatabase(),
          names[0], names[1]);
    default:
      throw new HiveException("Invalid index name:" + qualifiedIndexName);
    }
  }

  public Index getIndex(String baseTableName, String indexName) throws HiveException {
    Table t = newTable(baseTableName);
    return this.getIndex(t.getDbName(), t.getTableName(), indexName);
  }

  public Index getIndex(String dbName, String baseTableName,
      String indexName) throws HiveException {
    try {
      return this.getMSC().getIndex(dbName, baseTableName, indexName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean dropIndex(String db_name, String tbl_name, String index_name, boolean deleteData) throws HiveException {
    try {
      return getMSC().dropIndex(db_name, tbl_name, index_name, deleteData);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Partition or table doesn't exist.", e);
    } catch (Exception e) {
      throw new HiveException("Unknown error. Please check logs.", e);
    }
  }

  /**
   * Drops table along with the data in it. If the table doesn't exist then it
   * is a no-op
   *
   * @param tableName
   *          table to drop
   * @throws HiveException
   *           thrown if the drop fails
   */
  public void dropTable(String tableName) throws HiveException {
    Table t = newTable(tableName);
    dropTable(t.getDbName(), t.getTableName(), true, true);
  }

  /**
   * Drops table along with the data in it. If the table doesn't exist then it
   * is a no-op
   *
   * @param dbName
   *          database where the table lives
   * @param tableName
   *          table to drop
   * @throws HiveException
   *           thrown if the drop fails
   */
  public void dropTable(String dbName, String tableName) throws HiveException {
    dropTable(dbName, tableName, true, true);
  }

  /**
   * Drops the table.
   *
   * @param dbName
   * @param tableName
   * @param deleteData
   *          deletes the underlying data along with metadata
   * @param ignoreUnknownTab
   *          an exception if thrown if this is falser and table doesn't exist
   * @throws HiveException
   */
  public void dropTable(String dbName, String tableName, boolean deleteData,
      boolean ignoreUnknownTab) throws HiveException {

    try {
      getMSC().dropTable(dbName, tableName, deleteData, ignoreUnknownTab);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public HiveConf getConf() {
    return (conf);
  }

  /**
   * Returns metadata for the table named tableName
   * @param tableName the name of the table
   * @return the table metadata
   * @throws HiveException if there's an internal error or if the
   * table doesn't exist
   */
  public Table getTable(final String tableName) throws HiveException {
    Table t = newTable(tableName);
    return this.getTable(t.getDbName(), t.getTableName(), true);
  }

  /**
   * Returns metadata for the table named tableName
   * @param tableName the name of the table
   * @param throwException controls whether an exception is thrown or a returns a null
   * @return the table metadata
   * @throws HiveException if there's an internal error or if the
   * table doesn't exist
   */
  public Table getTable(final String tableName, boolean throwException) throws HiveException {
    Table t = newTable(tableName);
    return this.getTable(t.getDbName(), t.getTableName(), throwException);
  }

  /**
   * Returns metadata of the table
   *
   * @param dbName
   *          the name of the database
   * @param tableName
   *          the name of the table
   * @return the table
   * @exception HiveException
   *              if there's an internal error or if the table doesn't exist
   */
  public Table getTable(final String dbName, final String tableName) throws HiveException {
    if (tableName.contains(".")) {
      Table t = newTable(tableName);
      return this.getTable(t.getDbName(), t.getTableName(), true);
    } else {
      return this.getTable(dbName, tableName, true);
    }
  }

  /**
   * Returns metadata of the table
   *
   * @param dbName
   *          the name of the database
   * @param tableName
   *          the name of the table
   * @param throwException
   *          controls whether an exception is thrown or a returns a null
   * @return the table or if throwException is false a null value.
   * @throws HiveException
   */
  public Table getTable(final String dbName, final String tableName,
      boolean throwException) throws HiveException {

    if (tableName == null || tableName.equals("")) {
      throw new HiveException("empty table creation??");
    }

    // Get the table from metastore
    org.apache.hadoop.hive.metastore.api.Table tTable = null;
    try {
      tTable = getMSC().getTable(dbName, tableName);
    } catch (NoSuchObjectException e) {
      if (throwException) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidTableException(tableName);
      }
      return null;
    } catch (Exception e) {
      throw new HiveException("Unable to fetch table " + tableName, e);
    }

    // For non-views, we need to do some extra fixes
    if (!TableType.VIRTUAL_VIEW.toString().equals(tTable.getTableType())) {
      // Fix the non-printable chars
      Map<String, String> parameters = tTable.getSd().getParameters();
      String sf = parameters.get(SERIALIZATION_FORMAT);
      if (sf != null) {
        char[] b = sf.toCharArray();
        if ((b.length == 1) && (b[0] < 10)) { // ^A, ^B, ^C, ^D, \t
          parameters.put(SERIALIZATION_FORMAT, Integer.toString(b[0]));
        }
      }

      // Use LazySimpleSerDe for MetadataTypedColumnsetSerDe.
      // NOTE: LazySimpleSerDe does not support tables with a single column of
      // col
      // of type "array<string>". This happens when the table is created using
      // an
      // earlier version of Hive.
      if (org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class
          .getName().equals(
            tTable.getSd().getSerdeInfo().getSerializationLib())
          && tTable.getSd().getColsSize() > 0
          && tTable.getSd().getCols().get(0).getType().indexOf('<') == -1) {
        tTable.getSd().getSerdeInfo().setSerializationLib(
            org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      }
    }

    Table table = new Table(tTable);

    table.checkValidity();
    return table;
  }

  /**
   * Get all table names for the current database.
   * @return List of table names
   * @throws HiveException
   */
  public List<String> getAllTables() throws HiveException {
    return getAllTables(SessionState.get().getCurrentDatabase());
  }

  /**
   * Get all table names for the specified database.
   * @param dbName
   * @return List of table names
   * @throws HiveException
   */
  public List<String> getAllTables(String dbName) throws HiveException {
    return getTablesByPattern(dbName, ".*");
  }

  /**
   * Returns all existing tables from default database which match the given
   * pattern. The matching occurs as per Java regular expressions
   *
   * @param tablePattern
   *          java re pattern
   * @return list of table names
   * @throws HiveException
   */
  public List<String> getTablesByPattern(String tablePattern) throws HiveException {
    return getTablesByPattern(SessionState.get().getCurrentDatabase(),
        tablePattern);
  }

  /**
   * Returns all existing tables from the specified database which match the given
   * pattern. The matching occurs as per Java regular expressions.
   * @param dbName
   * @param tablePattern
   * @return list of table names
   * @throws HiveException
   */
  public List<String> getTablesByPattern(String dbName, String tablePattern) throws HiveException {
    try {
      return getMSC().getTables(dbName, tablePattern);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Returns all existing tables from the given database which match the given
   * pattern. The matching occurs as per Java regular expressions
   *
   * @param database
   *          the database name
   * @param tablePattern
   *          java re pattern
   * @return list of table names
   * @throws HiveException
   */
  public List<String> getTablesForDb(String database, String tablePattern)
      throws HiveException {
    try {
      return getMSC().getTables(database, tablePattern);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all existing database names.
   *
   * @return List of database names.
   * @throws HiveException
   */
  public List<String> getAllDatabases() throws HiveException {
    try {
      return getMSC().getAllDatabases();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all existing databases that match the given
   * pattern. The matching occurs as per Java regular expressions
   *
   * @param databasePattern
   *          java re pattern
   * @return list of database names
   * @throws HiveException
   */
  public List<String> getDatabasesByPattern(String databasePattern) throws HiveException {
    try {
      return getMSC().getDatabases(databasePattern);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean grantPrivileges(PrivilegeBag privileges)
      throws HiveException {
    try {
      return getMSC().grant_privileges(privileges);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * @param privileges
   *          a bag of privileges
   * @return true on success
   * @throws HiveException
   */
  public boolean revokePrivileges(PrivilegeBag privileges)
      throws HiveException {
    try {
      return getMSC().revoke_privileges(privileges);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Query metadata to see if a database with the given name already exists.
   *
   * @param dbName
   * @return true if a database with the given name already exists, false if
   *         does not exist.
   * @throws HiveException
   */
  public boolean databaseExists(String dbName) throws HiveException {
    return getDatabase(dbName) != null;
  }

  /**
   * Get the database by name.
   * @param dbName the name of the database.
   * @return a Database object if this database exists, null otherwise.
   * @throws HiveException
   */
  public Database getDatabase(String dbName) throws HiveException {
    try {
      return getMSC().getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get the Database object for current database
   * @return a Database object if this database exists, null otherwise.
   * @throws HiveException
   */
  public Database getDatabaseCurrent() throws HiveException {
    String currentDb = SessionState.get().getCurrentDatabase();
    return getDatabase(currentDb);
  }

  /**
   * Load a directory into a Hive Table Partition - Alters existing content of
   * the partition with the contents of loadPath. - If the partition does not
   * exist - one is created - files in loadPath are moved into Hive. But the
   * directory itself is not removed.
   *
   * @param loadPath
   *          Directory containing files to load into Table
   * @param tableName
   *          name of table to be loaded.
   * @param partSpec
   *          defines which partition needs to be loaded
   * @param replace
   *          if true - replace files in the partition, otherwise add files to
   *          the partition
   * @param holdDDLTime if true, force [re]create the partition
   * @param inheritTableSpecs if true, on [re]creating the partition, take the
   *          location/inputformat/outputformat/serde details from table spec
   */
  public void loadPartition(Path loadPath, String tableName,
      Map<String, String> partSpec, boolean replace, boolean holdDDLTime,
      boolean inheritTableSpecs, boolean isSkewedStoreAsSubdir)
      throws HiveException {
    Table tbl = getTable(tableName);
    try {
      /**
       * Move files before creating the partition since down stream processes
       * check for existence of partition in metadata before accessing the data.
       * If partition is created before data is moved, downstream waiting
       * processes might move forward with partial data
       */

      Partition oldPart = getPartition(tbl, partSpec, false);
      Path oldPartPath = null;
      if(oldPart != null) {
        oldPartPath = oldPart.getPartitionPath();
      }

      Path newPartPath = null;

      if (inheritTableSpecs) {
        Path partPath = new Path(tbl.getDataLocation().getPath(),
            Warehouse.makePartPath(partSpec));
        newPartPath = new Path(loadPath.toUri().getScheme(), loadPath.toUri().getAuthority(),
            partPath.toUri().getPath());

        if(oldPart != null) {
          /*
           * If we are moving the partition across filesystem boundaries
           * inherit from the table properties. Otherwise (same filesystem) use the
           * original partition location.
           *
           * See: HIVE-1707 and HIVE-2117 for background
           */
          FileSystem oldPartPathFS = oldPartPath.getFileSystem(getConf());
          FileSystem loadPathFS = loadPath.getFileSystem(getConf());
          if (oldPartPathFS.equals(loadPathFS)) {
            newPartPath = oldPartPath;
          }
        }
      } else {
        newPartPath = oldPartPath;
      }

      if (replace) {
        Hive.replaceFiles(loadPath, newPartPath, oldPartPath, getConf());
      } else {
        FileSystem fs = FileSystem.get(tbl.getDataLocation(), getConf());
        Hive.copyFiles(conf, loadPath, newPartPath, fs);
      }

      // recreate the partition if it existed before
      if (!holdDDLTime) {
        Partition newTPart = getPartition(tbl, partSpec, true, newPartPath.toString(),
            inheritTableSpecs);
        if (isSkewedStoreAsSubdir) {
          org.apache.hadoop.hive.metastore.api.Partition newCreatedTpart = newTPart.getTPartition();
          SkewedInfo skewedInfo = newCreatedTpart.getSd().getSkewedInfo();
          /* Construct list bucketing location mappings from sub-directory name. */
          Map<List<String>, String> skewedColValueLocationMaps = constructListBucketingLocationMap(
              newPartPath, skewedInfo);
          /* Add list bucketing location mappings. */
          skewedInfo.setSkewedColValueLocationMaps(skewedColValueLocationMaps);
          newCreatedTpart.getSd().setSkewedInfo(skewedInfo);
          alterPartition(tbl.getTableName(), new Partition(tbl, newCreatedTpart));
          newTPart = getPartition(tbl, partSpec, true, newPartPath.toString(), inheritTableSpecs);
          newCreatedTpart = newTPart.getTPartition();
        }
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    } catch (MetaException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    } catch (InvalidOperationException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }

  }

  /**
 * Walk through sub-directory tree to construct list bucketing location map.
 *
 * @param fSta
 * @param fSys
 * @param skewedColValueLocationMaps
 * @param newPartPath
 * @param skewedInfo
 * @throws IOException
 */
private void walkDirTree(FileStatus fSta, FileSystem fSys,
    Map<List<String>, String> skewedColValueLocationMaps, Path newPartPath, SkewedInfo skewedInfo)
    throws IOException {
  /* Base Case. It's leaf. */
  if (!fSta.isDir()) {
    /* construct one location map if not exists. */
    constructOneLBLocationMap(fSta, skewedColValueLocationMaps, newPartPath, skewedInfo);
    return;
  }

  /* dfs. */
  FileStatus[] children = fSys.listStatus(fSta.getPath());
  if (children != null) {
    for (FileStatus child : children) {
      walkDirTree(child, fSys, skewedColValueLocationMaps, newPartPath, skewedInfo);
    }
  }
}

/**
 * Construct a list bucketing location map
 * @param fSta
 * @param skewedColValueLocationMaps
 * @param newPartPath
 * @param skewedInfo
 */
private void constructOneLBLocationMap(FileStatus fSta,
    Map<List<String>, String> skewedColValueLocationMaps,
    Path newPartPath, SkewedInfo skewedInfo) {
  Path lbdPath = fSta.getPath().getParent();
  List<String> skewedValue = new ArrayList<String>();
  String lbDirName = FileUtils.unescapePathName(lbdPath.toString());
  String partDirName = FileUtils.unescapePathName(newPartPath.toString());
  String lbDirSuffix = lbDirName.replace(partDirName, "");
  String[] dirNames = lbDirSuffix.split(Path.SEPARATOR);
  for (String dirName : dirNames) {
    if ((dirName != null) && (dirName.length() > 0)) {
      // Construct skewed-value to location map except default directory.
      // why? query logic knows default-dir structure and don't need to get from map
        if (!dirName
            .equalsIgnoreCase(ListBucketingPrunerUtils.HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME)) {
        String[] kv = dirName.split("=");
        if (kv.length == 2) {
          skewedValue.add(kv[1]);
        }
      }
    }
  }
  if ((skewedValue.size() > 0) && (skewedValue.size() == skewedInfo.getSkewedColNames().size())
      && !skewedColValueLocationMaps.containsKey(skewedValue)) {
    skewedColValueLocationMaps.put(skewedValue, lbdPath.toString());
  }
}

  /**
   * Construct location map from path
   *
   * @param newPartPath
   * @param skewedInfo
   * @return
   * @throws IOException
   * @throws FileNotFoundException
   */
  private Map<List<String>, String> constructListBucketingLocationMap(Path newPartPath,
      SkewedInfo skewedInfo) throws IOException, FileNotFoundException {
    Map<List<String>, String> skewedColValueLocationMaps = new HashMap<List<String>, String>();
    FileSystem fSys = newPartPath.getFileSystem(conf);
    walkDirTree(fSys.getFileStatus(newPartPath), fSys, skewedColValueLocationMaps, newPartPath,
        skewedInfo);
    return skewedColValueLocationMaps;
  }


  /**
   * Given a source directory name of the load path, load all dynamically generated partitions
   * into the specified table and return a list of strings that represent the dynamic partition
   * paths.
   * @param loadPath
   * @param tableName
   * @param partSpec
   * @param replace
   * @param numDP number of dynamic partitions
   * @param holdDDLTime
   * @return a list of strings with the dynamic partition paths
   * @throws HiveException
   */
  public ArrayList<LinkedHashMap<String, String>> loadDynamicPartitions(Path loadPath,
      String tableName, Map<String, String> partSpec, boolean replace,
      int numDP, boolean holdDDLTime, boolean listBucketingEnabled)
      throws HiveException {

    Set<Path> validPartitions = new HashSet<Path>();
    try {
      ArrayList<LinkedHashMap<String, String>> fullPartSpecs =
        new ArrayList<LinkedHashMap<String, String>>();

      FileSystem fs = loadPath.getFileSystem(conf);
      FileStatus[] leafStatus = HiveStatsUtils.getFileStatusRecurse(loadPath, numDP+1, fs);
      // Check for empty partitions
      for (FileStatus s : leafStatus) {
        // Check if the hadoop version supports sub-directories for tables/partitions
        if (s.isDir() &&
          !conf.getBoolVar(HiveConf.ConfVars.HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES)) {
          // No leaves in this directory
          LOG.info("NOT moving empty directory: " + s.getPath());
        } else {
          try {
            validatePartitionNameCharacters(
                Warehouse.getPartValuesFromPartName(s.getPath().getParent().toString()));
          } catch (MetaException e) {
            throw new HiveException(e);
          }
          validPartitions.add(s.getPath().getParent());
        }
      }

      if (validPartitions.size() == 0) {
        LOG.warn("No partition is generated by dynamic partitioning");
      }

      if (validPartitions.size() > conf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS)) {
        throw new HiveException("Number of dynamic partitions created is " + validPartitions.size()
            + ", which is more than "
            + conf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS)
            +". To solve this try to set " + HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.varname
            + " to at least " + validPartitions.size() + '.');
      }

      // for each dynamically created DP directory, construct a full partition spec
      // and load the partition based on that
      Iterator<Path> iter = validPartitions.iterator();
      while (iter.hasNext()) {
        // get the dynamically created directory
        Path partPath = iter.next();
        assert fs.getFileStatus(partPath).isDir():
          "partitions " + partPath + " is not a directory !";

        // generate a full partition specification
        LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<String, String>(partSpec);
        Warehouse.makeSpecFromName(fullPartSpec, partPath);
        fullPartSpecs.add(fullPartSpec);

        // finally load the partition -- move the file to the final table address
        loadPartition(partPath, tableName, fullPartSpec, replace, holdDDLTime, true,
            listBucketingEnabled);
        LOG.info("New loading path = " + partPath + " with partSpec " + fullPartSpec);
      }
      return fullPartSpecs;
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Load a directory into a Hive Table. - Alters existing content of table with
   * the contents of loadPath. - If table does not exist - an exception is
   * thrown - files in loadPath are moved into Hive. But the directory itself is
   * not removed.
   *
   * @param loadPath
   *          Directory containing files to load into Table
   * @param tableName
   *          name of table to be loaded.
   * @param replace
   *          if true - replace files in the table, otherwise add files to table
   * @param holdDDLTime
   */
  public void loadTable(Path loadPath, String tableName, boolean replace,
      boolean holdDDLTime) throws HiveException {
    Table tbl = getTable(tableName);

    if (replace) {
      tbl.replaceFiles(loadPath);
    } else {
      tbl.copyFiles(loadPath);
    }

    if (!holdDDLTime) {
      try {
        alterTable(tableName, tbl);
      } catch (InvalidOperationException e) {
        throw new HiveException(e);
      }
    }
  }

 /**
   * Creates a partition.
   *
   * @param tbl
   *          table for which partition needs to be created
   * @param partSpec
   *          partition keys and their values
   * @return created partition object
   * @throws HiveException
   *           if table doesn't exist or partition already exists
   */
  public Partition createPartition(Table tbl, Map<String, String> partSpec)
      throws HiveException {
    return createPartition(tbl, partSpec, null, null, null, null, -1,
        null, null, null, null, null);
  }

  /**
   * Creates a partition
   *
   * @param tbl
   *          table for which partition needs to be created
   * @param partSpec
   *          partition keys and their values
   * @param location
   *          location of this partition
   * @param partParams
   *          partition parameters
   * @param inputFormat the inputformat class
   * @param outputFormat the outputformat class
   * @param numBuckets the number of buckets
   * @param cols the column schema
   * @param serializationLib the serde class
   * @param serdeParams the serde parameters
   * @param bucketCols the bucketing columns
   * @param sortCols sort columns and order
   *
   * @return created partition object
   * @throws HiveException
   *           if table doesn't exist or partition already exists
   */
  public Partition createPartition(Table tbl, Map<String, String> partSpec,
      Path location, Map<String, String> partParams, String inputFormat, String outputFormat,
      int numBuckets, List<FieldSchema> cols,
      String serializationLib, Map<String, String> serdeParams,
      List<String> bucketCols, List<Order> sortCols) throws HiveException {

    org.apache.hadoop.hive.metastore.api.Partition partition = null;

    for (FieldSchema field : tbl.getPartCols()) {
      String val = partSpec.get(field.getName());
      if (val == null || val.length() == 0) {
        throw new HiveException("add partition: Value for key "
            + field.getName() + " is null or empty");
      }
    }

    try {
      Partition tmpPart = new Partition(tbl, partSpec, location);
      // No need to clear DDL_TIME in parameters since we know it's
      // not populated on construction.
      org.apache.hadoop.hive.metastore.api.Partition inPart
        = tmpPart.getTPartition();
      if (partParams != null) {
        inPart.setParameters(partParams);
      }
      if (inputFormat != null) {
        inPart.getSd().setInputFormat(inputFormat);
      }
      if (outputFormat != null) {
        inPart.getSd().setOutputFormat(outputFormat);
      }
      if (numBuckets != -1) {
        inPart.getSd().setNumBuckets(numBuckets);
      }
      if (cols != null) {
        inPart.getSd().setCols(cols);
      }
      if (serializationLib != null) {
          inPart.getSd().getSerdeInfo().setSerializationLib(serializationLib);
      }
      if (serdeParams != null) {
        inPart.getSd().getSerdeInfo().setParameters(serdeParams);
      }
      if (bucketCols != null) {
        inPart.getSd().setBucketCols(bucketCols);
      }
      if (sortCols != null) {
        inPart.getSd().setSortCols(sortCols);
      }
      partition = getMSC().add_partition(inPart);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }

    return new Partition(tbl, partition);
  }

  public Partition getPartition(Table tbl, Map<String, String> partSpec,
      boolean forceCreate) throws HiveException {
    return getPartition(tbl, partSpec, forceCreate, null, true);
  }

  private static void clearPartitionStats(org.apache.hadoop.hive.metastore.api.Partition tpart) {
    Map<String,String> tpartParams = tpart.getParameters();
    if (tpartParams == null) {
      return;
    }

    for (String statType : StatsSetupConst.supportedStats) {
      tpartParams.remove(statType);
    }
  }

  /**
   * Returns partition metadata
   *
   * @param tbl
   *          the partition's table
   * @param partSpec
   *          partition keys and values
   * @param forceCreate
   *          if this is true and partition doesn't exist then a partition is
   *          created
   * @param partPath the path where the partition data is located
   * @param inheritTableSpecs whether to copy over the table specs for if/of/serde
   * @return result partition object or null if there is no partition
   * @throws HiveException
   */
  public Partition getPartition(Table tbl, Map<String, String> partSpec,
      boolean forceCreate, String partPath, boolean inheritTableSpecs) throws HiveException {
    if (!tbl.isValidSpec(partSpec)) {
      throw new HiveException("Invalid partition: " + partSpec);
    }
    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : tbl.getPartCols()) {
      String val = partSpec.get(field.getName());
      // enable dynamic partitioning
      if (val == null && !HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING)
          || val.length() == 0) {
        throw new HiveException("get partition: Value for key "
            + field.getName() + " is null or empty");
      } else if (val != null){
        pvals.add(val);
      }
    }
    org.apache.hadoop.hive.metastore.api.Partition tpart = null;
    try {
      tpart = getMSC().getPartitionWithAuthInfo(tbl.getDbName(),
          tbl.getTableName(), pvals, getUserName(), getGroupNames());
    } catch (NoSuchObjectException nsoe) {
      // this means no partition exists for the given partition
      // key value pairs - thrift cannot handle null return values, hence
      // getPartition() throws NoSuchObjectException to indicate null partition
      tpart = null;
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    try {
      if (forceCreate) {
        if (tpart == null) {
          LOG.debug("creating partition for table " + tbl.getTableName()
                    + " with partition spec : " + partSpec);
          tpart = getMSC().appendPartition(tbl.getDbName(), tbl.getTableName(), pvals);
        }
        else {
          LOG.debug("altering partition for table " + tbl.getTableName()
                    + " with partition spec : " + partSpec);
          if (inheritTableSpecs) {
            tpart.getSd().setOutputFormat(tbl.getTTable().getSd().getOutputFormat());
            tpart.getSd().setInputFormat(tbl.getTTable().getSd().getInputFormat());
            tpart.getSd().getSerdeInfo().setSerializationLib(tbl.getSerializationLib());
            tpart.getSd().getSerdeInfo().setParameters(
                tbl.getTTable().getSd().getSerdeInfo().getParameters());
            tpart.getSd().setBucketCols(tbl.getBucketCols());
            tpart.getSd().setNumBuckets(tbl.getNumBuckets());
            tpart.getSd().setSortCols(tbl.getSortCols());
          }
          if (partPath == null || partPath.trim().equals("")) {
            throw new HiveException("new partition path should not be null or empty.");
          }
          tpart.getSd().setLocation(partPath);
          clearPartitionStats(tpart);
          String fullName = tbl.getTableName();
          if (!org.apache.commons.lang.StringUtils.isEmpty(tbl.getDbName())) {
            fullName = tbl.getDbName() + "." + tbl.getTableName();
          }
          alterPartition(fullName, new Partition(tbl, tpart));
        }
      }
      if (tpart == null) {
        return null;
      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return new Partition(tbl, tpart);
  }

  public boolean dropPartition(String tblName, List<String> part_vals, boolean deleteData)
  throws HiveException {
    Table t = newTable(tblName);
    return dropPartition(t.getDbName(), t.getTableName(), part_vals, deleteData);
  }

  public boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, boolean deleteData) throws HiveException {
    try {
      return getMSC().dropPartition(db_name, tbl_name, part_vals, deleteData);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Partition or table doesn't exist.", e);
    } catch (Exception e) {
      throw new HiveException("Unknown error. Please check logs.", e);
    }
  }

  public List<String> getPartitionNames(String tblName, short max) throws HiveException {
    Table t = newTable(tblName);
    return getPartitionNames(t.getDbName(), t.getTableName(), max);
  }

  public List<String> getPartitionNames(String dbName, String tblName, short max)
      throws HiveException {
    List<String> names = null;
    try {
      names = getMSC().listPartitionNames(dbName, tblName, max);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return names;
  }

  public List<String> getPartitionNames(String dbName, String tblName,
      Map<String, String> partSpec, short max) throws HiveException {
    List<String> names = null;
    Table t = getTable(dbName, tblName);

    List<String> pvals = MetaStoreUtils.getPvals(t.getPartCols(), partSpec);

    try {
      names = getMSC().listPartitionNames(dbName, tblName, pvals, max);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return names;
  }

  /**
   * get all the partitions that the table has
   *
   * @param tbl
   *          object for which partition is needed
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitions(Table tbl) throws HiveException {
    if (tbl.isPartitioned()) {
      List<org.apache.hadoop.hive.metastore.api.Partition> tParts;
      try {
        tParts = getMSC().listPartitionsWithAuthInfo(tbl.getDbName(), tbl.getTableName(),
            (short) -1, getUserName(), getGroupNames());
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new HiveException(e);
      }
      List<Partition> parts = new ArrayList<Partition>(tParts.size());
      for (org.apache.hadoop.hive.metastore.api.Partition tpart : tParts) {
        parts.add(new Partition(tbl, tpart));
      }
      return parts;
    } else {
      Partition part = new Partition(tbl);
      ArrayList<Partition> parts = new ArrayList<Partition>(1);
      parts.add(part);
      return parts;
    }
  }

  /**
   * Get all the partitions; unlike {@link #getPartitions(Table)}, does not include auth.
   * @param tbl table for which partitions are needed
   * @return list of partition objects
   */
  public Set<Partition> getAllPartitionsOf(Table tbl) throws HiveException {
    if (!tbl.isPartitioned()) {
      return Sets.newHashSet(new Partition(tbl));
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> tParts;
    try {
      tParts = getMSC().listPartitions(tbl.getDbName(), tbl.getTableName(), (short)-1);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    Set<Partition> parts = new LinkedHashSet<Partition>(tParts.size());
    for (org.apache.hadoop.hive.metastore.api.Partition tpart : tParts) {
      parts.add(new Partition(tbl, tpart));
    }
    return parts;
  }

  /**
   * get all the partitions of the table that matches the given partial
   * specification. partition columns whose value is can be anything should be
   * an empty string.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @param limit number of partitions to return
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitions(Table tbl, Map<String, String> partialPartSpec,
      short limit)
  throws HiveException {
    if (!tbl.isPartitioned()) {
      throw new HiveException("Partition spec should only be supplied for a " +
          "partitioned table");
    }

    List<String> partialPvals = MetaStoreUtils.getPvals(tbl.getPartCols(), partialPartSpec);

    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = null;
    try {
      partitions = getMSC().listPartitionsWithAuthInfo(tbl.getDbName(), tbl.getTableName(),
          partialPvals, limit, getUserName(), getGroupNames());
    } catch (Exception e) {
      throw new HiveException(e);
    }

    List<Partition> qlPartitions = new ArrayList<Partition>();
    for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
      qlPartitions.add( new Partition(tbl, p));
    }

    return qlPartitions;
  }

  /**
   * get all the partitions of the table that matches the given partial
   * specification. partition columns whose value is can be anything should be
   * an empty string.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitions(Table tbl, Map<String, String> partialPartSpec)
  throws HiveException {
    return getPartitions(tbl, partialPartSpec, (short)-1);
  }

  /**
   * get all the partitions of the table that matches the given partial
   * specification. partition columns whose value is can be anything should be
   * an empty string.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @param partialPartSpec
   *          partial partition specification (some subpartitions can be empty).
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitionsByNames(Table tbl,
      Map<String, String> partialPartSpec)
      throws HiveException {

    if (!tbl.isPartitioned()) {
      throw new HiveException("Partition spec should only be supplied for a " +
                "partitioned table");
    }

    List<String> names = getPartitionNames(tbl.getDbName(), tbl.getTableName(),
        partialPartSpec, (short)-1);

    List<Partition> partitions = getPartitionsByNames(tbl, names);
    return partitions;
  }

  /**
   * Get all partitions of the table that matches the list of given partition names.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @param partNames
   *          list of partition names
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitionsByNames(Table tbl, List<String> partNames)
      throws HiveException {

    if (!tbl.isPartitioned()) {
      throw new HiveException("Partition spec should only be supplied for a " +
          "partitioned table");
    }
    List<Partition> partitions = new ArrayList<Partition>(partNames.size());

    int batchSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX);
    // TODO: might want to increase the default batch size. 1024 is viable; MS gets OOM if too high.
    int nParts = partNames.size();
    int nBatches = nParts / batchSize;

    try {
      for (int i = 0; i < nBatches; ++i) {
        List<org.apache.hadoop.hive.metastore.api.Partition> tParts =
          getMSC().getPartitionsByNames(tbl.getDbName(), tbl.getTableName(),
          partNames.subList(i*batchSize, (i+1)*batchSize));
        if (tParts != null) {
          for (org.apache.hadoop.hive.metastore.api.Partition tpart: tParts) {
            partitions.add(new Partition(tbl, tpart));
          }
        }
      }

      if (nParts > nBatches * batchSize) {
        List<org.apache.hadoop.hive.metastore.api.Partition> tParts =
          getMSC().getPartitionsByNames(tbl.getDbName(), tbl.getTableName(),
          partNames.subList(nBatches*batchSize, nParts));
        if (tParts != null) {
          for (org.apache.hadoop.hive.metastore.api.Partition tpart: tParts) {
            partitions.add(new Partition(tbl, tpart));
          }
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return partitions;
  }

  /**
   * Get a list of Partitions by filter.
   * @param tbl The table containing the partitions.
   * @param filter A string represent partition predicates.
   * @return a list of partitions satisfying the partition predicates.
   * @throws HiveException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  public List<Partition> getPartitionsByFilter(Table tbl, String filter)
      throws HiveException, MetaException, NoSuchObjectException, TException {

    if (!tbl.isPartitioned()) {
      throw new HiveException("Partition spec should only be supplied for a " +
          "partitioned table");
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> tParts = getMSC().listPartitionsByFilter(
        tbl.getDbName(), tbl.getTableName(), filter, (short)-1);
    return convertFromMetastore(tbl, tParts, null);
  }

  private static List<Partition> convertFromMetastore(Table tbl,
      List<org.apache.hadoop.hive.metastore.api.Partition> src,
      List<Partition> dest) throws HiveException {
    if (src == null) {
      return dest;
    }
    if (dest == null) {
      dest = new ArrayList<Partition>(src.size());
    }
    for (org.apache.hadoop.hive.metastore.api.Partition tPart : src) {
      dest.add(new Partition(tbl, tPart));
    }
    return dest;
  }

  /**
   * Get a list of Partitions by expr.
   * @param tbl The table containing the partitions.
   * @param expr A serialized expression for partition predicates.
   * @param conf Hive config.
   * @param result the resulting list of partitions
   * @return whether the resulting list contains partitions which may or may not match the expr
   */
  public boolean getPartitionsByExpr(Table tbl, ExprNodeGenericFuncDesc expr, HiveConf conf,
      List<Partition> result) throws HiveException, TException {
    assert result != null;
    byte[] exprBytes = Utilities.serializeExpressionToKryo(expr);
    String defaultPartitionName = HiveConf.getVar(conf, ConfVars.DEFAULTPARTITIONNAME);
    List<org.apache.hadoop.hive.metastore.api.Partition> msParts =
        new ArrayList<org.apache.hadoop.hive.metastore.api.Partition>();
    boolean hasUnknownParts = getMSC().listPartitionsByExpr(tbl.getDbName(),
        tbl.getTableName(), exprBytes, defaultPartitionName, (short)-1, msParts);
    convertFromMetastore(tbl, msParts, result);
    return hasUnknownParts;
  }

  public void validatePartitionNameCharacters(List<String> partVals) throws HiveException {
    try {
      getMSC().validatePartitionNameCharacters(partVals);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public void createRole(String roleName, String ownerName)
      throws HiveException {
    try {
      getMSC().create_role(new Role(roleName, -1, ownerName));
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropRole(String roleName) throws HiveException {
    try {
      getMSC().drop_role(roleName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all existing role names.
   *
   * @return List of role names.
   * @throws HiveException
   */
  public List<String> getAllRoleNames() throws HiveException {
    try {
      return getMSC().listRoleNames();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<Role> showRoleGrant(String principalName, PrincipalType principalType) throws HiveException {
    try {
      return getMSC().list_roles(principalName, principalType);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean grantRole(String roleName, String userName,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption) throws HiveException {
    try {
      return getMSC().grant_role(roleName, userName, principalType, grantor,
          grantorType, grantOption);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean revokeRole(String roleName, String userName,
      PrincipalType principalType)  throws HiveException {
    try {
      return getMSC().revoke_role(roleName, userName, principalType);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<Role> listRoles(String userName,  PrincipalType principalType)
      throws HiveException {
    try {
      return getMSC().list_roles(userName, principalType);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * @param objectType
   *          hive object type
   * @param db_name
   *          database name
   * @param table_name
   *          table name
   * @param part_values
   *          partition values
   * @param column_name
   *          column name
   * @param user_name
   *          user name
   * @param group_names
   *          group names
   * @return the privilege set
   * @throws HiveException
   */
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectType objectType,
      String db_name, String table_name, List<String> part_values,
      String column_name, String user_name, List<String> group_names)
      throws HiveException {
    try {
      HiveObjectRef hiveObj = new HiveObjectRef(objectType, db_name,
          table_name, part_values, column_name);
      return getMSC().get_privilege_set(hiveObj, user_name, group_names);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * @param objectType
   *          hive object type
   * @param principalName
   * @param principalType
   * @param dbName
   * @param tableName
   * @param partValues
   * @param columnName
   * @return list of privileges
   * @throws HiveException
   */
  public List<HiveObjectPrivilege> showPrivilegeGrant(
      HiveObjectType objectType, String principalName,
      PrincipalType principalType, String dbName, String tableName,
      List<String> partValues, String columnName) throws HiveException {
    try {
      HiveObjectRef hiveObj = new HiveObjectRef(objectType, dbName, tableName,
          partValues, columnName);
      return getMSC().list_privileges(principalName, principalType, hiveObj);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  // for each file or directory in 'srcs', make mapping for every file in src to safe name in dest
  private static List<List<Path[]>> checkPaths(HiveConf conf,
      FileSystem fs, FileStatus[] srcs, Path destf,
      boolean replace) throws HiveException {

    List<List<Path[]>> result = new ArrayList<List<Path[]>>();
    try {
      FileStatus destStatus = !replace && fs.exists(destf) ? fs.getFileStatus(destf) : null;
      if (destStatus != null && !destStatus.isDir()) {
        throw new HiveException("checkPaths: destination " + destf
            + " should be a directory");
      }
      for (FileStatus src : srcs) {
        FileStatus[] items;
        if (src.isDir()) {
          items = fs.listStatus(src.getPath());
          Arrays.sort(items);
        } else {
          items = new FileStatus[] {src};
        }

        List<Path[]> srcToDest = new ArrayList<Path[]>();
        for (FileStatus item : items) {

          Path itemSource = item.getPath();

          if (Utilities.isTempPath(item)) {
            // This check is redundant because temp files are removed by
            // execution layer before
            // calling loadTable/Partition. But leaving it in just in case.
            fs.delete(itemSource, true);
            continue;
          }

          if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES) &&
            item.isDir()) {
            throw new HiveException("checkPaths: " + src.getPath()
                + " has nested directory" + itemSource);
          }
          // Strip off the file type, if any so we don't make:
          // 000000_0.gz -> 000000_0.gz_copy_1
          String name = itemSource.getName();
          String filetype;
          int index = name.lastIndexOf('.');
          if (index >= 0) {
            filetype = name.substring(index);
            name = name.substring(0, index);
          } else {
            filetype = "";
          }

          Path itemDest = new Path(destf, itemSource.getName());

          if (!replace) {
            // It's possible that the file we're copying may have the same
            // relative name as an existing file in the "destf" directory.
            // So let's make a quick check to see if we can rename any
            // potential offenders so as to allow them to move into the
            // "destf" directory. The scheme is dead simple: simply tack
            // on "_copy_N" where N starts at 1 and works its way up until
            // we find a free space.

            // removed source file staging.. it's more confusing when faild.
            for (int counter = 1; fs.exists(itemDest) || destExists(result, itemDest); counter++) {
              itemDest = new Path(destf, name + ("_copy_" + counter) + filetype);
            }
          }
          srcToDest.add(new Path[]{itemSource, itemDest});
        }
        result.add(srcToDest);
      }
    } catch (IOException e) {
      throw new HiveException("checkPaths: filesystem error in check phase", e);
    }
    return result;
  }

  private static boolean destExists(List<List<Path[]>> result, Path proposed) {
    for (List<Path[]> sdpairs : result) {
      for (Path[] sdpair : sdpairs) {
        if (sdpair[1].equals(proposed)) {
          return true;
        }
      }
    }
    return false;
  }

  //it is assumed that parent directory of the destf should already exist when this
  //method is called. when the replace value is true, this method works a little different
  //from mv command if the destf is a directory, it replaces the destf instead of moving under
  //the destf. in this case, the replaced destf still preserves the original destf's permission
  static protected boolean renameFile(HiveConf conf, Path srcf, Path destf, FileSystem fs,
      boolean replace) throws HiveException {
    boolean success = false;
    boolean inheritPerms = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);
    String group = null;
    String permission = null;

    try {
      if (inheritPerms || replace) {
        try{
          FileStatus deststatus = fs.getFileStatus(destf);
          if (inheritPerms) {
            group = deststatus.getGroup();
            permission= Integer.toString(deststatus.getPermission().toShort(), 8);
          }
          //if destf is an existing directory:
          //if replace is true, delete followed by rename(mv) is equivalent to replace
          //if replace is false, rename (mv) actually move the src under dest dir
          //if destf is an existing file, rename is actually a replace, and do not need
          // to delete the file first
          if (replace && deststatus.isDir()) {
            fs.delete(destf, true);
          }
        } catch (FileNotFoundException ignore) {
          //if dest dir does not exist, any re
          if (inheritPerms) {
            FileStatus deststatus = fs.getFileStatus(destf.getParent());
            group = deststatus.getGroup();
            permission= Integer.toString(deststatus.getPermission().toShort(), 8);
          }
        }
      }
      success = fs.rename(srcf, destf);
      LOG.debug((replace ? "Replacing src:" : "Renaming src:") + srcf.toString()
          + ";dest: " + destf.toString()  + ";Status:" + success);
    } catch (IOException ioe) {
      throw new HiveException("Unable to move source" + srcf + " to destination " + destf, ioe);
    }

    if (success && inheritPerms) {
      //use FsShell to change group and permissions recursively
      try {
        FsShell fshell = new FsShell();
        fshell.setConf(conf);
        fshell.run(new String[]{"-chgrp", "-R", group, destf.toString()});
        fshell.run(new String[]{"-chmod", "-R", permission, destf.toString()});
      } catch (Exception e) {
        throw new HiveException("Unable to set permissions of " + destf, e);
      }
    }
    return success;
  }

  static protected void copyFiles(HiveConf conf, Path srcf, Path destf, FileSystem fs)
      throws HiveException {
    boolean inheritPerms = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);
    try {
      // create the destination if it does not exist
      if (!fs.exists(destf)) {
        fs.mkdirs(destf);
        if (inheritPerms) {
          fs.setPermission(destf, fs.getFileStatus(destf.getParent()).getPermission());
        }
      }
    } catch (IOException e) {
      throw new HiveException(
          "copyFiles: error while checking/creating destination directory!!!",
          e);
    }

    FileStatus[] srcs;
    try {
      srcs = fs.globStatus(srcf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
    if (srcs == null) {
      LOG.info("No sources specified to move: " + srcf);
      return;
      // srcs = new FileStatus[0]; Why is this needed?
    }
    // check that source and target paths exist
    List<List<Path[]>> result = checkPaths(conf, fs, srcs, destf, false);

    // move it, move it
    try {
      for (List<Path[]> sdpairs : result) {
        for (Path[] sdpair : sdpairs) {
          if (!renameFile(conf, sdpair[0], sdpair[1], fs, false)) {
            throw new IOException("Cannot move " + sdpair[0] + " to " + sdpair[1]);
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException("copyFiles: error while moving files!!!", e);
    }
  }

  /**
   * Replaces files in the partition with new data set specified by srcf. Works
   * by renaming directory of srcf to the destination file.
   * srcf, destf, and tmppath should resident in the same DFS, but the oldPath can be in a
   * different DFS.
   *
   * @param srcf
   *          Source directory to be renamed to tmppath. It should be a
   *          leaf directory where the final data files reside. However it
   *          could potentially contain subdirectories as well.
   * @param destf
   *          The directory where the final data needs to go
   * @param oldPath
   *          The directory where the old data location, need to be cleaned up.
   */
  static protected void replaceFiles(Path srcf, Path destf, Path oldPath, HiveConf conf)
      throws HiveException {
    try {
      FileSystem fs = srcf.getFileSystem(conf);
      boolean inheritPerms = HiveConf.getBoolVar(conf,
          HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);

      // check if srcf contains nested sub-directories
      FileStatus[] srcs;
      try {
        srcs = fs.globStatus(srcf);
      } catch (IOException e) {
        throw new HiveException("Getting globStatus " + srcf.toString(), e);
      }
      if (srcs == null) {
        LOG.info("No sources specified to move: " + srcf);
        return;
      }
      List<List<Path[]>> result = checkPaths(conf, fs, srcs, destf, true);

      // point of no return -- delete oldPath only if it is not same as destf,
      // otherwise, the oldPath/destf will be cleaned later just before move
      if (oldPath != null && (!destf.getFileSystem(conf).equals(oldPath.getFileSystem(conf))
          || !destf.equals(oldPath))) {
        try {
          FileSystem fs2 = oldPath.getFileSystem(conf);
          if (fs2.exists(oldPath)) {
            // use FsShell to move data to .Trash first rather than delete permanently
            FsShell fshell = new FsShell();
            fshell.setConf(conf);
            fshell.run(new String[]{"-rmr", oldPath.toString()});
          }
        } catch (Exception e) {
          //swallow the exception
          LOG.warn("Directory " + oldPath.toString() + " canot be removed.");
        }
      }

      // rename src directory to destf
      if (srcs.length == 1 && srcs[0].isDir()) {
        // rename can fail if the parent doesn't exist
        Path destfp = destf.getParent();
        if (!fs.exists(destfp)) {
          boolean success = fs.mkdirs(destfp);
          if (inheritPerms && success) {
            fs.setPermission(destfp, fs.getFileStatus(destfp.getParent()).getPermission());
          }
        }

        boolean b = renameFile(conf, srcs[0].getPath(), destf, fs, true);
        if (!b) {
          throw new HiveException("Unable to move results from " + srcs[0].getPath()
              + " to destination directory: " + destf);
        }
      } else { // srcf is a file or pattern containing wildcards
        if (!fs.exists(destf)) {
          boolean success = fs.mkdirs(destf);
          if (inheritPerms && success) {
            fs.setPermission(destf, fs.getFileStatus(destf.getParent()).getPermission());
          }
        }
        // srcs must be a list of files -- ensured by LoadSemanticAnalyzer
        for (List<Path[]> sdpairs : result) {
          for (Path[] sdpair : sdpairs) {
            if (!renameFile(conf, sdpair[0], sdpair[1], fs, true)) {
              throw new IOException("Error moving: " + sdpair[0] + " into: " + sdpair[1]);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

  public void exchangeTablePartitions(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destDb,
      String destinationTableName) throws HiveException {
    try {
      getMSC().exchange_partition(partitionSpecs, sourceDb, sourceTable, destDb,
        destinationTableName);
    } catch (Exception ex) {
      LOG.error(StringUtils.stringifyException(ex));
      throw new HiveException(ex);
    }
  }

  /**
   * Creates a metastore client. Currently it creates only JDBC based client as
   * File based store support is removed
   *
   * @returns a Meta Store Client
   * @throws HiveMetaException
   *           if a working client can't be created
   */
  private IMetaStoreClient createMetaStoreClient() throws MetaException {

    HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
        public HiveMetaHook getHook(
          org.apache.hadoop.hive.metastore.api.Table tbl)
          throws MetaException {

          try {
            if (tbl == null) {
              return null;
            }
            HiveStorageHandler storageHandler =
              HiveUtils.getStorageHandler(conf,
                tbl.getParameters().get(META_TABLE_STORAGE));
            if (storageHandler == null) {
              return null;
            }
            return storageHandler.getMetaHook();
          } catch (HiveException ex) {
            LOG.error(StringUtils.stringifyException(ex));
            throw new MetaException(
              "Failed to load storage handler:  " + ex.getMessage());
          }
        }
      };
    return RetryingMetaStoreClient.getProxy(conf, hookLoader,
        HiveMetaStoreClient.class.getName());
  }

  /**
   * @return the metastore client for the current thread
   * @throws MetaException
   */
  @LimitedPrivate(value = {"Hive"})
  @Unstable
  public IMetaStoreClient getMSC() throws MetaException {
    if (metaStoreClient == null) {
      metaStoreClient = createMetaStoreClient();
    }
    return metaStoreClient;
  }

  private String getUserName() {
    SessionState ss = SessionState.get();
    if (ss != null && ss.getAuthenticator() != null) {
      return ss.getAuthenticator().getUserName();
    }
    return null;
  }

  private List<String> getGroupNames() {
    SessionState ss = SessionState.get();
    if (ss != null && ss.getAuthenticator() != null) {
      return ss.getAuthenticator().getGroupNames();
    }
    return null;
  }

  public static List<FieldSchema> getFieldsFromDeserializer(String name,
      Deserializer serde) throws HiveException {
    try {
      return MetaStoreUtils.getFieldsFromDeserializer(name, serde);
    } catch (SerDeException e) {
      throw new HiveException("Error in getting fields from serde. "
          + e.getMessage(), e);
    } catch (MetaException e) {
      throw new HiveException("Error in getting fields from serde."
          + e.getMessage(), e);
    }
  }

  public List<Index> getIndexes(String dbName, String tblName, short max) throws HiveException {
    List<Index> indexes = null;
    try {
      indexes = getMSC().listIndexes(dbName, tblName, max);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return indexes;
  }

  public boolean updateTableColumnStatistics(ColumnStatistics statsObj) throws HiveException {
    try {
      return getMSC().updateTableColumnStatistics(statsObj);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj) throws HiveException {
    try {
      return getMSC().updatePartitionColumnStatistics(statsObj);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName, String colName)
    throws HiveException {
    try {
      return getMSC().getTableColumnStatistics(dbName, tableName, colName);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }

  }

  public ColumnStatistics getPartitionColumnStatistics(String dbName, String tableName,
    String partName, String colName) throws HiveException {
      try {
        return getMSC().getPartitionColumnStatistics(dbName, tableName, partName, colName);
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new HiveException(e);
      }
    }

  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
    throws HiveException {
    try {
      return getMSC().deleteTableColumnStatistics(dbName, tableName, colName);
    } catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
    String colName) throws HiveException {
      try {
        return getMSC().deletePartitionColumnStatistics(dbName, tableName, partName, colName);
      } catch(Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new HiveException(e);
      }
    }

  public Table newTable(String tableName) throws HiveException {
    String[] names = getQualifiedNames(tableName);
    switch (names.length) {
    case 2:
      return new Table(names[0], names[1]);
    case 1:
      return new Table(SessionState.get().getCurrentDatabase(), names[0]);
    default:
      try{
        throw new HiveException("Invalid table name: " + tableName);
      }catch(Exception e) {
        e.printStackTrace();
      }
      throw new HiveException("Invalid table name: " + tableName);
    }
  }

  public String getDelegationToken(String owner, String renewer)
    throws HiveException{
    try {
      return getMSC().getDelegationToken(owner, renewer);
    } catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public void cancelDelegationToken(String tokenStrForm)
    throws HiveException {
    try {
      getMSC().cancelDelegationToken(tokenStrForm);
    }  catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  private static String[] getQualifiedNames(String qualifiedName) {
    return qualifiedName.split("\\.");
  }

};
