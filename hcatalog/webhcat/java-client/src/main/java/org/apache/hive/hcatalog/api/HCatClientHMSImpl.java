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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.hcatalog.api.repl.HCatReplicationTaskIterator;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * The HCatClientHMSImpl is the Hive Metastore client based implementation of
 * HCatClient.
 */
public class HCatClientHMSImpl extends HCatClient {

  private static final Logger LOG = LoggerFactory.getLogger(HCatClientHMSImpl.class);
  private IMetaStoreClient hmsClient;
  private Configuration config;
  private HiveConf hiveConfig;

  @Override
  public List<String> listDatabaseNamesByPattern(String pattern)
    throws HCatException {
    List<String> dbNames = null;
    try {
      dbNames = hmsClient.getDatabases(pattern);
    } catch (MetaException exp) {
      throw new HCatException("MetaException while listing db names. " + exp.getMessage(), exp);
    } catch (TException e) {
      throw new HCatException("Transport Exception while listing db names. " + e.getMessage(), e);
    }
    return dbNames;
  }

  @Override
  public HCatDatabase getDatabase(String dbName) throws HCatException {
    HCatDatabase db = null;
    try {
      Database hiveDB = hmsClient.getDatabase(checkDB(dbName));
      if (hiveDB != null) {
        db = new HCatDatabase(hiveDB);
      }
    } catch (NoSuchObjectException exp) {
      throw new ObjectNotFoundException(
        "NoSuchObjectException while fetching database", exp);
    } catch (MetaException exp) {
      throw new HCatException("MetaException while fetching database",
        exp);
    } catch (TException exp) {
      throw new ConnectionFailureException(
        "TException while fetching database", exp);
    }
    return db;
  }

  @Override
  public void createDatabase(HCatCreateDBDesc dbInfo) throws HCatException {
    try {
      hmsClient.createDatabase(dbInfo.toHiveDb());
    } catch (AlreadyExistsException exp) {
      if (!dbInfo.getIfNotExists()) {
        throw new HCatException(
          "AlreadyExistsException while creating database", exp);
      }
    } catch (InvalidObjectException exp) {
      throw new HCatException(
        "InvalidObjectException while creating database", exp);
    } catch (MetaException exp) {
      throw new HCatException("MetaException while creating database",
        exp);
    } catch (TException exp) {
      throw new ConnectionFailureException(
        "TException while creating database", exp);
    }
  }

  @Override
  public void dropDatabase(String dbName, boolean ifExists, DropDBMode mode)
    throws HCatException {
    boolean isCascade = mode.toString().equalsIgnoreCase("cascade");
    try {
      hmsClient.dropDatabase(checkDB(dbName), true, ifExists, isCascade);
    } catch (NoSuchObjectException e) {
      if (!ifExists) {
        throw new ObjectNotFoundException(
          "NoSuchObjectException while dropping db.", e);
      }
    } catch (InvalidOperationException e) {
      throw new HCatException(
        "InvalidOperationException while dropping db.", e);
    } catch (MetaException e) {
      throw new HCatException("MetaException while dropping db.", e);
    } catch (TException e) {
      throw new ConnectionFailureException("TException while dropping db.",
        e);
    }
  }

  @Override
  public List<String> listTableNamesByPattern(String dbName,
                        String tablePattern) throws HCatException {
    List<String> tableNames = null;
    try {
      tableNames = hmsClient.getTables(checkDB(dbName), tablePattern);
    } catch (MetaException e) {
      throw new HCatException("MetaException while fetching table names. " + e.getMessage(), e);
    } catch (UnknownDBException e) {
      throw new HCatException("UnknownDB " + dbName + " while fetching table names.", e);
    } catch (TException e) {
      throw new HCatException("Transport exception while fetching table names. "
          + e.getMessage(), e);
    }
    return tableNames;
  }

  @Override
  public HCatTable getTable(String dbName, String tableName)
    throws HCatException {
    HCatTable table = null;
    try {
      Table hiveTable = hmsClient.getTable(checkDB(dbName), tableName);
      if (hiveTable != null) {
        table = new HCatTable(hiveTable);
      }
    } catch (MetaException e) {
      throw new HCatException("MetaException while fetching table.", e);
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
        "NoSuchObjectException while fetching table.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
          "TException while fetching table.", e);
    }
    return table;
  }

  @Override
  public void createTable(HCatCreateTableDesc createTableDesc)
    throws HCatException {
    try {
      hmsClient.createTable(createTableDesc.getHCatTable().toHiveTable());
    } catch (AlreadyExistsException e) {
      if (!createTableDesc.getIfNotExists()) {
        throw new HCatException(
          "AlreadyExistsException while creating table.", e);
      }
    } catch (InvalidObjectException e) {
      throw new HCatException(
        "InvalidObjectException while creating table.", e);
    } catch (MetaException e) {
      throw new HCatException("MetaException while creating table.", e);
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
        "NoSuchObjectException while creating table.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
        "TException while creating table.", e);
    } catch (IOException e) {
      throw new HCatException("IOException while creating hive conf.", e);
    }

  }

  @Override
  public void updateTableSchema(String dbName, String tableName, List<HCatFieldSchema> columnSchema)
    throws HCatException {
    try {
      Table table = hmsClient.getTable(dbName, tableName);
      table.getSd().setCols(HCatSchemaUtils.getFieldSchemas(columnSchema));
      hmsClient.alter_table(dbName, tableName, table);
    }
    catch (InvalidOperationException e) {
      throw new HCatException("InvalidOperationException while updating table schema.", e);
    }
    catch (MetaException e) {
      throw new HCatException("MetaException while updating table schema.", e);
    }
    catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
          "NoSuchObjectException while updating table schema.", e);
    }
    catch (TException e) {
      throw new ConnectionFailureException(
          "TException while updating table schema.", e);
    }
  }

  @Override
  public void updateTableSchema(String dbName, String tableName, HCatTable newTableDefinition) throws HCatException {
    try {
      hmsClient.alter_table(dbName, tableName, newTableDefinition.toHiveTable());
    }
    catch (InvalidOperationException e) {
      throw new HCatException("InvalidOperationException while updating table schema.", e);
    }
    catch (MetaException e) {
      throw new HCatException("MetaException while updating table schema.", e);
    }
    catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
          "NoSuchObjectException while updating table schema.", e);
    }
    catch (TException e) {
      throw new ConnectionFailureException(
          "TException while updating table schema.", e);
    }
  }

  @Override
  public void createTableLike(String dbName, String existingTblName,
                String newTableName, boolean ifNotExists, boolean isExternal,
                String location) throws HCatException {

    Table hiveTable = getHiveTableLike(checkDB(dbName), existingTblName,
      newTableName, ifNotExists, location);
    if (hiveTable != null) {
      try {
        hmsClient.createTable(hiveTable);
      } catch (AlreadyExistsException e) {
        if (!ifNotExists) {
          throw new HCatException(
            "A table already exists with the name "
              + newTableName, e);
        }
      } catch (InvalidObjectException e) {
        throw new HCatException(
          "InvalidObjectException in create table like command.",
          e);
      } catch (MetaException e) {
        throw new HCatException(
          "MetaException in create table like command.", e);
      } catch (NoSuchObjectException e) {
        throw new ObjectNotFoundException(
          "NoSuchObjectException in create table like command.",
          e);
      } catch (TException e) {
        throw new ConnectionFailureException(
          "TException in create table like command.", e);
      }
    }
  }

  @Override
  public void dropTable(String dbName, String tableName, boolean ifExists)
    throws HCatException {
    try {
      hmsClient.dropTable(checkDB(dbName), tableName, true, ifExists);
    } catch (NoSuchObjectException e) {
      if (!ifExists) {
        throw new ObjectNotFoundException(
          "NoSuchObjectException while dropping table.", e);
      }
    } catch (MetaException e) {
      throw new HCatException("MetaException while dropping table.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
        "TException while dropping table.", e);
    }
  }

  @Override
  public void renameTable(String dbName, String oldName, String newName)
    throws HCatException {
    Table tbl;
    try {
      Table oldtbl = hmsClient.getTable(checkDB(dbName), oldName);
      if (oldtbl != null) {
        // TODO : Should be moved out.
        if (oldtbl
          .getParameters()
          .get(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE) != null) {
          throw new HCatException(
            "Cannot use rename command on a non-native table");
        }
        tbl = new Table(oldtbl);
        tbl.setTableName(newName);
        hmsClient.alter_table(checkDB(dbName), oldName, tbl);
      }
    } catch (MetaException e) {
      throw new HCatException("MetaException while renaming table", e);
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
        "NoSuchObjectException while renaming table", e);
    } catch (InvalidOperationException e) {
      throw new HCatException(
        "InvalidOperationException while renaming table", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
          "TException while renaming table", e);
    }
  }

  @Override
  public List<HCatPartition> getPartitions(String dbName, String tblName)
    throws HCatException {
    List<HCatPartition> hcatPtns = new ArrayList<HCatPartition>();
    try {
      HCatTable hcatTable = getTable(dbName, tblName);
      List<Partition> hivePtns = hmsClient.listPartitions(
        checkDB(dbName), tblName, (short) -1);
      for (Partition ptn : hivePtns) {
        hcatPtns.add(new HCatPartition(hcatTable, ptn));
      }
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
        "NoSuchObjectException while retrieving partition.", e);
    } catch (MetaException e) {
      throw new HCatException(
        "MetaException while retrieving partition.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
        "TException while retrieving partition.", e);
    }
    return hcatPtns;
  }

  @Override
  public List<HCatPartition> getPartitions(String dbName, String tblName, Map<String, String> partitionSpec) throws HCatException {
    return listPartitionsByFilter(dbName, tblName, getFilterString(partitionSpec));
  }

  @Override
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public HCatPartitionSpec getPartitionSpecs(String dbName, String tableName, int maxPartitions) throws HCatException {
    try {
      return new HCatPartitionSpec(getTable(dbName, tableName),
                                   hmsClient.listPartitionSpecs(dbName, tableName, maxPartitions));
    }
    catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
          "NoSuchObjectException while retrieving partition.", e);
    } catch (MetaException e) {
      throw new HCatException(
          "MetaException while retrieving partition.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
          "TException while retrieving partition.", e);
    }
  }

  @Override
  public HCatPartitionSpec getPartitionSpecs(String dbName, String tableName, Map<String, String> partitionSelector, int maxPartitions) throws HCatException {
    return listPartitionSpecsByFilter(dbName, tableName, getFilterString(partitionSelector), maxPartitions);
  }

  private static String getFilterString(Map<String, String> partitionSpec) {
    final String AND = " AND ";

    StringBuilder filter = new StringBuilder();
    for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
      filter.append(entry.getKey()).append("=").append("\"").append(entry.getValue()).append("\"").append(AND);
    }

    int length = filter.toString().length();
    if (length > 0)
      filter.delete(length - AND.length(), length);

    return filter.toString();
  }

  @Override
  public HCatPartition getPartition(String dbName, String tableName,
                    Map<String, String> partitionSpec) throws HCatException {
    HCatPartition partition = null;
    try {
      HCatTable hcatTable = getTable(dbName, tableName);
      List<HCatFieldSchema> partitionColumns = hcatTable.getPartCols();
      if (partitionColumns.size() != partitionSpec.size()) {
        throw new HCatException("Partition-spec doesn't have the right number of partition keys.");
      }

      ArrayList<String> ptnValues = new ArrayList<String>();
      for (HCatFieldSchema partitionColumn : partitionColumns) {
        String partKey = partitionColumn.getName();
        if (partitionSpec.containsKey(partKey)) {
          ptnValues.add(partitionSpec.get(partKey)); // Partition-keys added in order.
        }
        else {
          throw new HCatException("Invalid partition-key specified: " + partKey);
        }
      }
      Partition hivePartition = hmsClient.getPartition(checkDB(dbName),
        tableName, ptnValues);
      if (hivePartition != null) {
        partition = new HCatPartition(hcatTable, hivePartition);
      }
    } catch (MetaException e) {
      throw new HCatException(
        "MetaException while retrieving partition.", e);
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
        "NoSuchObjectException while retrieving partition.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
          "TException while retrieving partition.", e);
    }
    return partition;
  }

  @Override
  public void addPartition(HCatAddPartitionDesc partInfo)
    throws HCatException {
    Table tbl = null;
    try {
      tbl = hmsClient.getTable(partInfo.getDatabaseName(),
          partInfo.getTableName());
      // TODO: Should be moved out.
      if (tbl.getPartitionKeysSize() == 0) {
        throw new HCatException("The table " + partInfo.getTableName()
          + " is not partitioned.");
      }

      HCatTable hcatTable = new HCatTable(tbl);

      HCatPartition hcatPartition = partInfo.getHCatPartition();

      // TODO: Remove in Hive 0.16.
      // This is only required to support the deprecated methods in HCatAddPartitionDesc.Builder.
      if (hcatPartition == null) {
        hcatPartition = partInfo.getHCatPartition(hcatTable);
      }

      hmsClient.add_partition(hcatPartition.toHivePartition());
    } catch (InvalidObjectException e) {
      throw new HCatException(
        "InvalidObjectException while adding partition.", e);
    } catch (AlreadyExistsException e) {
      throw new HCatException(
        "AlreadyExistsException while adding partition.", e);
    } catch (MetaException e) {
      throw new HCatException("MetaException while adding partition.", e);
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException("The table " + partInfo.getTableName()
        + " is could not be found.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
          "TException while adding partition.", e);
    }
  }

  /**
   * Helper class to help build ExprDesc tree to represent the partitions to be dropped.
   * Note: At present, the ExpressionBuilder only constructs partition predicates where
   * partition-keys equal specific values, and logical-AND expressions. E.g.
   *  ( dt = '20150310' AND region = 'US' )
   * This only supports the partition-specs specified by the Map argument of:
   * {@link org.apache.hive.hcatalog.api.HCatClient#dropPartitions(String, String, Map, boolean)}
   */
  private static class ExpressionBuilder {

    private Map<String, PrimitiveTypeInfo> partColumnTypesMap = Maps.newHashMap();
    private Map<String, String> partSpecs;

    public ExpressionBuilder(Table table, Map<String, String> partSpecs) {
      this.partSpecs = partSpecs;
      for (FieldSchema partField : table.getPartitionKeys()) {
        partColumnTypesMap.put(partField.getName().toLowerCase(),
            TypeInfoFactory.getPrimitiveTypeInfo(partField.getType()));
      }
    }

    private PrimitiveTypeInfo getTypeFor(String partColumn) {
      return partColumnTypesMap.get(partColumn.toLowerCase());
    }

    private Object getTypeAppropriateValueFor(PrimitiveTypeInfo type, String value) {
      ObjectInspectorConverters.Converter converter = ObjectInspectorConverters.getConverter(
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoFactory.stringTypeInfo),
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(type));

      return converter.convert(value);
    }

    public ExprNodeGenericFuncDesc equalityPredicate(String partColumn, String value) throws SemanticException {

      PrimitiveTypeInfo partColumnType = getTypeFor(partColumn);
      ExprNodeColumnDesc partColumnExpr = new ExprNodeColumnDesc(partColumnType, partColumn, null, true);
      ExprNodeConstantDesc valueExpr = new ExprNodeConstantDesc(partColumnType,
          getTypeAppropriateValueFor(partColumnType, value));

      return binaryPredicate("=", partColumnExpr, valueExpr);
    }

    public ExprNodeGenericFuncDesc binaryPredicate(String function, ExprNodeDesc lhs, ExprNodeDesc rhs) throws SemanticException {
      return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
          FunctionRegistry.getFunctionInfo(function).getGenericUDF(),
          Lists.newArrayList(lhs, rhs));
    }

    public ExprNodeGenericFuncDesc build() throws SemanticException {
      ExprNodeGenericFuncDesc resultExpr = null;

      for (Map.Entry<String,String> partSpec : partSpecs.entrySet()) {
        String column = partSpec.getKey();
        String value  = partSpec.getValue();
        ExprNodeGenericFuncDesc partExpr = equalityPredicate(column, value);

        resultExpr = (resultExpr == null? partExpr : binaryPredicate("and", resultExpr, partExpr));
      }

      return resultExpr;
    }
  } // class ExpressionBuilder;

  private static boolean isExternal(Table table) {
    return table.getParameters() != null
        && "TRUE".equalsIgnoreCase(table.getParameters().get("EXTERNAL"));
  }

  private void dropPartitionsUsingExpressions(Table table, Map<String, String> partitionSpec,
                                              boolean ifExists, boolean deleteData)
      throws SemanticException, TException {
    LOG.info("HCatClient: Dropping partitions using partition-predicate Expressions.");
    ExprNodeGenericFuncDesc partitionExpression = new ExpressionBuilder(table, partitionSpec).build();
    ObjectPair<Integer, byte[]> serializedPartitionExpression = new ObjectPair<>(partitionSpec.size(),
            SerializationUtilities.serializeExpressionToKryo(partitionExpression));
    hmsClient.dropPartitions(table.getDbName(), table.getTableName(), Arrays.asList(serializedPartitionExpression),
        deleteData && !isExternal(table),  // Delete data?
        ifExists,                          // Fail if table doesn't exist?
        false);                            // Need results back?
  }

  private void dropPartitionsIteratively(String dbName, String tableName,
                                         Map<String, String> partitionSpec, boolean ifExists, boolean deleteData)
      throws HCatException, TException {
    LOG.info("HCatClient: Dropping partitions iteratively.");
    List<Partition> partitions = hmsClient.listPartitionsByFilter(dbName, tableName,
        getFilterString(partitionSpec), (short) -1);
    for (Partition partition : partitions) {
      dropPartition(partition, ifExists, deleteData);
    }
  }

  @Override
  public void dropPartitions(String dbName, String tableName,
                 Map<String, String> partitionSpec, boolean ifExists, boolean deleteData)
    throws HCatException {
    LOG.info("HCatClient dropPartitions(db=" + dbName + ",table=" + tableName + ", partitionSpec: ["+ partitionSpec + "]).");
    try {
      dbName = checkDB(dbName);
      Table table = hmsClient.getTable(dbName, tableName);

      if (hiveConfig.getBoolVar(HiveConf.ConfVars.METASTORE_CLIENT_DROP_PARTITIONS_WITH_EXPRESSIONS)) {
        try {
          dropPartitionsUsingExpressions(table, partitionSpec, ifExists, deleteData);
        }
        catch (SemanticException parseFailure) {
          LOG.warn("Could not push down partition-specification to back-end, for dropPartitions(). Resorting to iteration.",
              parseFailure);
          dropPartitionsIteratively(dbName, tableName, partitionSpec, ifExists, deleteData);
        }
      }
      else {
        // Not using expressions.
        dropPartitionsIteratively(dbName, tableName, partitionSpec, ifExists, deleteData);
      }
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
          "NoSuchObjectException while dropping partition. " +
              "Either db(" + dbName + ") or table(" + tableName + ") missing.", e);
    } catch (MetaException e) {
      throw new HCatException("MetaException while dropping partition.",
        e);
    } catch (TException e) {
      throw new ConnectionFailureException(
        "TException while dropping partition.", e);
    }
  }

  @Override
  public void dropPartitions(String dbName, String tableName,
                             Map<String, String> partitionSpec, boolean ifExists) throws HCatException {
    dropPartitions(dbName, tableName, partitionSpec, ifExists, true);
  }

  private void dropPartition(Partition partition, boolean ifExists, boolean deleteData)
    throws HCatException, MetaException, TException {
    try {
      hmsClient.dropPartition(partition.getDbName(), partition.getTableName(), partition.getValues(), deleteData);
    } catch (NoSuchObjectException e) {
      if (!ifExists) {
        throw new ObjectNotFoundException(
            "NoSuchObjectException while dropping partition: " + partition.getValues(), e);
      }
    }
  }

  @Override
  public List<HCatPartition> listPartitionsByFilter(String dbName,
                            String tblName, String filter) throws HCatException {
    List<HCatPartition> hcatPtns = new ArrayList<HCatPartition>();
    try {
      HCatTable table = getTable(dbName, tblName);
      List<Partition> hivePtns = hmsClient.listPartitionsByFilter(
          table.getDbName(), table.getTableName(), filter, (short) -1);
      for (Partition ptn : hivePtns) {
        hcatPtns.add(new HCatPartition(table, ptn));
      }
    } catch (MetaException e) {
      throw new HCatException("MetaException while fetching partitions.",
        e);
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
        "NoSuchObjectException while fetching partitions.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
        "TException while fetching partitions.", e);
    }
    return hcatPtns;
  }

  @Override
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public HCatPartitionSpec listPartitionSpecsByFilter(String dbName, String tblName, String filter, int maxPartitions)
      throws HCatException {
    try {
      return new HCatPartitionSpec(getTable(dbName, tblName),
                                   hmsClient.listPartitionSpecsByFilter(dbName, tblName, filter, maxPartitions));
    }
    catch(MetaException e) {
      throw new HCatException("MetaException while fetching partitions.", e);
    }
    catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
          "NoSuchObjectException while fetching partitions.", e);
    }
    catch (TException e) {
      throw new ConnectionFailureException(
        "TException while fetching partitions.", e);
    }
  }

  @Override
  public void markPartitionForEvent(String dbName, String tblName,
                    Map<String, String> partKVs, PartitionEventType eventType)
    throws HCatException {
    try {
      hmsClient.markPartitionForEvent(checkDB(dbName), tblName, partKVs,
        eventType);
    } catch (MetaException e) {
      throw new HCatException(
        "MetaException while marking partition for event.", e);
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
        "NoSuchObjectException while marking partition for event.",
        e);
    } catch (UnknownTableException e) {
      throw new HCatException(
        "UnknownTableException while marking partition for event.",
        e);
    } catch (UnknownDBException e) {
      throw new HCatException(
        "UnknownDBException while marking partition for event.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
        "TException while marking partition for event.", e);
    }
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName,
                       Map<String, String> partKVs, PartitionEventType eventType)
    throws HCatException {
    boolean isMarked = false;
    try {
      isMarked = hmsClient.isPartitionMarkedForEvent(checkDB(dbName),
        tblName, partKVs, eventType);
    } catch (MetaException e) {
      throw new HCatException(
        "MetaException while checking partition for event.", e);
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException(
        "NoSuchObjectException while checking partition for event.",
        e);
    } catch (UnknownTableException e) {
      throw new HCatException(
        "UnknownTableException while checking partition for event.",
        e);
    } catch (UnknownDBException e) {
      throw new HCatException(
        "UnknownDBException while checking partition for event.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
        "TException while checking partition for event.", e);
    }
    return isMarked;
  }

  @Override
  public String getDelegationToken(String owner,
                   String renewerKerberosPrincipalName) throws HCatException {
    String token = null;
    try {
      token = hmsClient.getDelegationToken(owner,
          renewerKerberosPrincipalName);
    } catch (MetaException e) {
      throw new HCatException(
        "MetaException while getting delegation token.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
        "TException while getting delegation token.", e);
    }

    return token;
  }

  @Override
  public long renewDelegationToken(String tokenStrForm) throws HCatException {
    long time = 0;
    try {
      time = hmsClient.renewDelegationToken(tokenStrForm);
    } catch (MetaException e) {
      throw new HCatException(
        "MetaException while renewing delegation token.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
        "TException while renewing delegation token.", e);
    }

    return time;
  }

  @Override
  public void cancelDelegationToken(String tokenStrForm)
    throws HCatException {
    try {
      hmsClient.cancelDelegationToken(tokenStrForm);
    } catch (MetaException e) {
      throw new HCatException(
        "MetaException while canceling delegation token.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
        "TException while canceling delegation token.", e);
    }
  }

  /*
   * @param conf /* @throws HCatException,ConnectionFailureException
   *
   * @see
   * org.apache.hive.hcatalog.api.HCatClient#initialize(org.apache.hadoop.conf.
   * Configuration)
   */
  @Override
  void initialize(Configuration conf) throws HCatException {
    this.config = conf;
    try {
      hiveConfig = HCatUtil.getHiveConf(config);
      hmsClient = HCatUtil.getHiveMetastoreClient(hiveConfig);
    } catch (MetaException exp) {
      throw new HCatException("MetaException while creating HMS client",
        exp);
    } catch (IOException exp) {
      throw new HCatException("IOException while creating HMS client",
        exp);
    }

  }

  @Override
  public String getConfVal(String key, String defaultVal) {
    return hiveConfig.get(key,defaultVal);
  }

  private Table getHiveTableLike(String dbName, String existingTblName,
                   String newTableName, boolean isExternal, String location)
    throws HCatException {
    Table oldtbl = null;
    Table newTable = null;
    try {
      oldtbl = hmsClient.getTable(checkDB(dbName), existingTblName);
    } catch (MetaException e1) {
      throw new HCatException(
        "MetaException while retrieving existing table.", e1);
    } catch (NoSuchObjectException e1) {
      throw new ObjectNotFoundException(
        "NoSuchObjectException while retrieving existing table.",
        e1);
    } catch (TException e1) {
      throw new ConnectionFailureException(
          "TException while retrieving existing table.", e1);
    }
    if (oldtbl != null) {
      newTable = new Table();
      newTable.setTableName(newTableName);
      newTable.setDbName(dbName);
      StorageDescriptor sd = new StorageDescriptor(oldtbl.getSd());
      newTable.setSd(sd);
      newTable.setParameters(oldtbl.getParameters());
      if (location == null) {
        newTable.getSd().setLocation(oldtbl.getSd().getLocation());
      } else {
        newTable.getSd().setLocation(location);
      }
      if (isExternal) {
        newTable.putToParameters("EXTERNAL", "TRUE");
        newTable.setTableType(TableType.EXTERNAL_TABLE.toString());
      } else {
        newTable.getParameters().remove("EXTERNAL");
      }
      // set create time
      newTable.setCreateTime((int) (System.currentTimeMillis() / 1000));
      newTable.setLastAccessTimeIsSet(false);
    }
    return newTable;
  }

  /*
   * @throws HCatException
   *
   * @see org.apache.hive.hcatalog.api.HCatClient#closeClient()
   */
  @Override
  public void close() throws HCatException {
    hmsClient.close();
  }

  private String checkDB(String name) {
    if (StringUtils.isEmpty(name)) {
      return Warehouse.DEFAULT_DATABASE_NAME;
    } else {
      return name;
    }
  }

  /*
   * @param partInfoList
   *  @return The size of the list of partitions.
   * @throws HCatException,ConnectionFailureException
   * @see org.apache.hive.hcatalog.api.HCatClient#addPartitions(java.util.List)
   */
  @Override
  public int addPartitions(List<HCatAddPartitionDesc> partInfoList)
    throws HCatException {
    int numPartitions = -1;
    if ((partInfoList == null) || (partInfoList.size() == 0)) {
      throw new HCatException("The partition list is null or empty.");
    }

    Table tbl = null;
    try {
      tbl = hmsClient.getTable(partInfoList.get(0).getDatabaseName(),
        partInfoList.get(0).getTableName());
      HCatTable hcatTable = new HCatTable(tbl);
      ArrayList<Partition> ptnList = new ArrayList<Partition>();
      for (HCatAddPartitionDesc desc : partInfoList) {
        HCatPartition hCatPartition = desc.getHCatPartition();

        // TODO: Remove in Hive 0.16.
        // This is required only to support the deprecated HCatAddPartitionDesc.Builder interfaces.
        if (hCatPartition == null) {
          hCatPartition = desc.getHCatPartition(hcatTable);
        }

        ptnList.add(hCatPartition.toHivePartition());
      }
      numPartitions = hmsClient.add_partitions(ptnList);
    } catch (InvalidObjectException e) {
      throw new HCatException(
        "InvalidObjectException while adding partition.", e);
    } catch (AlreadyExistsException e) {
      throw new HCatException(
        "AlreadyExistsException while adding partition.", e);
    } catch (MetaException e) {
      throw new HCatException("MetaException while adding partition.", e);
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException("The table "
        + partInfoList.get(0).getTableName()
        + " is could not be found.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
          "TException while adding partition.", e);
    }
    return numPartitions;
  }

  @Override
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public int addPartitionSpec(HCatPartitionSpec partitionSpec) throws HCatException {

    try {
      return hmsClient.add_partitions_pspec(partitionSpec.toPartitionSpecProxy());
    } catch (InvalidObjectException e) {
      throw new HCatException(
          "InvalidObjectException while adding partition.", e);
    } catch (AlreadyExistsException e) {
      throw new HCatException(
          "AlreadyExistsException while adding partition.", e);
    } catch (MetaException e) {
      throw new HCatException("MetaException while adding partition.", e);
    } catch (NoSuchObjectException e) {
      throw new ObjectNotFoundException("The table "
          + "could not be found.", e);
    } catch (TException e) {
      throw new ConnectionFailureException(
          "TException while adding partition.", e);
    }
  }

  @Override
  public String getMessageBusTopicName(String dbName, String tableName) throws HCatException {
    try {
      return hmsClient.getTable(dbName, tableName).getParameters().get(
          HCatConstants.HCAT_MSGBUS_TOPIC_NAME);
    }
    catch (MetaException e) {
      throw new HCatException("MetaException while retrieving JMS Topic name.", e);
    } catch (NoSuchObjectException e) {
      throw new HCatException("Could not find DB:" + dbName + " or Table:" + tableName, e);
    } catch (TException e) {
      throw new ConnectionFailureException(
          "TException while retrieving JMS Topic name.", e);
    }
  }

  @Override
  public Iterator<ReplicationTask> getReplicationTasks(
      long lastEventId, int maxEvents, String dbName, String tableName) throws HCatException {
    return new HCatReplicationTaskIterator(this,lastEventId,maxEvents,dbName,tableName);
  }

  @Override
  public List<HCatNotificationEvent> getNextNotification(long lastEventId, int maxEvents,
                                                         IMetaStoreClient.NotificationFilter filter)
      throws HCatException {
    try {
      NotificationEventResponse rsp = hmsClient.getNextNotification(lastEventId, maxEvents, filter);
      if (rsp != null && rsp.getEvents() != null) {
        return Lists.transform(rsp.getEvents(), new Function<NotificationEvent, HCatNotificationEvent>() {
          @Override
          public HCatNotificationEvent apply(@Nullable NotificationEvent notificationEvent) {
            return new HCatNotificationEvent(notificationEvent);
          }
        });
      } else {
        return new ArrayList<HCatNotificationEvent>();
      }
    } catch (TException e) {
      throw new ConnectionFailureException("TException while getting notifications", e);
    }
  }

  @Override
  public long getCurrentNotificationEventId() throws HCatException {
    try {
      CurrentNotificationEventId id = hmsClient.getCurrentNotificationEventId();
      return id.getEventId();
    } catch (TException e) {
      throw new ConnectionFailureException("TException while getting current notification event " +
          "id " , e);
    }
  }

  @Override
  public String serializeTable(HCatTable hcatTable) throws HCatException {
    return MetadataSerializer.get().serializeTable(hcatTable);
  }

  @Override
  public HCatTable deserializeTable(String hcatTableStringRep) throws HCatException {
    return MetadataSerializer.get().deserializeTable(hcatTableStringRep);
  }

  @Override
  public String serializePartition(HCatPartition hcatPartition) throws HCatException {
    return MetadataSerializer.get().serializePartition(hcatPartition);
  }

  @Override
  public List<String> serializePartitions(List<HCatPartition> hcatPartitions) throws HCatException {
    List<String> partStrings = new ArrayList<String>(hcatPartitions.size());
    MetadataSerializer serializer = MetadataSerializer.get();

    for (HCatPartition partition : hcatPartitions) {
      partStrings.add(serializer.serializePartition(partition));
    }

    return partStrings;
  }

  @Override
  public HCatPartition deserializePartition(String hcatPartitionStringRep) throws HCatException {
    HCatPartition hcatPartition = MetadataSerializer.get().deserializePartition(hcatPartitionStringRep);
    hcatPartition.hcatTable(getTable(hcatPartition.getDatabaseName(), hcatPartition.getTableName()));
    return hcatPartition;
  }

  @Override
  public List<HCatPartition> deserializePartitions(List<String> hcatPartitionStringReps) throws HCatException {
    List<HCatPartition> partitions = new ArrayList<HCatPartition>(hcatPartitionStringReps.size());
    MetadataSerializer deserializer = MetadataSerializer.get();
    HCatTable table = null;
    for (String partString : hcatPartitionStringReps) {
      HCatPartition partition;
      if (table == null) {
        partition = deserializePartition(partString);
        table = partition.hcatTable();
      }
      else {
        partition = deserializer.deserializePartition(partString);
        if (partition.getDatabaseName().equals(table.getDbName())
            && partition.getTableName().equals(table.getTableName())) {
          partition.hcatTable(table);
        }
        else {
          throw new HCatException("All partitions are not of the same table: "
              + table.getDbName() + "." + table.getTableName());
        }
      }
      partitions.add(partition);
    }
    return partitions;
  }

  @Override
  public List<String> serializePartitionSpec(HCatPartitionSpec partitionSpec) throws HCatException {
    return MetadataSerializer.get().serializePartitionSpec(partitionSpec);
  }

  @Override
  public HCatPartitionSpec deserializePartitionSpec(List<String> hcatPartitionSpecStrings) throws HCatException {
    HCatPartitionSpec hcatPartitionSpec = MetadataSerializer.get()
        .deserializePartitionSpec(hcatPartitionSpecStrings);
    hcatPartitionSpec
        .hcatTable(getTable(hcatPartitionSpec.getDbName(), hcatPartitionSpec.getTableName()));
    return hcatPartitionSpec;
  }
}
