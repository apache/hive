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

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.HBaseSerDe.ColumnMapping;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TBase;
import org.apache.zookeeper.ZooKeeper;

import com.facebook.fb303.FacebookBase;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * HBaseStorageHandler provides a HiveStorageHandler implementation for
 * HBase.
 */
public class HBaseStorageHandler extends DefaultStorageHandler
  implements HiveMetaHook, HiveStoragePredicateHandler {

  final static public String DEFAULT_PREFIX = "default.";

  //Check if the configure job properties is called from input or output for setting asymmetric properties
  String CONFIGURE_JOB_PROPERTIES = "INPUT";

  private Configuration jobConf;
  private Configuration hbaseConf;
  private HBaseAdmin admin;

  private HBaseAdmin getHBaseAdmin() throws MetaException {
    try {
      if (admin == null) {
        admin = new HBaseAdmin(hbaseConf);
      }
      return admin;
    } catch (IOException ioe) {
      throw new MetaException(StringUtils.stringifyException(ioe));
    }
  }

  private String getHBaseTableName(Table tbl) {
    // Give preference to TBLPROPERTIES over SERDEPROPERTIES
    // (really we should only use TBLPROPERTIES, so this is just
    // for backwards compatibility with the original specs).
    String tableName = tbl.getParameters().get(HBaseSerDe.HBASE_TABLE_NAME);
    if (tableName == null) {
      //convert to lower case in case we are getting from serde
      tableName = tbl.getSd().getSerdeInfo().getParameters().get(
        HBaseSerDe.HBASE_TABLE_NAME);
      //standardize to lower case
      if (tableName != null) {
        tableName = tableName.toLowerCase();
      }
    }
    if (tableName == null) {
      tableName = (tbl.getDbName() + "." + tbl.getTableName()).toLowerCase();
      if (tableName.startsWith(DEFAULT_PREFIX)) {
        tableName = tableName.substring(DEFAULT_PREFIX.length());
      }
    }
    return tableName;
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public void commitDropTable(
    Table tbl, boolean deleteData) throws MetaException {

    try {
      String tableName = getHBaseTableName(tbl);
      boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
      if (deleteData && !isExternal) {
        if (getHBaseAdmin().isTableEnabled(tableName)) {
          getHBaseAdmin().disableTable(tableName);
        }
        getHBaseAdmin().deleteTable(tableName);
      }
    } catch (IOException ie) {
      throw new MetaException(StringUtils.stringifyException(ie));
    }
  }

  @Override
  public void preCreateTable(Table tbl) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

    // We'd like to move this to HiveMetaStore for any non-native table, but
    // first we need to support storing NULL for location on a table
    if (tbl.getSd().getLocation() != null) {
      throw new MetaException("LOCATION may not be specified for HBase.");
    }

    try {
      String tableName = getHBaseTableName(tbl);
      Map<String, String> serdeParam = tbl.getSd().getSerdeInfo().getParameters();
      String hbaseColumnsMapping = serdeParam.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);
      List<ColumnMapping> columnsMapping = null;

      columnsMapping = HBaseSerDe.parseColumnsMapping(hbaseColumnsMapping);

      HTableDescriptor tableDesc;

      if (!getHBaseAdmin().tableExists(tableName)) {
        // if it is not an external table then create one
        if (!isExternal) {
          // Create the column descriptors
          tableDesc = new HTableDescriptor(tableName);
          Set<String> uniqueColumnFamilies = new HashSet<String>();

          for (ColumnMapping colMap : columnsMapping) {
            if (!colMap.hbaseRowKey) {
              uniqueColumnFamilies.add(colMap.familyName);
            }
          }

          for (String columnFamily : uniqueColumnFamilies) {
            tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes(columnFamily)));
          }

          getHBaseAdmin().createTable(tableDesc);
        } else {
          // an external table
          throw new MetaException("HBase table " + tableName +
              " doesn't exist while the table is declared as an external table.");
        }

      } else {
        if (!isExternal) {
          throw new MetaException("Table " + tableName + " already exists"
            + " within HBase; use CREATE EXTERNAL TABLE instead to"
            + " register it in Hive.");
        }
        // make sure the schema mapping is right
        tableDesc = getHBaseAdmin().getTableDescriptor(Bytes.toBytes(tableName));

        for (int i = 0; i < columnsMapping.size(); i++) {
          ColumnMapping colMap = columnsMapping.get(i);

          if (colMap.hbaseRowKey) {
            continue;
          }

          if (!tableDesc.hasFamily(colMap.familyNameBytes)) {
            throw new MetaException("Column Family " + colMap.familyName
                + " is not defined in hbase table " + tableName);
          }
        }
      }

      // ensure the table is online
      new HTable(hbaseConf, tableDesc.getName());
    } catch (MasterNotRunningException mnre) {
      throw new MetaException(StringUtils.stringifyException(mnre));
    } catch (IOException ie) {
      throw new MetaException(StringUtils.stringifyException(ie));
    } catch (SerDeException se) {
      throw new MetaException(StringUtils.stringifyException(se));
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(table);
    String tableName = getHBaseTableName(table);
    try {
      if (!isExternal && getHBaseAdmin().tableExists(tableName)) {
        // we have created an HBase table, so we delete it to roll back;
        if (getHBaseAdmin().isTableEnabled(tableName)) {
          getHBaseAdmin().disableTable(tableName);
        }
        getHBaseAdmin().deleteTable(tableName);
      }
    } catch (IOException ie) {
      throw new MetaException(StringUtils.stringifyException(ie));
    }
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public Configuration getConf() {
    return hbaseConf;
  }

  public Configuration getJobConf() {
    return jobConf;
  }

  @Override
  public void setConf(Configuration conf) {
    jobConf = conf;
    hbaseConf = HBaseConfiguration.create(conf);
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveHBaseTableInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return org.apache.hadoop.hive.hbase.HiveHBaseTableOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return HBaseSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public void configureInputJobProperties(
      TableDesc tableDesc,
      Map<String, String> jobProperties) {
    CONFIGURE_JOB_PROPERTIES = "INPUT";
    configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureOutputJobProperties(
      TableDesc tableDesc,
      Map<String, String> jobProperties) {
    CONFIGURE_JOB_PROPERTIES = "OUTPUT";
    configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureTableJobProperties(
    TableDesc tableDesc,
    Map<String, String> jobProperties) {

    Properties tableProperties = tableDesc.getProperties();

    jobProperties.put(
      HBaseSerDe.HBASE_COLUMNS_MAPPING,
      tableProperties.getProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING));
    jobProperties.put(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE,
      tableProperties.getProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE,"string"));

    String tableName =
      tableProperties.getProperty(HBaseSerDe.HBASE_TABLE_NAME);
    if (tableName == null) {
      tableName =
        tableProperties.getProperty(hive_metastoreConstants.META_TABLE_NAME);
        tableName = tableName.toLowerCase();
      if (tableName.startsWith(DEFAULT_PREFIX)) {
        tableName = tableName.substring(DEFAULT_PREFIX.length());
      }
    }
    jobProperties.put(HBaseSerDe.HBASE_TABLE_NAME, tableName);

    // do this for reconciling HBaseStorageHandler for use in HCatalog
    // check to see if this an input job or an outputjob
    if (CONFIGURE_JOB_PROPERTIES == "INPUT") {
      try {
        Configuration jobConf = getJobConf();
        addResources(jobConf, jobProperties);
        JobConf copyOfConf = new JobConf(jobConf);
        HBaseConfiguration.addHbaseResources(copyOfConf);
        //Getting hbase delegation token in getInputSplits does not work with PIG.
        if (jobConf instanceof JobConf) { //Should be the case
          addHBaseDelegationToken(copyOfConf);
          ((JobConf)jobConf).getCredentials().addAll(copyOfConf.getCredentials());
        }
      }//try
      catch (IOException e) {
        throw new IllegalStateException("Error while configuring input job properties", e);
      }
    }
    else {
      //find out a method of populating HCat specific info here
      Map<String, String> tableJobProperties = tableDesc.getJobProperties();
      Configuration jobConf = getJobConf();
      addResources(jobConf, jobProperties);
      JobConf copyOfConf = new JobConf(jobConf);
      HBaseConfiguration.addHbaseResources(copyOfConf);
      jobProperties.put(TableOutputFormat.OUTPUT_TABLE, tableName);
    }
  }

  /**
   * Utility method to add hbase-default.xml and hbase-site.xml properties to a new map
   * if they are not already present in the jobConf.
   * @param jobConf Job configuration
   * @param newJobProperties  Map to which new properties should be added
   */
  private void addResources(Configuration jobConf,
      Map<String, String> newJobProperties) {
    Configuration conf = new Configuration(false);
    HBaseConfiguration.addHbaseResources(conf);
    for (Entry<String, String> entry : conf) {
      if (jobConf.get(entry.getKey()) == null) {
        newJobProperties.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private void addHBaseDelegationToken(JobConf job) throws IOException {
    if (User.isHBaseSecurityEnabled(job)) {
      try {
        User.getCurrent().obtainAuthTokenForJob(job);
      } catch (InterruptedException e) {
        throw new IOException("Error while obtaining hbase delegation token", e);
      }
    }
  }

@Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    try {
      TableMapReduceUtil.addDependencyJars(jobConf);
      org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.addDependencyJars(jobConf,
          HBaseStorageHandler.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DecomposedPredicate decomposePredicate(
    JobConf jobConf,
    Deserializer deserializer,
    ExprNodeDesc predicate)
  {
    String columnNameProperty = jobConf.get(
      org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS);
    List<String> columnNames =
      Arrays.asList(columnNameProperty.split(","));

    HBaseSerDe hbaseSerde = (HBaseSerDe) deserializer;
    int keyColPos = hbaseSerde.getKeyColumnOffset();
    String keyColType = jobConf.get(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES).
        split(",")[keyColPos];
    IndexPredicateAnalyzer analyzer =
      HiveHBaseTableInputFormat.newIndexPredicateAnalyzer(columnNames.get(keyColPos), keyColType,
        hbaseSerde.getStorageFormatOfCol(keyColPos).get(0));
    List<IndexSearchCondition> searchConditions =
      new ArrayList<IndexSearchCondition>();
    ExprNodeDesc residualPredicate =
      analyzer.analyzePredicate(predicate, searchConditions);
    int scSize = searchConditions.size();
    if (scSize < 1 || 2 < scSize) {
      // Either there was nothing which could be pushed down (size = 0),
      // there were complex predicates which we don't support yet.
      // Currently supported are one of the form:
      // 1. key < 20                        (size = 1)
      // 2. key = 20                        (size = 1)
      // 3. key < 20 and key > 10           (size = 2)
      return null;
    }
    if (scSize == 2 &&
        (searchConditions.get(0).getComparisonOp()
        .equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual") ||
        searchConditions.get(1).getComparisonOp()
        .equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual"))) {
      // If one of the predicates is =, then any other predicate with it is illegal.
      return null;
    }

    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(
      searchConditions);
    decomposedPredicate.residualPredicate = residualPredicate;
    return decomposedPredicate;
  }
}
