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
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * HBaseStorageHandler provides a HiveStorageHandler implementation for
 * HBase.
 */
public class HBaseStorageHandler
  implements HiveStorageHandler, HiveMetaHook {

  private HBaseConfiguration hbaseConf;
  private HBaseAdmin admin;
  
  private HBaseAdmin getHBaseAdmin() throws MetaException {
    try {
      if (admin == null) {
        admin = new HBaseAdmin(hbaseConf);
      }
      return admin;
    } catch (MasterNotRunningException mnre) {
      throw new MetaException(StringUtils.stringifyException(mnre));
    }
  }

  private String getHBaseTableName(Table tbl) {
    String tableName = tbl.getSd().getSerdeInfo().getParameters().get(
      HBaseSerDe.HBASE_TABLE_NAME);
    if (tableName == null) {
      tableName = tbl.getTableName();
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
      String tblName = getHBaseTableName(tbl);

      // Build the mapping schema
      Set<String> columnFamilies = new HashSet<String>();
      // Check the hbase columns and get all the families
      Map<String, String> serdeParam =
        tbl.getSd().getSerdeInfo().getParameters();
      String hbaseColumnStr = serdeParam.get(HBaseSerDe.HBASE_COL_MAPPING);
      if (hbaseColumnStr == null) {
        throw new MetaException("No hbase.columns.mapping defined in Serde.");
      }
      String [] hbaseColumns = hbaseColumnStr.split(",");
      for (String hbaseColumn : hbaseColumns) {
        int idx = hbaseColumn.indexOf(":");
        if (idx < 0) {
          throw new MetaException(
            hbaseColumn + " is not a qualified hbase column.");
        }
        columnFamilies.add(hbaseColumn.substring(0, idx));
      }
  
      // Check if the given hbase table exists
      HTableDescriptor tblDesc;
      
      if (!getHBaseAdmin().tableExists(tblName)) {
        // if it is not an external table then create one
        if (!isExternal) {
          // Create the all column descriptors
          tblDesc = new HTableDescriptor(tblName);
          for (String cf : columnFamilies) {
            tblDesc.addFamily(new HColumnDescriptor(cf + ":"));
          }
  
          getHBaseAdmin().createTable(tblDesc);
        } else {
          // an external table
          throw new MetaException("HBase table " + tblName + 
              " doesn't exist while the table is declared as an external table.");
        }
      
      } else {
        if (!isExternal) {
          throw new MetaException("Table " + tblName + " already exists"
            + " within HBase; use CREATE EXTERNAL TABLE instead to"
            + " register it in Hive.");
        }
        // make sure the schema mapping is right
        tblDesc = getHBaseAdmin().getTableDescriptor(Bytes.toBytes(tblName));
        for (String cf : columnFamilies) {
          if (!tblDesc.hasFamily(Bytes.toBytes(cf))) {
            throw new MetaException("Column Family " + cf
              + " is not defined in hbase table " + tblName);
          }
        }

      }
      // ensure the table is online
      new HTable(hbaseConf, tblDesc.getName());
    } catch (MasterNotRunningException mnre) {
      throw new MetaException(StringUtils.stringifyException(mnre));
    } catch (IOException ie) {
      throw new MetaException(StringUtils.stringifyException(ie));
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(table);
    String tableName = getHBaseTableName(table);
    try {
      if (!isExternal && getHBaseAdmin().tableExists(tableName)) {
        // we have create an hbase table, so we delete it to roll back;
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

  @Override
  public void setConf(Configuration conf) {
    hbaseConf = new HBaseConfiguration(conf);
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveHBaseTableInputFormat.class;
  }
  
  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveHBaseTableOutputFormat.class;
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
  public void configureTableJobProperties(
    TableDesc tableDesc,
    Map<String, String> jobProperties) {

    Properties tableProperties = tableDesc.getProperties();
    
    jobProperties.put(
      HBaseSerDe.HBASE_COL_MAPPING,
      tableProperties.getProperty(HBaseSerDe.HBASE_COL_MAPPING));

    String tableName =
      tableProperties.getProperty(HBaseSerDe.HBASE_TABLE_NAME);
    if (tableName == null) {
      tableName =
        tableProperties.getProperty(Constants.META_TABLE_NAME);
    }
    jobProperties.put(HBaseSerDe.HBASE_TABLE_NAME, tableName);
  }
}
