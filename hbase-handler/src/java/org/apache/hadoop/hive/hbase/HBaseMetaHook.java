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

package org.apache.hadoop.hive.hbase;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * MetaHook for HBase. Updates the table data in HBase too. Not thread safe, and cleanup should
 * be used after usage.
 */
public class HBaseMetaHook implements HiveMetaHook, Closeable {
  private Configuration hbaseConf;
  private Admin admin;

  public HBaseMetaHook(Configuration hbaseConf) {
    this.hbaseConf = hbaseConf;
  }

  private Admin getHBaseAdmin() throws MetaException {
    try {
      if (admin == null) {
        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        admin = conn.getAdmin();
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
      tableName = tbl.getSd().getSerdeInfo().getParameters().get(HBaseSerDe.HBASE_TABLE_NAME);
      //standardize to lower case
      if (tableName != null) {
        tableName = tableName.toLowerCase();
      }
    }
    if (tableName == null) {
      tableName = (tbl.getDbName() + "." + tbl.getTableName()).toLowerCase();
      if (tableName.startsWith(HBaseStorageHandler.DEFAULT_PREFIX)) {
        tableName = tableName.substring(HBaseStorageHandler.DEFAULT_PREFIX.length());
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
  public void commitDropTable(Table tbl, boolean deleteData) throws MetaException {
    try {
      String tableName = getHBaseTableName(tbl);
      boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
      if (deleteData && !isExternal) {
        if (getHBaseAdmin().isTableEnabled(TableName.valueOf(tableName))) {
          getHBaseAdmin().disableTable(TableName.valueOf(tableName));
        }
        getHBaseAdmin().deleteTable(TableName.valueOf(tableName));
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

    org.apache.hadoop.hbase.client.Table htable = null;

    try {
      String tableName = getHBaseTableName(tbl);
      Map<String, String> serdeParam = tbl.getSd().getSerdeInfo().getParameters();
      String hbaseColumnsMapping = serdeParam.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);

      ColumnMappings columnMappings = HBaseSerDe.parseColumnsMapping(hbaseColumnsMapping);

      HTableDescriptor tableDesc;

      if (!getHBaseAdmin().tableExists(TableName.valueOf(tableName))) {
        // if it is not an external table then create one
        if (!isExternal) {
          // Create the column descriptors
          tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
          Set<String> uniqueColumnFamilies = new HashSet<String>();

          for (ColumnMappings.ColumnMapping colMap : columnMappings) {
            if (!colMap.hbaseRowKey && !colMap.hbaseTimestamp) {
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
          throw new MetaException("Table " + tableName + " already exists within HBase; "
              + "use CREATE EXTERNAL TABLE instead to register it in Hive.");
        }
        // make sure the schema mapping is right
        tableDesc = getHBaseAdmin().getTableDescriptor(TableName.valueOf(tableName));

        for (ColumnMappings.ColumnMapping colMap : columnMappings) {

          if (colMap.hbaseRowKey || colMap.hbaseTimestamp) {
            continue;
          }

          if (!tableDesc.hasFamily(colMap.familyNameBytes)) {
            throw new MetaException("Column Family " + colMap.familyName
                + " is not defined in hbase table " + tableName);
          }
        }
      }

      // ensure the table is online
      htable = getHBaseAdmin().getConnection().getTable(tableDesc.getTableName());
    } catch (Exception se) {
      throw new MetaException(StringUtils.stringifyException(se));
    } finally {
      if (htable != null) {
        IOUtils.closeQuietly(htable);
      }
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(table);
    String tableName = getHBaseTableName(table);
    try {
      if (!isExternal && getHBaseAdmin().tableExists(TableName.valueOf(tableName))) {
        // we have created an HBase table, so we delete it to roll back;
        if (getHBaseAdmin().isTableEnabled(TableName.valueOf(tableName))) {
          getHBaseAdmin().disableTable(TableName.valueOf(tableName));
        }
        getHBaseAdmin().deleteTable(TableName.valueOf(tableName));
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
  public void close() throws IOException {
    if (admin != null) {
      Connection connection = admin.getConnection();
      admin.close();
      admin = null;
      if (connection != null) {
        connection.close();
      }
    }
  }
}
