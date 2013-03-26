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

package org.apache.hcatalog.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.facebook.fb303.FacebookBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hcatalog.mapreduce.HCatInputStorageDriver;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.HCatOutputStorageDriver;
import org.apache.hcatalog.mapreduce.HCatTableInfo;
import org.apache.hcatalog.storagehandler.HCatStorageHandler;
import org.apache.thrift.TBase;
import org.apache.zookeeper.ZooKeeper;

/**
 * This class HBaseHCatStorageHandler provides functionality to create HBase
 * tables through HCatalog. The implementation is very similar to the
 * HiveHBaseStorageHandler, with more details to suit HCatalog.
 */
public class HBaseHCatStorageHandler extends HCatStorageHandler {

    final static public String DEFAULT_PREFIX = "default.";

    private Configuration      hbaseConf;

    private HBaseAdmin         admin;

    /*
     * @return subclass of HCatInputStorageDriver
     *
     * @see org.apache.hcatalog.storagehandler.HCatStorageHandler
     * #getInputStorageDriver()
     */
    @Override
    public Class<? extends HCatInputStorageDriver> getInputStorageDriver() {
        return HBaseInputStorageDriver.class;
    }

    /*
     * @return subclass of HCatOutputStorageDriver
     *
     * @see org.apache.hcatalog.storagehandler.HCatStorageHandler
     * #getOutputStorageDriver()
     */
    @Override
    public Class<? extends HCatOutputStorageDriver> getOutputStorageDriver() {
        return HBaseOutputStorageDriver.class;
    }

    /*
     * @return instance of HiveAuthorizationProvider
     *
     * @throws HiveException
     *
     * @see org.apache.hcatalog.storagehandler.HCatStorageHandler#
     * getAuthorizationProvider()
     */
    @Override
    public HiveAuthorizationProvider getAuthorizationProvider()
            throws HiveException {

        HBaseAuthorizationProvider hbaseAuth = new HBaseAuthorizationProvider();
        hbaseAuth.init(getConf());
        return hbaseAuth;
    }

    /*
     * @param table
     *
     * @throws MetaException
     *
     * @see org.apache.hcatalog.storagehandler.HCatStorageHandler
     * #commitCreateTable(org.apache.hadoop.hive.metastore.api.Table)
     */
    @Override
    public void commitCreateTable(Table table) throws MetaException {
    }

    /*
     * @param instance of table
     *
     * @param deleteData
     *
     * @throws MetaException
     *
     * @see org.apache.hcatalog.storagehandler.HCatStorageHandler
     * #commitDropTable(org.apache.hadoop.hive.metastore.api.Table, boolean)
     */
    @Override
    public void commitDropTable(Table tbl, boolean deleteData)
            throws MetaException {
        checkDeleteTable(tbl);

    }

    /*
     * @param instance of table
     *
     * @throws MetaException
     *
     * @see org.apache.hcatalog.storagehandler.HCatStorageHandler
     * #preCreateTable(org.apache.hadoop.hive.metastore.api.Table)
     */
    @Override
    public void preCreateTable(Table tbl) throws MetaException {
        boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

        hbaseConf = getConf();

        if (tbl.getSd().getLocation() != null) {
            throw new MetaException("LOCATION may not be specified for HBase.");
        }

        try {
            String tableName = getHBaseTableName(tbl);
            String hbaseColumnsMapping = tbl.getParameters().get(
                    HBaseSerDe.HBASE_COLUMNS_MAPPING);

            tbl.putToParameters(HBaseConstants.PROPERTY_COLUMN_MAPPING_KEY,
                    hbaseColumnsMapping);

            if (hbaseColumnsMapping == null) {
                throw new MetaException(
                        "No hbase.columns.mapping defined in table"
                                + " properties.");
            }

            List<String> hbaseColumnFamilies = new ArrayList<String>();
            List<String> hbaseColumnQualifiers = new ArrayList<String>();
            List<byte[]> hbaseColumnFamiliesBytes = new ArrayList<byte[]>();
            List<byte[]> hbaseColumnQualifiersBytes = new ArrayList<byte[]>();
            int iKey = HBaseSerDe.parseColumnMapping(hbaseColumnsMapping,
                    hbaseColumnFamilies, hbaseColumnFamiliesBytes,
                    hbaseColumnQualifiers, hbaseColumnQualifiersBytes);

            HTableDescriptor tableDesc;

            if (!getHBaseAdmin().tableExists(tableName)) {
                // if it is not an external table then create one
                if (!isExternal) {
                    // Create the column descriptors
                    tableDesc = new HTableDescriptor(tableName);
                    Set<String> uniqueColumnFamilies = new HashSet<String>(
                            hbaseColumnFamilies);
                    uniqueColumnFamilies.remove(hbaseColumnFamilies.get(iKey));

                    for (String columnFamily : uniqueColumnFamilies) {
                        tableDesc.addFamily(new HColumnDescriptor(Bytes
                                .toBytes(columnFamily)));
                    }

                    getHBaseAdmin().createTable(tableDesc);
                } else {
                    // an external table
                    throw new MetaException("HBase table " + tableName
                            + " doesn't exist while the table is "
                            + "declared as an external table.");
                }

            } else {
                if (!isExternal) {
                    throw new MetaException("Table " + tableName
                            + " already exists within HBase."
                            + " Use CREATE EXTERNAL TABLE instead to"
                            + " register it in HCatalog.");
                }
                // make sure the schema mapping is right
                tableDesc = getHBaseAdmin().getTableDescriptor(
                        Bytes.toBytes(tableName));

                for (int i = 0; i < hbaseColumnFamilies.size(); i++) {
                    if (i == iKey) {
                        continue;
                    }

                    if (!tableDesc.hasFamily(hbaseColumnFamiliesBytes.get(i))) {
                        throw new MetaException("Column Family "
                                + hbaseColumnFamilies.get(i)
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

    /*
     * @param table
     *
     * @throws MetaException
     *
     * @see org.apache.hcatalog.storagehandler.HCatStorageHandler
     * #preDropTable(org.apache.hadoop.hive.metastore.api.Table)
     */
    @Override
    public void preDropTable(Table table) throws MetaException {
    }

    /*
     * @param table
     *
     * @throws MetaException
     *
     * @see org.apache.hcatalog.storagehandler.HCatStorageHandler
     * #rollbackCreateTable(org.apache.hadoop.hive.metastore.api.Table)
     */
    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
        checkDeleteTable(table);
    }

    /*
     * @param table
     *
     * @throws MetaException
     *
     * @see org.apache.hcatalog.storagehandler.HCatStorageHandler
     * #rollbackDropTable(org.apache.hadoop.hive.metastore.api.Table)
     */
    @Override
    public void rollbackDropTable(Table table) throws MetaException {
    }

    /*
     * @return instance of HiveMetaHook
     *
     * @see org.apache.hcatalog.storagehandler.HCatStorageHandler#getMetaHook()
     */
    @Override
    public HiveMetaHook getMetaHook() {
        return this;
    }

    /*
     * @param tableDesc
     *
     * @param jobProperties
     *
     * @see org.apache.hcatalog.storagehandler.HCatStorageHandler
     * #configureTableJobProperties(org.apache.hadoop.hive.ql.plan.TableDesc,
     * java.util.Map)
     */
    @Override
    public void configureTableJobProperties(TableDesc tableDesc,
            Map<String, String> jobProperties) {
        Properties tableProperties = tableDesc.getProperties();

        jobProperties.put(HBaseSerDe.HBASE_COLUMNS_MAPPING,
                tableProperties.getProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING));

        String tableName = tableProperties
                .getProperty(HBaseSerDe.HBASE_TABLE_NAME);
        if (tableName == null) {
            tableName = tableProperties.getProperty(Constants.META_TABLE_NAME);
            if (tableName.startsWith(DEFAULT_PREFIX)) {
                tableName = tableName.substring(DEFAULT_PREFIX.length());
            }
        }
        jobProperties.put(HBaseSerDe.HBASE_TABLE_NAME, tableName);

    }

    private HBaseAdmin getHBaseAdmin() throws MetaException {
        try {
            if (admin == null) {
                admin = new HBaseAdmin(this.getConf());
            }
            return admin;
        } catch (MasterNotRunningException mnre) {
            throw new MetaException(StringUtils.stringifyException(mnre));
        } catch (ZooKeeperConnectionException zkce) {
            throw new MetaException(StringUtils.stringifyException(zkce));
        }
    }

    private String getHBaseTableName(Table tbl) {
        String tableName = tbl.getParameters().get(HBaseSerDe.HBASE_TABLE_NAME);
        if (tableName == null) {
            tableName = tbl.getSd().getSerdeInfo().getParameters()
                    .get(HBaseSerDe.HBASE_TABLE_NAME);
        }
        if (tableName == null) {
            if (tbl.getDbName().equals(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
                tableName = tbl.getTableName();
            } else {
                tableName = tbl.getDbName() + "." + tbl.getTableName();
            }
        }
        return tableName;
    }

    /*
     * @return subclass of SerDe
     *
     * @throws UnsupportedOperationException
     *
     * @see
     * org.apache.hcatalog.storagehandler.HCatStorageHandler#getSerDeClass()
     */
    @Override
    public Class<? extends SerDe> getSerDeClass()
            throws UnsupportedOperationException {
        return HBaseSerDe.class;
    }

    @Override
    public Configuration getConf() {

        if (hbaseConf == null) {
            hbaseConf = HBaseConfiguration.create();
        }
        return hbaseConf;
    }

    @Override
    public void setConf(Configuration conf) {
        hbaseConf = HBaseConfiguration.create(conf);
    }

    private void checkDeleteTable(Table table) throws MetaException {
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

    static String getFullyQualifiedName(HCatTableInfo tableInfo){
        String qualifiedName;
        String databaseName = tableInfo.getDatabaseName();
        String tableName = tableInfo.getTableName();

        if ((databaseName == null) || (databaseName.equals(MetaStoreUtils.DEFAULT_DATABASE_NAME))) {
            qualifiedName = tableName;
        } else {
            qualifiedName = databaseName + "." + tableName;
        }

        return qualifiedName;
    }

    /**
     * Helper method for users to add the required depedency jars to distributed cache.
     * @param conf
     * @throws IOException
     */
    public static void addDependencyJars(Configuration conf) throws IOException {
        //TODO provide a facility/interface for loading/specifying dependencies
        //Ideally this method shouldn't be exposed to the user
        TableMapReduceUtil.addDependencyJars(conf,
                //hadoop-core
                Writable.class,
                //ZK
                ZooKeeper.class,
                //HBase
                HTable.class,
                //Hive
                HiveException.class,
                //HCatalog jar
                HCatOutputFormat.class,
                //hive hbase storage handler jar
                HBaseSerDe.class,
                //hcat hbase storage driver jar
                HBaseOutputStorageDriver.class,
                //hive jar
                Table.class,
                //libthrift jar
                TBase.class,
                //hbase jar
                Bytes.class,
                //thrift-fb303 .jar
                FacebookBase.class);
    }

}
