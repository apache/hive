/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.thrift.TException;

public class StreamingTestUtils {

  public HiveConf newHiveConf(String metaStoreUri) {
    HiveConf conf = new HiveConf(this.getClass());
    conf.set("fs.raw.impl", RawFileSystem.class.getName());
    if (metaStoreUri != null) {
      conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUri);
    }
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    return conf;
  }

  public void prepareTransactionDatabase(HiveConf conf) throws Exception {
    TxnDbUtil.setConfValues(conf);
    TxnDbUtil.cleanDb(conf);
    TxnDbUtil.prepDb(conf);
  }

  public IMetaStoreClient newMetaStoreClient(HiveConf conf) throws Exception {
    return new HiveMetaStoreClient(conf);
  }

  public static class RawFileSystem extends RawLocalFileSystem {
    private static final URI NAME;
    static {
      try {
        NAME = new URI("raw:///");
      } catch (URISyntaxException se) {
        throw new IllegalArgumentException("bad uri", se);
      }
    }

    @Override
    public URI getUri() {
      return NAME;
    }

    @Override
    public String getScheme() {
      return "raw";
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      File file = pathToFile(path);
      if (!file.exists()) {
        throw new FileNotFoundException("Can't find " + path);
      }
      // get close enough
      short mod = 0;
      if (file.canRead()) {
        mod |= 0444;
      }
      if (file.canWrite()) {
        mod |= 0200;
      }
      if (file.canExecute()) {
        mod |= 0111;
      }
      return new FileStatus(file.length(), file.isDirectory(), 1, 1024, file.lastModified(), file.lastModified(),
          FsPermission.createImmutable(mod), "owen", "users", path);
    }
  }

  public static DatabaseBuilder databaseBuilder(File warehouseFolder) {
    return new DatabaseBuilder(warehouseFolder);
  }

  public static class DatabaseBuilder {

    private Database database;
    private File warehouseFolder;

    public DatabaseBuilder(File warehouseFolder) {
      this.warehouseFolder = warehouseFolder;
      database = new Database();
    }

    public DatabaseBuilder name(String name) {
      database.setName(name);
      File databaseFolder = new File(warehouseFolder, name + ".db");
      String databaseLocation = "raw://" + databaseFolder.toURI().getPath();
      database.setLocationUri(databaseLocation);
      return this;
    }

    public Database dropAndCreate(IMetaStoreClient metaStoreClient) throws Exception {
      if (metaStoreClient == null) {
        throw new IllegalArgumentException();
      }
      try {
        for (String table : metaStoreClient.listTableNamesByFilter(database.getName(), "", (short) -1)) {
          metaStoreClient.dropTable(database.getName(), table, true, true);
        }
        metaStoreClient.dropDatabase(database.getName());
      } catch (TException e) {
      }
      metaStoreClient.createDatabase(database);
      return database;
    }

    public Database build() {
      return database;
    }

  }

  public static TableBuilder tableBuilder(Database database) {
    return new TableBuilder(database);
  }

  public static class TableBuilder {

    private Table table;
    private StorageDescriptor sd;
    private SerDeInfo serDeInfo;
    private Database database;
    private List<List<String>> partitions;
    private List<String> columnNames;
    private List<String> columnTypes;
    private List<String> partitionKeys;

    public TableBuilder(Database database) {
      this.database = database;
      partitions = new ArrayList<>();
      columnNames = new ArrayList<>();
      columnTypes = new ArrayList<>();
      partitionKeys = Collections.emptyList();
      table = new Table();
      table.setDbName(database.getName());
      table.setTableType(TableType.MANAGED_TABLE.toString());
      Map<String, String> tableParams = new HashMap<String, String>();
      tableParams.put("transactional", Boolean.TRUE.toString());
      table.setParameters(tableParams);

      sd = new StorageDescriptor();
      sd.setInputFormat(OrcInputFormat.class.getName());
      sd.setOutputFormat(OrcOutputFormat.class.getName());
      sd.setNumBuckets(1);
      table.setSd(sd);

      serDeInfo = new SerDeInfo();
      serDeInfo.setParameters(new HashMap<String, String>());
      serDeInfo.getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
      serDeInfo.setSerializationLib(OrcSerde.class.getName());
      sd.setSerdeInfo(serDeInfo);
    }

    public TableBuilder name(String name) {
      sd.setLocation(database.getLocationUri() + Path.SEPARATOR + name);
      table.setTableName(name);
      serDeInfo.setName(name);
      return this;
    }

    public TableBuilder buckets(int buckets) {
      sd.setNumBuckets(buckets);
      return this;
    }

    public TableBuilder bucketCols(List<String> columnNames) {
      sd.setBucketCols(columnNames);
      return this;
    }

    public TableBuilder addColumn(String columnName, String columnType) {
      columnNames.add(columnName);
      columnTypes.add(columnType);
      return this;
    }

    public TableBuilder partitionKeys(String... partitionKeys) {
      this.partitionKeys = Arrays.asList(partitionKeys);
      return this;
    }

    public TableBuilder addPartition(String... partitionValues) {
      partitions.add(Arrays.asList(partitionValues));
      return this;
    }

    public TableBuilder addPartition(List<String> partitionValues) {
      partitions.add(partitionValues);
      return this;
    }

    public Table create(IMetaStoreClient metaStoreClient) throws Exception {
      if (metaStoreClient == null) {
        throw new IllegalArgumentException();
      }
      return internalCreate(metaStoreClient);
    }

    public Table build() throws Exception {
      return internalCreate(null);
    }

    private Table internalCreate(IMetaStoreClient metaStoreClient) throws Exception {
      List<FieldSchema> fields = new ArrayList<FieldSchema>(columnNames.size());
      for (int i = 0; i < columnNames.size(); i++) {
        fields.add(new FieldSchema(columnNames.get(i), columnTypes.get(i), ""));
      }
      sd.setCols(fields);

      if (!partitionKeys.isEmpty()) {
        List<FieldSchema> partitionFields = new ArrayList<FieldSchema>();
        for (String partitionKey : partitionKeys) {
          partitionFields.add(new FieldSchema(partitionKey, serdeConstants.STRING_TYPE_NAME, ""));
        }
        table.setPartitionKeys(partitionFields);
      }
      if (metaStoreClient != null) {
        metaStoreClient.createTable(table);
      }

      for (List<String> partitionValues : partitions) {
        Partition partition = new Partition();
        partition.setDbName(database.getName());
        partition.setTableName(table.getTableName());
        StorageDescriptor partitionSd = new StorageDescriptor(table.getSd());
        partitionSd.setLocation(table.getSd().getLocation() + Path.SEPARATOR
            + Warehouse.makePartName(table.getPartitionKeys(), partitionValues));
        partition.setSd(partitionSd);
        partition.setValues(partitionValues);

        if (metaStoreClient != null) {
          metaStoreClient.add_partition(partition);
        }
      }
      return table;
    }
  }

}
