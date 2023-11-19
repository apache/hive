/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Tables;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.rules.TemporaryFolder;

public class TestHelper {
  private final Configuration conf;
  private final Tables tables;
  private final String tableIdentifier;
  private final Schema schema;
  private final PartitionSpec spec;
  private final FileFormat fileFormat;
  private final TemporaryFolder tmp;
  private final Map<String, String> tblProps;
  private SortOrder order;

  private Table table;

  public TestHelper(Configuration conf, Tables tables, String tableIdentifier, Schema schema, PartitionSpec spec,
                    FileFormat fileFormat, TemporaryFolder tmp) {
    this(conf, tables, tableIdentifier, schema, spec, fileFormat, ImmutableMap.of(), tmp);
  }

  public TestHelper(Configuration conf, Tables tables, String tableIdentifier, Schema schema, PartitionSpec spec,
      FileFormat fileFormat, Map<String, String> tblProps, TemporaryFolder tmp) {
    this.conf = conf;
    this.tables = tables;
    this.tableIdentifier = tableIdentifier;
    this.schema = schema;
    this.spec = spec;
    this.fileFormat = fileFormat;
    this.tblProps = tblProps;
    this.tmp = tmp;
  }

  public void setTable(Table table) {
    this.table = table;
    conf.set(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(table.schema()));
  }

  public void setOrder(SortOrder order) {
    this.order = order;
  }

  public Table table() {
    return table;
  }

  public Map<String, String> properties() {
    Map<String, String> props = Maps.newHashMap(tblProps);
    props.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    props.put(TableProperties.ENGINE_HIVE_ENABLED, "true");
    props.put(TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
    return props;
  }

  public Table createTable(Schema theSchema, PartitionSpec theSpec) {
    return createTable(theSchema, theSpec, null);
  }

  public Table createTable(Schema theSchema, PartitionSpec theSpec, SortOrder theOrder) {
    Table tbl;
    if (theOrder != null) {
      tbl = tables.create(theSchema, theSpec, theOrder, properties(), tableIdentifier);
    } else {
      tbl = tables.create(theSchema, theSpec, properties(), tableIdentifier);
    }
    setTable(tbl);
    return tbl;
  }

  public Table createTable() {
    return createTable(schema, spec, order);
  }

  public Table createUnpartitionedTable() {
    return createTable(schema, PartitionSpec.unpartitioned());
  }


  public List<Record> generateRandomRecords(int num, long seed) {
    Preconditions.checkNotNull(table, "table not set");
    return generateRandomRecords(table.schema(), num, seed);
  }

  public static List<Record> generateRandomRecords(Schema schema, int num, long seed) {
    return RandomGenericData.generate(schema, num, seed);
  }

  public void appendToTable(DataFile... dataFiles) {
    appender().appendToTable(dataFiles);
  }

  public void appendToTable(StructLike partition, List<Record> records) throws IOException {
    appender().appendToTable(partition, records);
  }

  /**
   * Appends the rows to the table. If the table is partitioned then it will create the correct partitions.
   * @param rowSet The rows to add
   * @throws IOException If there is an exception during writing out the files
   */
  public void appendToTable(List<Record> rowSet) throws IOException {
    // The rows collected by partitions
    Map<PartitionKey, List<Record>> rows = Maps.newHashMap();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    for (Record record : rowSet) {
      partitionKey.partition(record);
      List<Record> partitionRows = rows.get(partitionKey);
      if (partitionRows == null) {
        partitionRows = Lists.newArrayList();
        rows.put(partitionKey.copy(), partitionRows);
      }

      partitionRows.add(record);
    }

    for (PartitionKey partition : rows.keySet()) {
      appendToTable(partition, rows.get(partition));
    }
  }

  public DataFile writeFile(StructLike partition, List<Record> records) throws IOException {
    return appender().writeFile(partition, records);
  }

  public Map<DataFile, List<Record>> writeFiles(List<Record> rowSet) throws IOException {
    // The rows collected by partitions
    Map<PartitionKey, List<Record>> rows = Maps.newHashMap();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    for (Record record : rowSet) {
      partitionKey.partition(record);
      List<Record> partitionRows = rows.get(partitionKey);
      if (partitionRows == null) {
        partitionRows = rows.put(partitionKey.copy(), Lists.newArrayList());
      }

      partitionRows.add(record);
    }

    // Write out the partitions one-by-one
    Map<DataFile, List<Record>> dataFiles = Maps.newHashMapWithExpectedSize(rows.size());
    for (PartitionKey partition : rows.keySet()) {
      dataFiles.put(writeFile(partition, rows.get(partition)), rows.get(partition));
    }

    return dataFiles;
  }

  private GenericAppenderHelper appender() {
    return new GenericAppenderHelper(table, fileFormat, tmp, conf);
  }

  public static class RecordsBuilder {

    private final List<Record> records = new ArrayList<Record>();
    private final Schema schema;

    private RecordsBuilder(Schema schema) {
      this.schema = schema;
    }

    public RecordsBuilder add(Object... values) {
      Preconditions.checkArgument(schema.columns().size() == values.length);

      GenericRecord record = GenericRecord.create(schema);

      for (int i = 0; i < values.length; i++) {
        record.set(i, values[i]);
      }

      records.add(record);
      return this;
    }

    public List<Record> build() {
      return Collections.unmodifiableList(records);
    }

    public static RecordsBuilder newInstance(Schema schema) {
      return new RecordsBuilder(schema);
    }
  }
}
