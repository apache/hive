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
package org.apache.hive.hcatalog.templeton;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * TestDesc - Test the desc objects that are correctly converted to
 * and from json.  This also sets every field of the TableDesc object.
 */
public class TestDesc {
  @Test
  public void testTableDesc()
    throws Exception
  {
    TableDesc td = buildTableDesc();
    assertNotNull(td);

    String json = toJson(td);
    assertTrue(json.length() > 100);

    TableDesc tdCopy = (TableDesc) fromJson(json, TableDesc.class);
    assertEquals(td, tdCopy);
  }

  private TableDesc buildTableDesc() {
    TableDesc x = new TableDesc();
    x.group = "staff";
    x.permissions = "755";
    x.external = true;
    x.ifNotExists = true;
    x.table = "a_table";
    x.comment = "a comment";
    x.columns = buildColumns();
    x.partitionedBy = buildPartitionedBy();
    x.clusteredBy = buildClusterBy();
    x.format = buildStorageFormat();
    x.location = "hdfs://localhost:9000/user/me/a_table";
    x.tableProperties = buildGenericProperties();
    return x;
  }

  public List<ColumnDesc> buildColumns() {
    ArrayList<ColumnDesc> x = new ArrayList<ColumnDesc>();
    x.add(new ColumnDesc("id", "bigint", null));
    x.add(new ColumnDesc("price", "float", "The unit price"));
    x.add(new ColumnDesc("name", "string", "The item name"));
    return x;
  }

  public List<ColumnDesc> buildPartitionedBy() {
    ArrayList<ColumnDesc> x = new ArrayList<ColumnDesc>();
    x.add(new ColumnDesc("country", "string", "The country of origin"));
    return x;
  }

  public TableDesc.ClusteredByDesc buildClusterBy() {
    TableDesc.ClusteredByDesc x = new TableDesc.ClusteredByDesc();
    x.columnNames = new ArrayList<String>();
    x.columnNames.add("id");
    x.sortedBy = buildSortedBy();
    x.numberOfBuckets = 16;
    return x;
  }

  public List<TableDesc.ClusterSortOrderDesc> buildSortedBy() {
    ArrayList<TableDesc.ClusterSortOrderDesc> x
      = new ArrayList<TableDesc.ClusterSortOrderDesc>();
    x.add(new TableDesc.ClusterSortOrderDesc("id", TableDesc.SortDirectionDesc.ASC));
    return x;
  }

  public TableDesc.StorageFormatDesc buildStorageFormat() {
    TableDesc.StorageFormatDesc x = new TableDesc.StorageFormatDesc();
    x.rowFormat = buildRowFormat();
    x.storedAs = "rcfile";
    x.storedBy = buildStoredBy();
    return x;
  }

  public TableDesc.RowFormatDesc buildRowFormat() {
    TableDesc.RowFormatDesc x = new TableDesc.RowFormatDesc();
    x.fieldsTerminatedBy = "\u0001";
    x.collectionItemsTerminatedBy = "\u0002";
    x.mapKeysTerminatedBy = "\u0003";
    x.linesTerminatedBy = "\u0004";
    x.serde = buildSerde();
    return x;
  }

  public TableDesc.SerdeDesc buildSerde() {
    TableDesc.SerdeDesc x = new TableDesc.SerdeDesc();
    x.name = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";
    x.properties = new HashMap<String, String>();
    x.properties.put("field.delim", ",");
    return x;
  }

  public TableDesc.StoredByDesc buildStoredBy() {
    TableDesc.StoredByDesc x = new TableDesc.StoredByDesc();
    x.className = "org.apache.hadoop.hive.hbase.HBaseStorageHandler";
    x.properties = new HashMap<String, String>();
    x.properties.put("hbase.columns.mapping", "cf:string");
    x.properties.put("hbase.table.name", "hbase_table_0");
    return x;
  }

  public Map<String, String> buildGenericProperties() {
    HashMap<String, String> x = new HashMap<String, String>();
    x.put("carmas", "evil");
    x.put("rachel", "better");
    x.put("ctdean", "angelic");
    x.put("paul", "dangerously unbalanced");
    x.put("dra", "organic");
    return x;
  }

  private String toJson(Object obj)
    throws Exception
  {
    ObjectMapper mapper = new ObjectMapper();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    mapper.writeValue(out, obj);
    return out.toString();
  }

  private Object fromJson(String json, Class klass)
    throws Exception
  {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(json, klass);
  }
}
