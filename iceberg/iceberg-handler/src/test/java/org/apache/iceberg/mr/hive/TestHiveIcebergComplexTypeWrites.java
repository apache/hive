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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Runs insert queries against Iceberg tables with complex type columns, such as arrays, structs and maps.
 */
public class TestHiveIcebergComplexTypeWrites extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testWriteArrayOfPrimitivesInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "arrayofprimitives",
            Types.ListType.ofRequired(3, Types.StringType.get())));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteArrayOfArraysInTable() throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "arrayofarrays",
                Types.ListType.ofRequired(3, Types.ListType.ofRequired(4, Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 3, 1L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteArrayOfMapsInTable() throws IOException {
    Schema schema =
        new Schema(required(1, "id", Types.LongType.get()),
            required(2, "arrayofmaps", Types.ListType
                .ofRequired(3, Types.MapType.ofRequired(4, 5, Types.StringType.get(),
                    Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 1L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteArrayOfStructsInTable() throws IOException {
    Schema schema =
        new Schema(required(1, "id", Types.LongType.get()),
            required(2, "arrayofstructs", Types.ListType.ofRequired(3, Types.StructType
                .of(required(4, "something", Types.StringType.get()), required(5, "someone",
                    Types.StringType.get()), required(6, "somewhere", Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteMapOfPrimitivesInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "mapofprimitives", Types.MapType.ofRequired(3, 4, Types.StringType.get(),
            Types.StringType.get())));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteMapOfArraysInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "mapofarrays",
            Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.ListType.ofRequired(5,
                Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteMapOfMapsInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "mapofmaps", Types.MapType.ofRequired(3, 4, Types.StringType.get(),
            Types.MapType.ofRequired(5, 6, Types.StringType.get(), Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteMapOfStructsInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "mapofstructs", Types.MapType.ofRequired(3, 4, Types.StringType.get(),
            Types.StructType.of(required(5, "something", Types.StringType.get()),
                required(6, "someone", Types.StringType.get()),
                required(7, "somewhere", Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteStructOfPrimitivesInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "structofprimitives",
            Types.StructType.of(required(3, "key", Types.StringType.get()), required(4, "value",
                Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteStructOfArraysInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "structofarrays", Types.StructType
            .of(required(3, "names", Types.ListType.ofRequired(4, Types.StringType.get())),
                required(5, "birthdays", Types.ListType.ofRequired(6,
                    Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 1L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteStructOfMapsInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "structofmaps", Types.StructType
            .of(required(3, "map1", Types.MapType.ofRequired(4, 5,
                Types.StringType.get(), Types.StringType.get())), required(6, "map2",
                Types.MapType.ofRequired(7, 8, Types.StringType.get(),
                    Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @Test
  public void testWriteStructOfStructsInTable() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        required(2, "structofstructs", Types.StructType.of(required(3, "struct1", Types.StructType
            .of(required(4, "key", Types.StringType.get()), required(5, "value",
                Types.StringType.get()))))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  private void testComplexTypeWrite(Schema schema, List<Record> records) throws IOException {
    String tableName = "complex_table";
    Table table = testTables.createTable(shell, "complex_table", schema, fileFormat, ImmutableList.of());

    String dummyTableName = "dummy";
    shell.executeStatement("CREATE TABLE default." + dummyTableName + "(a int)");
    shell.executeStatement("INSERT INTO TABLE default." + dummyTableName + " VALUES(1)");
    records.forEach(r -> shell.executeStatement(insertQueryForComplexType(tableName, dummyTableName, schema, r)));
    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  private String insertQueryForComplexType(String tableName, String dummyTableName, Schema schema, Record record) {
    StringBuilder query = new StringBuilder("INSERT INTO TABLE ").append(tableName).append(" SELECT ")
        .append(record.get(0)).append(", ");
    Type type = schema.asStruct().fields().get(1).type();
    query.append(buildComplexTypeInnerQuery(record.get(1), type));
    query.setLength(query.length() - 1);
    query.append(" FROM ").append(dummyTableName).append(" LIMIT 1");
    return query.toString();
  }

  private StringBuilder buildComplexTypeInnerQuery(Object field, Type type) {
    StringBuilder query = new StringBuilder();
    if (type instanceof Types.ListType) {
      query.append("array(");
      List<Object> elements = (List<Object>) field;
      Assert.assertFalse("Hive can not handle empty array() inserts", elements.isEmpty());
      Type innerType = ((Types.ListType) type).fields().get(0).type();
      if (!elements.isEmpty()) {
        elements.forEach(e -> query.append(buildComplexTypeInnerQuery(e, innerType)));
        query.setLength(query.length() - 1);
      }
      query.append("),");
    } else if (type instanceof Types.MapType) {
      query.append("map(");
      Map<Object, Object> entries = (Map<Object, Object>) field;
      Type keyType = ((Types.MapType) type).fields().get(0).type();
      Type valueType = ((Types.MapType) type).fields().get(1).type();
      if (!entries.isEmpty()) {
        entries.entrySet().forEach(e -> query.append(buildComplexTypeInnerQuery(e.getKey(), keyType)
            .append(buildComplexTypeInnerQuery(e.getValue(), valueType))));
        query.setLength(query.length() - 1);
      }
      query.append("),");
    } else if (type instanceof Types.StructType) {
      query.append("named_struct(");
      ((GenericRecord) field).struct().fields().stream()
          .forEach(f -> query.append(buildComplexTypeInnerQuery(f.name(), Types.StringType.get()))
              .append(buildComplexTypeInnerQuery(((GenericRecord) field).getField(f.name()), f.type())));
      query.setLength(query.length() - 1);
      query.append("),");
    } else if (type instanceof Types.StringType) {
      if (field != null) {
        query.append("'").append(field).append("',");
      }
    } else {
      throw new RuntimeException("Unsupported type in complex query build.");
    }
    return query;
  }
}
