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
package org.apache.hadoop.hive.ql.io.parquet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VariantColumnVectors;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.io.NullWritable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestVectorizedVariantColumnReader {

  private static final String SCHEMA = "message hive_schema { "
      + "optional group v { optional binary metadata; optional binary value; } }";

  @Test
  public void testVectorizedReadOfTheTwoBinaryGroup() throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path(System.getProperty("test.tmp.dir", "target/tmp"), "TestVectorizedVariantColumnReader.parquet");
    FileSystem.getLocal(conf).delete(path, false);
    List<Variant> written = Arrays.asList(
        VariantBuilder.parseJson("{\"a\":1}", false), null, VariantBuilder.parseJson("[true,\"x\"]", false));

    MessageType schema = MessageTypeParser.parseMessageType(SCHEMA);
    SimpleGroupFactory groups = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withType(schema).withConf(conf).build()) {
      for (Variant variant : written) {
        Group row = groups.newGroup();
        if (variant != null) {
          row.addGroup("v")
              .append("metadata", Binary.fromConstantByteArray(variant.getMetadata()))
              .append("value", Binary.fromConstantByteArray(variant.getValue()));
        }
        writer.write(row);
      }
    }

    Configuration readerConf = new Configuration();
    readerConf.set(IOConstants.COLUMNS, "v");
    readerConf.set(IOConstants.COLUMNS_TYPES, "variant");
    readerConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    readerConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
        VectorizedColumnReaderTestBase.createTestParquetReader(SCHEMA, readerConf, null, path);
    try {
      VectorizedRowBatch batch = reader.createValue();
      assertTrue(reader.next(NullWritable.get(), batch));
      assertEquals(written.size(), batch.size);
      StructColumnVector column = (StructColumnVector) batch.cols[0];
      for (int i = 0; i < written.size(); i++) {
        Variant expected = written.get(i);
        if (expected == null) {
          assertTrue(column.isNull[i]);
        } else {
          assertFalse(column.isNull[i]);
          assertEquals(expected, VariantColumnVectors.getVariant(column, i));
        }
      }
    } finally {
      reader.close();
    }
  }

  /** Index access maps the column by position; its fields are still found by name. */
  @Test
  public void testIndexAccessReadsTheFieldsByName() throws Exception {
    Variant variant = VariantBuilder.parseJson("\"abcd\"", false);
    Path path = write("index-access", "optional group file_name { optional binary value; optional binary metadata; }",
        "file_name", variant);

    Configuration conf = readerConf();
    conf.setBoolean(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS, true);
    assertEquals(List.of(variant), read(path, conf));
  }

  @Test
  public void testShreddedVariantFails() throws Exception {
    Path path = write("shredded",
        "optional group v { optional binary value; optional binary metadata; optional int64 typed_value; }",
        "v", VariantBuilder.parseJson("{\"b\":\"x\"}", false));

    for (boolean indexAccess : new boolean[] {false, true}) {
      Configuration conf = readerConf();
      conf.setBoolean(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS, indexAccess);
      Exception e = assertThrows(Exception.class, () -> read(path, conf));
      TestParquetVariant.assertShreddedVariantError(e, "v");
    }
  }

  /** A caller that rebuilds shredded values from typed_value gets the metadata and value of each row. */
  @Test
  public void testShreddedVariantReadsMetadataAndValueWhenRebuilt() throws Exception {
    Variant variant = VariantBuilder.parseJson("{\"b\":\"x\"}", false);
    Path path = write("rebuilt",
        "optional group v { optional binary value; optional binary metadata; optional int64 typed_value; }",
        "v", variant);

    for (boolean indexAccess : new boolean[] {false, true}) {
      Configuration conf = readerConf();
      conf.setBoolean(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS, indexAccess);
      conf.setBoolean(DataWritableReadSupport.SHREDDED_VARIANT_REBUILT, true);
      assertEquals(List.of(variant), read(path, conf));
    }
  }

  /** Writes one row whose only column, the group {@code field} declared by {@code group}, holds {@code variant}. */
  private static Path write(String name, String group, String field, Variant variant) throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path(System.getProperty("test.tmp.dir", "target/tmp"),
        "TestVectorizedVariantColumnReader-" + name + ".parquet");
    FileSystem.getLocal(conf).delete(path, false);
    MessageType schema = MessageTypeParser.parseMessageType("message hive_schema { " + group + " }");
    Group row = new SimpleGroupFactory(schema).newGroup();
    row.addGroup(field)
        .append("metadata", Binary.fromConstantByteArray(variant.getMetadata()))
        .append("value", Binary.fromConstantByteArray(variant.getValue()));
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withType(schema).withConf(conf).build()) {
      writer.write(row);
    }
    return path;
  }

  private static Configuration readerConf() {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "v");
    conf.set(IOConstants.COLUMNS_TYPES, "variant");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    return conf;
  }

  private static List<Variant> read(Path path, Configuration conf) throws Exception {
    VectorizedParquetRecordReader reader =
        VectorizedColumnReaderTestBase.createTestParquetReader(SCHEMA, conf, null, path);
    try {
      VectorizedRowBatch batch = reader.createValue();
      List<Variant> variants = new ArrayList<>();
      while (reader.next(NullWritable.get(), batch)) {
        StructColumnVector column = (StructColumnVector) batch.cols[0];
        for (int i = 0; i < batch.size; i++) {
          variants.add(column.isNull[i] ? null : VariantColumnVectors.getVariant(column, i));
        }
      }
      return variants;
    } finally {
      reader.close();
    }
  }
}
