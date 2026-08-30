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
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.parquet.ParquetBloomRowGroupFilter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Verifies that Hive inserts into Parquet Iceberg tables honor the
 * {@code write.parquet.bloom-filter-enabled.column.*} table properties: the written files must contain working
 * bloom filters that Iceberg's read-side row group filter can prune on.
 */
public class TestHiveIcebergParquetBloomFilter extends HiveIcebergStorageHandlerWithEngineBase {

  private static final long PRESENT_ID = 42L;
  private static final long ABSENT_ID = 12345678L;

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.fileFormat() == FileFormat.PARQUET && p.testTableType() == TestTableType.HIVE_CATALOG &&
            p.formatVersion() == 2);
  }

  @Test
  public void testBloomFilterWrittenByHiveInsert() throws IOException {
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        optional(2, "name", Types.StringType.get()));

    testTables.createTable(shell, "bloom_test", schema, fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of(
            TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id", "true",
            TableProperties.PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX + "id", "0.01"));

    shell.executeStatement("INSERT INTO bloom_test VALUES (1, 'a'), (" + PRESENT_ID + ", 'b'), (100, 'c')");

    Table table = testTables.loadTable(TableIdentifier.of("default", "bloom_test"));
    List<DataFile> dataFiles = Lists.newArrayList(table.currentSnapshot().addedDataFiles(table.io()));
    Assert.assertEquals(1, dataFiles.size());

    HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(dataFiles.get(0).location()), shell.getHiveConf());
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
      List<BlockMetaData> rowGroups = reader.getFooter().getBlocks();
      Assert.assertFalse(rowGroups.isEmpty());

      for (BlockMetaData rowGroup : rowGroups) {
        BloomFilterReader bloomReader = reader.getBloomFilterDataReader(rowGroup);

        BloomFilter bloom = bloomReader.readBloomFilter(columnChunk(rowGroup, "id"));
        Assert.assertNotNull("Bloom filter should be written for the enabled column", bloom);
        Assert.assertTrue(bloom.findHash(bloom.hash(PRESENT_ID)));
        Assert.assertFalse(bloom.findHash(bloom.hash(ABSENT_ID)));

        Assert.assertNull("Bloom filter should not be written for a column where it was not enabled",
            bloomReader.readBloomFilter(columnChunk(rowGroup, "name")));

        Assert.assertTrue(new ParquetBloomRowGroupFilter(schema, Expressions.equal("id", PRESENT_ID))
            .shouldRead(fileSchema, rowGroup, bloomReader));
        Assert.assertFalse("Row group should be prunable for a value not in the bloom filter",
            new ParquetBloomRowGroupFilter(schema, Expressions.equal("id", ABSENT_ID))
                .shouldRead(fileSchema, rowGroup, bloomReader));
      }
    }

    List<Object[]> rows = shell.executeStatement("SELECT name FROM bloom_test WHERE id = " + PRESENT_ID);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("b", rows.get(0)[0]);
    Assert.assertTrue(shell.executeStatement("SELECT * FROM bloom_test WHERE id = " + ABSENT_ID).isEmpty());
  }

  private static ColumnChunkMetaData columnChunk(BlockMetaData rowGroup, String columnName) {
    return rowGroup.getColumns().stream()
        .filter(column -> column.getPath().toDotString().equals(columnName))
        .findAny()
        .orElseThrow();
  }
}
