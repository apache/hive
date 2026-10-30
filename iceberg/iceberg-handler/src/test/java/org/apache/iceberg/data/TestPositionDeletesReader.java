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

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestPositionDeletesReader {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));
  private static final String DATA_DIR = "s3a://bucket/data/warehouse/tablespace/external/hive/sensors.db/readings/" +
      "data/station_bucket=4/ts_day=2024-02-08/sensor_type_trunc=10/";

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testSortedDeletes() throws IOException {
    List<Pair<CharSequence, Long>> deletes = randomDeletes(5, 20_000, new Random(1));
    deletes.sort(pathThenPos());

    assertSameAsDeletes(writeDeletes(FileFormat.PARQUET, ImmutableMap.of(), deletes));
  }

  @Test
  public void testUnsortedDeletesRevisitingPaths() throws IOException {
    List<Pair<CharSequence, Long>> deletes = randomDeletes(5, 20_000, new Random(2));

    assertSameAsDeletes(writeDeletes(FileFormat.PARQUET, ImmutableMap.of(), deletes));
  }

  @Test
  public void testDictionaryPerRowGroup() throws IOException {
    List<Pair<CharSequence, Long>> deletes = randomDeletes(4, 50_000, new Random(3));
    deletes.sort(pathThenPos());
    DeleteFile deleteFile = writeDeletes(FileFormat.PARQUET, ImmutableMap.of(
        TableProperties.DELETE_PARQUET_ROW_GROUP_SIZE_BYTES, "4096",
        TableProperties.DELETE_PARQUET_PAGE_SIZE_BYTES, "512"), deletes);

    List<BlockMetaData> rowGroups = rowGroups(deleteFile);
    Assert.assertTrue("Expected several row groups, got " + rowGroups.size(), rowGroups.size() > 1);
    Assert.assertTrue(pathChunk(rowGroups.get(0)).hasDictionaryPage());
    assertSameAsDeletes(deleteFile);
  }

  @Test
  public void testPlainEncodedPaths() throws IOException {
    DeleteFile deleteFile = writeDeletesWithoutDictionary(ImmutableMap.of());

    Assert.assertTrue(pathChunks(deleteFile).stream().noneMatch(ColumnChunkMetaData::hasDictionaryPage));
    assertSameAsDeletes(deleteFile);
  }

  @Test
  public void testDeltaEncodedPathsOnV2Pages() throws IOException {
    DeleteFile deleteFile =
        writeDeletesWithoutDictionary(ImmutableMap.of(TableProperties.DELETE_PARQUET_PAGE_VERSION, "v2"));

    Assert.assertTrue(pathChunks(deleteFile).stream()
        .allMatch(chunk -> chunk.getEncodings().contains(Encoding.DELTA_BYTE_ARRAY)));
    assertSameAsDeletes(deleteFile);
  }

  @Test
  public void testOrcDeletes() throws IOException {
    assertSameAsDeletes(
        writeDeletes(FileFormat.ORC, ImmutableMap.of(), randomDeletes(5, 20_000, new Random(5))));
  }

  @Test
  public void testAvroDeletes() throws IOException {
    assertSameAsDeletes(
        writeDeletes(FileFormat.AVRO, ImmutableMap.of(), randomDeletes(5, 20_000, new Random(6))));
  }

  /**
   * Positions spread over the data files in random order, with duplicates.
   */
  private static List<Pair<CharSequence, Long>> randomDeletes(int dataFiles, int count, Random random) {
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      deletes.add(Pair.of(dataFile(random.nextInt(dataFiles)), (long) random.nextInt(count)));
    }
    return deletes;
  }

  private static String dataFile(int index) {
    return String.format("%s%05d-6-3f3c6a5e-7b1d-4f6e-9a0b-9c8d7e6f5a4b-0-%05d.parquet", DATA_DIR, index, index);
  }

  private static Comparator<Pair<CharSequence, Long>> pathThenPos() {
    return Comparator.<Pair<CharSequence, Long>, String>comparing(delete -> delete.first().toString())
        .thenComparing(Pair::second);
  }

  private DeleteFile writeDeletesWithoutDictionary(Map<String, String> properties) throws IOException {
    List<Pair<CharSequence, Long>> deletes = randomDeletes(200, 20_000, new Random(4));
    deletes.sort(pathThenPos());
    return writeDeletes(FileFormat.PARQUET, ImmutableMap.<String, String>builder()
        .put(TableProperties.DELETE_PARQUET_DICT_SIZE_BYTES, "1")
        .putAll(properties)
        .buildOrThrow(), deletes);
  }

  private DeleteFile writeDeletes(FileFormat format, Map<String, String> properties,
      List<Pair<CharSequence, Long>> deletes) throws IOException {
    Table table = new HadoopTables(new Configuration()).create(SCHEMA, PartitionSpec.unpartitioned(),
        ImmutableMap.<String, String>builder()
            .put(TableProperties.FORMAT_VERSION, "2")
            .put(TableProperties.DELETE_DEFAULT_FILE_FORMAT, format.name())
            .putAll(properties)
            .buildOrThrow(),
        temp.newFolder().toString());
    File file = new File(temp.newFolder(), "deletes." + format.name().toLowerCase());
    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(table, Files.localOutput(file), null, deletes, 2).first();
    Assert.assertEquals(format, deleteFile.format());
    return deleteFile;
  }

  private static void assertSameAsDeletes(DeleteFile deleteFile) {
    InputFile file = inputFile(deleteFile);
    CharSequenceMap<PositionDeleteIndex> expected = Deletes.toPositionIndexes(
        FormatModelRegistry.readBuilder(deleteFile.format(), Record.class, file)
            .project(DeleteSchemaUtil.pathPosSchema())
            .build(),
        deleteFile);

    Map<String, List<Long>> actual = positions(PositionDeletesReader.read(deleteFile, file), deleteFile);
    Assert.assertFalse(actual.isEmpty());
    Assert.assertEquals(positions(expected, deleteFile), actual);
  }

  private static Map<String, List<Long>> positions(CharSequenceMap<PositionDeleteIndex> indexes,
      DeleteFile deleteFile) {
    Map<String, List<Long>> positions = new TreeMap<>();
    indexes.forEach((path, index) -> {
      Assert.assertEquals(ImmutableList.of(deleteFile), ImmutableList.copyOf(index.deleteFiles()));
      List<Long> list = Lists.newArrayList();
      index.forEach(list::add);
      positions.put(path.toString(), list);
    });
    return positions;
  }

  private static InputFile inputFile(DeleteFile deleteFile) {
    return Files.localInput(deleteFile.location().replaceFirst("^file:", ""));
  }

  private static List<BlockMetaData> rowGroups(DeleteFile deleteFile) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(new Path(deleteFile.location()), new Configuration()))) {
      return reader.getFooter().getBlocks();
    }
  }

  private static List<ColumnChunkMetaData> pathChunks(DeleteFile deleteFile) throws IOException {
    return rowGroups(deleteFile).stream().map(TestPositionDeletesReader::pathChunk).collect(Collectors.toList());
  }

  private static ColumnChunkMetaData pathChunk(BlockMetaData rowGroup) {
    List<ColumnChunkMetaData> chunks = rowGroup.getColumns().stream()
        .filter(chunk -> chunk.getPath().toDotString().equals("file_path"))
        .collect(Collectors.toList());
    Assert.assertEquals(1, chunks.size());
    return chunks.iterator().next();
  }
}
