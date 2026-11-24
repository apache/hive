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

package org.apache.iceberg.hive;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMetadataCachingFileIO {

  private static final String FIRST = "written first";
  private static final String AGAIN = "written again";

  @TempDir
  private Path temp;

  @Test
  public void aTablesMetadataIsReadOncePerProcess() throws Exception {
    for (String name : List.of("00001-6e1f.metadata.json", "00002-6e1f.gz.metadata.json", "snap-1-1-6e1f.avro",
        "6e1f-m0.avro", "1-6e1f.stats", "column-stats-1-6e1f.puffin", "partition-stats-1-6e1f.parquet")) {
      String location = write("tbl/metadata/" + name);
      assertThat(read(cachingFileIO().newInputFile(location))).isEqualTo(FIRST);
      rewrite(location);
      // the next query reads through a FileIO of its own, with the length or without it
      assertThat(read(cachingFileIO().newInputFile(location))).as(name).isEqualTo(FIRST);
      assertThat(read(cachingFileIO().newInputFile(location, FIRST.length()))).as(name).isEqualTo(FIRST);
    }
  }

  @Test
  public void dataAndDeleteFilesAreReadAsTheyAre() throws Exception {
    assertNotCached("tbl/data/00000-0-6e1f-00001.parquet", "tbl/data/00000-0-6e1f-00002.avro",
        "tbl/data/00000-0-6e1f-00003.puffin", "metadata/data/00000-0-6e1f-00001.parquet");
  }

  @Test
  public void filesWhoseNamesComeBackAreReadAsTheyAre() throws Exception {
    assertNotCached("tbl/metadata/v3.metadata.json", "tbl/metadata/v4.gz.metadata.json",
        "tbl/metadata/version-hint.text");
  }

  @Test
  public void aDeserializedFileIOReadsFromTheSameCache() throws Exception {
    MetadataCachingFileIO io = cachingFileIO();
    String location = write("tbl/metadata/00001-6e1f.metadata.json");
    assertThat(read(io.newInputFile(location))).isEqualTo(FIRST);
    rewrite(location);

    FileIO copy = roundTrip(io);
    assertThat(copy).isInstanceOf(MetadataCachingFileIO.class);
    assertThat(read(copy.newInputFile(location))).isEqualTo(FIRST);
  }

  @Test
  public void icebergDoesNotCacheTheSameFilesAgain() {
    MetadataCachingFileIO io = new MetadataCachingFileIO(new Configuration());
    io.initialize(Map.of(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true",
        CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES, "1000"));
    assertThat(io.properties()).doesNotContainKey(CatalogProperties.IO_MANIFEST_CACHE_ENABLED)
        .containsEntry(CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES, "1000");
  }

  @Test
  public void entriesOutliveTheGapBetweenQueriesUnlessTheCatalogSaysOtherwise() {
    assertThat(MetadataCachingFileIO.bounds(Map.of())).containsExactly(TimeUnit.HOURS.toMillis(1),
        CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT,
        CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT);
    assertThat(MetadataCachingFileIO.bounds(Map.of(
        CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS, "60000",
        CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES, "1000",
        CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH, "10"))).containsExactly(60000L, 1000L, 10L);
  }

  @Test
  public void aFileOverTheCatalogsPerFileBoundIsReadAsItIs() throws Exception {
    Map<String, String> properties = Map.of(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true",
        CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH, String.valueOf(FIRST.length() - 1));
    String location = write("tbl/metadata/00001-6e1f.metadata.json");
    assertThat(read(cachingFileIO(properties).newInputFile(location))).isEqualTo(FIRST);
    rewrite(location);
    assertThat(read(cachingFileIO(properties).newInputFile(location))).isEqualTo(AGAIN);
  }

  private void assertNotCached(String... paths) throws IOException {
    for (String path : paths) {
      String location = write(path);
      assertThat(read(cachingFileIO().newInputFile(location))).isEqualTo(FIRST);
      rewrite(location);
      assertThat(read(cachingFileIO().newInputFile(location))).as(path).isEqualTo(AGAIN);
      assertThat(read(cachingFileIO().newInputFile(location, AGAIN.length()))).as(path).isEqualTo(AGAIN);
    }
  }

  private static MetadataCachingFileIO cachingFileIO() {
    return cachingFileIO(Map.of(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true"));
  }

  private static MetadataCachingFileIO cachingFileIO(Map<String, String> properties) {
    MetadataCachingFileIO io = new MetadataCachingFileIO(new Configuration());
    io.initialize(properties);
    return io;
  }

  private String write(String path) throws IOException {
    Path file = temp.resolve(path);
    Files.createDirectories(file.getParent());
    Files.writeString(file, FIRST);
    return file.toString();
  }

  /** The same length as before: only the bytes tell a read of the file from a read of the cache. */
  private static void rewrite(String location) throws IOException {
    Files.writeString(Paths.get(location), AGAIN);
  }

  private static String read(InputFile file) throws IOException {
    try (SeekableInputStream in = file.newStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /** As a table's FileIO travels to the Tez AM and the tasks: Java serialization. */
  private static FileIO roundTrip(FileIO io) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(io);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (FileIO) in.readObject();
    }
  }
}
