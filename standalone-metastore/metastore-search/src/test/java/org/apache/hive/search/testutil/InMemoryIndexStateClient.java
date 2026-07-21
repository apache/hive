/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.testutil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.CRC32;

import org.apache.hive.search.exception.IndexIOException;
import org.apache.hive.search.index.store.IndexManifest;
import org.apache.hive.search.index.store.IndexStateClient;

/** In-memory {@link IndexStateClient} for unit tests. */
public final class InMemoryIndexStateClient implements IndexStateClient {
  private final Map<String, byte[]> files = new HashMap<>();
  private byte[] stagingManifest;

  @Override
  public Optional<IndexManifest> readManifest() throws IOException {
    byte[] bytes = files.get(IndexManifest.MANIFEST_FILE_NAME);
    return bytes == null ? Optional.empty() : Optional.of(IndexManifest.fromJson(bytes));
  }

  @Override
  public InputStream read(String fileName) throws IOException {
    byte[] bytes = files.get(fileName);
    if (bytes == null) {
      throw new IndexIOException("missing file: " + fileName);
    }
    return new ByteArrayInputStream(bytes);
  }

  @Override
  public void write(String fileName, InputStream stream) throws IOException {
    files.put(fileName, stream.readAllBytes());
  }

  @Override
  public void delete(String fileName) throws IOException {
    files.remove(fileName);
  }

  @Override
  public Optional<IndexManifest> readStagingManifest() throws IOException {
    return stagingManifest == null ? Optional.empty() : Optional.of(IndexManifest.fromJson(stagingManifest));
  }

  @Override
  public void writeStagingManifest(IndexManifest manifest) throws IOException {
    stagingManifest = manifest.toJsonBytes();
  }

  @Override
  public void clearStagingManifest() {
    stagingManifest = null;
  }

  @Override
  public void clear() {
    files.clear();
    stagingManifest = null;
  }

  @Override
  public IndexManifest readLocalFileManifest() throws IOException {
    List<IndexManifest.IndexFile> indexFiles = new ArrayList<>();
    for (Map.Entry<String, byte[]> entry : files.entrySet()) {
      if (IndexManifest.MANIFEST_FILE_NAME.equals(entry.getKey())) {
        continue;
      }
      byte[] bytes = entry.getValue();
      indexFiles.add(new IndexManifest.IndexFile(
          entry.getKey(), bytes.length, crc32C(bytes)));
    }
    return IndexManifest.create("test", indexFiles, "", -1);
  }

  public static long crc32C(byte[] bytes) {
    CRC32 crc = new CRC32();
    crc.update(bytes);
    return crc.getValue();
  }

  public boolean hasFile(String fileName) {
    return files.containsKey(fileName);
  }
}
