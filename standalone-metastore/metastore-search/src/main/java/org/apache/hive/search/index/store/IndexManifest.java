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

package org.apache.hive.search.index.store;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public record IndexManifest(String indexName, List<IndexFile> files, String embedder, long lastEventId) {
  public static final String MANIFEST_FILE_NAME = "index.json";
  /** Local-only staging copy of the remote manifest while a restore is in progress. */
  public static final String STAGING_MANIFEST_FILE_NAME = ".index.json";
  private static final ObjectMapper JSON = new ObjectMapper();

  public record IndexFile(String name, long size, Long crc32C) {
    public IndexFile {
      Objects.requireNonNull(name, "name");
    }

    public IndexFile(String name, long size) {
      this(name, size, null);
    }
  }

  public static IndexManifest create(String indexName, List<IndexFile> files,
      String embedder, long lastEventId) {
    return new IndexManifest(indexName, List.copyOf(files), embedder, lastEventId);
  }

  public byte[] toJsonBytes() throws IOException {
    return JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(this);
  }

  public static IndexManifest fromJson(byte[] bytes) throws IOException {
    return JSON.readValue(bytes, IndexManifest.class);
  }

  public boolean sameFilesAs(IndexManifest other) {
    return other != null && fileMap(files).equals(fileMap(other.files()));
  }

  public List<ChangedFileOp> diff(IndexManifest target) {
    Map<String, IndexFile> sourceMap = fileMap(files);
    Map<String, IndexFile> destMap = target == null ? Map.of() : fileMap(target.files());
    HashSet<String> keys = new HashSet<>();
    keys.addAll(sourceMap.keySet());
    keys.addAll(destMap.keySet());

    List<ChangedFileOp> ops = new ArrayList<>();
    for (String key : keys) {
      IndexFile source = sourceMap.get(key);
      IndexFile dest = destMap.get(key);
      if (source != null && dest != null && sameContent(source, dest)) {
        continue;
      }
      if (source != null) {
        ops.add(ChangedFileOp.add(key, source.size(), source.crc32C()));
      } else if (dest != null) {
        ops.add(ChangedFileOp.del(key));
      }
    }
    return ops;
  }

  private static boolean sameContent(IndexFile source, IndexFile dest) {
    if (source.size() != dest.size()) {
      return false;
    }
    Long sourceCrc = source.crc32C();
    Long destCrc = dest.crc32C();
    if (sourceCrc != null && destCrc != null) {
      return sourceCrc.equals(destCrc);
    }
    return sourceCrc == null && destCrc == null;
  }

  private static Map<String, IndexFile> fileMap(List<IndexFile> files) {
    HashMap<String, IndexFile> map = new HashMap<>();
    for (IndexFile file : files) {
      map.put(file.name(), file);
    }
    return map;
  }

  public interface ChangedFileOp {
    record Add(String fileName, Long size, Long crc32C) implements ChangedFileOp {}

    record Del(String fileName) implements ChangedFileOp {}

    static Add add(String fileName, Long size, Long crc32C) {
      return new Add(fileName, size, crc32C);
    }

    static Del del(String fileName) {
      return new Del(fileName);
    }
  }
}
