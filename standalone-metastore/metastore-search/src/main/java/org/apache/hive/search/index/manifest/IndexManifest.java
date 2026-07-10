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

package org.apache.hive.search.index.manifest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record IndexManifest(String indexName, List<IndexFile> files, String modelName, long lastEventId) {
  public static final String MANIFEST_FILE_NAME = "index.json";
  /** Local-only staging copy of the remote manifest while a restore is in progress. */
  public static final String STAGING_MANIFEST_FILE_NAME = ".index.json";
  private static final ObjectMapper JSON = new ObjectMapper();

  public record IndexFile(String name, long size) {}

  public static IndexManifest create(String indexName, List<IndexFile> files,
      String modelName, long lastEventId) {
    return new IndexManifest(indexName, List.copyOf(files), modelName, lastEventId);
  }

  public byte[] toJsonBytes() throws IOException {
    return JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(this);
  }

  public static IndexManifest fromJson(byte[] bytes) throws IOException {
    return JSON.readValue(bytes, IndexManifest.class);
  }

  public boolean sameFilesAs(IndexManifest other) {
    return other != null && toMap(files).equals(toMap(other.files()));
  }

  public List<ChangedFileOp> diff(IndexManifest target) {
    Map<String, Long> sourceMap = toMap(files);
    Map<String, Long> destMap = target == null ? Map.of() : toMap(target.files());
    HashSet<String> keys = new HashSet<>();
    keys.addAll(sourceMap.keySet());
    keys.addAll(destMap.keySet());

    List<ChangedFileOp> ops = new ArrayList<>();
    for (String key : keys) {
      Long sourceSize = sourceMap.get(key);
      Long destSize = destMap.get(key);
      if (sourceSize != null && sourceSize.equals(destSize)) {
        continue;
      }
      if (sourceSize != null) {
        ops.add(ChangedFileOp.add(key, sourceSize));
      } else if (destSize != null) {
        ops.add(ChangedFileOp.del(key));
      }
    }
    return ops;
  }

  private static Map<String, Long> toMap(List<IndexFile> files) {
    HashMap<String, Long> map = new HashMap<>();
    for (IndexFile file : files) {
      map.put(file.name(), file.size());
    }
    return map;
  }

  public interface ChangedFileOp {
    record Add(String fileName, Long size) implements ChangedFileOp {}

    record Del(String fileName) implements ChangedFileOp {}

    static Add add(String fileName, Long size) {
      return new Add(fileName, size);
    }

    static Del del(String fileName) {
      return new Del(fileName);
    }
  }
}
