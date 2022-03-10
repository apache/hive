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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * General facility to place a metadata file into a dir created by acid/compactor write.
 */
public class AcidMetaDataFile {
  //export command uses _metadata....
  public static final String METADATA_FILE = "_metadata_acid";
  public static final String CURRENT_VERSION = "0";

  public enum Field {
    VERSION("thisFileVersion"), DATA_FORMAT("dataFormat");

    private final String fieldName;

    Field(String fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String toString() {
      return fieldName;
    }
  }

  public enum DataFormat {
    // written by Major compaction
    COMPACTED,
    // written by truncate
    TRUNCATED,
    // written by drop partition
    DROPPED;

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  /**
   * Writes out an AcidMetaDataFile to the given location
   * @param fs FileSystem
   * @param basePath directory to write the file
   * @param format
   * @throws IOException
   */
  public static void writeToFile(FileSystem fs, Path basePath, DataFormat format) throws IOException {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(Field.VERSION.toString(), CURRENT_VERSION);
    metadata.put(Field.DATA_FORMAT.toString(), format.toString());
    String data = new ObjectMapper().writeValueAsString(metadata);
    try (FSDataOutputStream out = fs
        .create(new Path(basePath, METADATA_FILE)); OutputStreamWriter writer = new OutputStreamWriter(
        out, "UTF-8")) {
      writer.write(data);
      writer.flush();
    }
  }
}
