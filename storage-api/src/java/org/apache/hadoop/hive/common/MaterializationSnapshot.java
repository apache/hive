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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.common.type.SnapshotContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.Map;

/**
 * Class to store snapshot data of Materialized view source tables.
 * The data represents the state of the source tables when the view was created/last rebuilt.
 */
public class MaterializationSnapshot {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializationSnapshot.class);

  public static MaterializationSnapshot fromJson(String jsonString) {
    try {
      return new ObjectMapper().readValue(jsonString, MaterializationSnapshot.class);
    } catch (JsonProcessingException e) {
      LOG.warn(String.format("Unable to parse string as json '%s'. Falling back to parse it as a ValidTxnWriteIdList",
              jsonString), e);
      return new MaterializationSnapshot(jsonString);
    }
  }

  // Snapshot of native ACID tables.
  private String validTxnList;
  // Snapshot of non-native ACID and insert-only transactional tables. Key is the fully qualified name of the table.
  // Value is the unique id of the snapshot provided by the table's storage HiveStorageHandler.
  private Map<String, SnapshotContext> tableSnapshots;

  /**
   * Constructor for json serialization
   */
  private MaterializationSnapshot() {
  }

  public MaterializationSnapshot(String validTxnList) {
    this.validTxnList = validTxnList;
    this.tableSnapshots = null;
  }

  public MaterializationSnapshot(Map<String, SnapshotContext> tableSnapshots) {
    this.validTxnList = null;
    this.tableSnapshots = tableSnapshots;
  }

  /**
   * Returns the json representation of this object.
   * @return {@link String} containing a json.
   */
  public String asJsonString() {
    try (Writer out = new StringWriter()) {
      new ObjectMapper().writeValue(out, this);
      return out.toString();
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to convert " + this + " to json", e);
    }
  }

  @Override
  public String toString() {
    return "MaterializationSnapshot{" +
            "validTxnList='" + validTxnList + '\'' +
            ", tableSnapshots=" + tableSnapshots +
            '}';
  }

  /**
   * Gets {@link ValidTxnWriteIdList} of the MV source tables when the MV was created/last rebuilt
   * @return {@link String} representation of {@link ValidTxnWriteIdList} or null if all source tables are non-native.
   */
  public String getValidTxnList() {
    return validTxnList;
  }

  /**
   * Gets the snapshotIds of the MV source tables when the MV was created/last rebuilt
   * @return {@link Map} of snapshotIds where the key is the fully qualified name of the table and the
   * values is the {@link String} representation of snapshotId or null if all tables are native ACID
   */
  public Map<String, SnapshotContext> getTableSnapshots() {
    return tableSnapshots;
  }
}
