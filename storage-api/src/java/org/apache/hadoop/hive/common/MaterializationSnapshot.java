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

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.Map;

public class MaterializationSnapshot {

  public static MaterializationSnapshot fromJson(String jsonString) {
    try {
      return new ObjectMapper().readValue(jsonString, MaterializationSnapshot.class);
    } catch (JsonProcessingException e) {
      // this is not a jsonString, fall back to treating it as ValidTxnWriteIdList
      return new MaterializationSnapshot(jsonString, null);
    }
  }

  private String validTxnList;
  private Map<String, String> tableSnapshots;

  private MaterializationSnapshot() {
  }

  public MaterializationSnapshot(String validTxnList, Map<String, String> tableSnapshots) {
    this.validTxnList = validTxnList;
    this.tableSnapshots = tableSnapshots;
  }

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

  public String getValidTxnList() {
    return validTxnList;
  }

  public Map<String, String> getTableSnapshots() {
    return tableSnapshots;
  }
}
