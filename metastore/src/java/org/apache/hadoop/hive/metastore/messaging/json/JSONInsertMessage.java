/**
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

package org.apache.hadoop.hive.metastore.messaging.json;

import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.codehaus.jackson.annotate.JsonProperty;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * JSON implementation of InsertMessage
 */
public class JSONInsertMessage extends InsertMessage {

  @JsonProperty
  String server, servicePrincipal, db, table;

  @JsonProperty
  Long timestamp;

  @JsonProperty
  String replace;

  @JsonProperty
  List<String> files;

  @JsonProperty
  Map<String, String> partKeyVals;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONInsertMessage() {
  }

  public JSONInsertMessage(String server, String servicePrincipal, String db, String table,
      Map<String, String> partKeyVals, boolean replace, Iterator<String> fileIter, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = db;
    this.table = table;
    this.timestamp = timestamp;
    this.replace = Boolean.toString(replace);
    this.partKeyVals = partKeyVals;
    this.files = Lists.newArrayList(fileIter);
    checkValid();
  }

  @Override
  public String getTable() {
    return table;
  }

  @Override
  public String getServer() {
    return server;
  }

  @Override
  public Map<String, String> getPartitionKeyValues() {
    return partKeyVals;
  }

  @Override
  public Iterable<String> getFiles() {
    return files;
  }

  @Override
  public String getServicePrincipal() {
    return servicePrincipal;
  }

  @Override
  public String getDB() {
    return db;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean isReplace() { return Boolean.parseBoolean(replace); }

  @Override
  public String toString() {
    try {
      return JSONMessageDeserializer.mapper.writeValueAsString(this);
    } catch (Exception exception) {
      throw new IllegalArgumentException("Could not serialize: ", exception);
    }
  }

}