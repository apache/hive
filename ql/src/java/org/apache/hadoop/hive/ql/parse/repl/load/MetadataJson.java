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
package org.apache.hadoop.hive.ql.parse.repl.load;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.DBSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FunctionSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.PartitionSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.TableSerializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.parse.repl.dump.io.JsonWriter.Serializer.UTF_8;

public class MetadataJson {
  private final JSONObject json;
  private final TDeserializer deserializer;
  private final String tableDesc;

  public MetadataJson(String message) throws JSONException, SemanticException {
    deserializer = new TDeserializer(new TJSONProtocol.Factory());
    json = new JSONObject(message);
    checkCompatibility();
    tableDesc = jsonEntry(TableSerializer.FIELD_NAME);
  }

  public MetaData getMetaData() throws TException, JSONException {
    return new MetaData(
        database(),
        table(),
        partitions(),
        readReplicationSpec(),
        function()
    );
  }

  private Function function() throws TException {
    return deserialize(new Function(), jsonEntry(FunctionSerializer.FIELD_NAME));
  }

  private Database database() throws TException {
    return deserialize(new Database(), jsonEntry(DBSerializer.FIELD_NAME));
  }

  private Table table() throws TException {
    return deserialize(new Table(), tableDesc);
  }

  private <T extends TBase> T deserialize(T intoObject, String json) throws TException {
    if (json == null) {
      return null;
    }
    deserializer.deserialize(intoObject, json, UTF_8);
    return intoObject;
  }

  private List<Partition> partitions() throws JSONException, TException {
    if (tableDesc == null) {
      return null;
    }
    // TODO : jackson-streaming-iterable-redo this
    JSONArray jsonPartitions = new JSONArray(json.getString(PartitionSerializer.FIELD_NAME));
    List<Partition> partitionsList = new ArrayList<>(jsonPartitions.length());
    for (int i = 0; i < jsonPartitions.length(); ++i) {
      String partDesc = jsonPartitions.getString(i);
      partitionsList.add(deserialize(new Partition(), partDesc));
    }
    return partitionsList;
  }

  private ReplicationSpec readReplicationSpec() {
    com.google.common.base.Function<String, String> keyFetcher =
        new com.google.common.base.Function<String, String>() {
          @Override
          public String apply(@Nullable String s) {
            return jsonEntry(s);
          }
        };
    return new ReplicationSpec(keyFetcher);
  }

  private void checkCompatibility() throws SemanticException, JSONException {
    String version = json.getString("version");
    String fcVersion = jsonEntry("fcversion");
    EximUtil.doCheckCompatibility(
        EximUtil.METADATA_FORMAT_VERSION,
        version,
        fcVersion);
  }

  private String jsonEntry(String forName) {
    try {
      return json.getString(forName);
    } catch (JSONException ignored) {
      return null;
    }
  }
}
