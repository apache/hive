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
package org.apache.hadoop.hive.ql.parse.repl.dump.io;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

import java.io.IOException;

public class DBSerializer implements JsonWriter.Serializer {
  public static final String FIELD_NAME = "db";
  private final Database dbObject;

  public DBSerializer(Database dbObject) {
    this.dbObject = dbObject;
  }

  @Override
  public void writeTo(JsonWriter writer, ReplicationSpec additionalPropertiesProvider)
      throws SemanticException, IOException {
    dbObject.putToParameters(
        ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString(),
        additionalPropertiesProvider.getCurrentReplicationState()
    );

    try {
      TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
      String value = serializer.toString(dbObject);
      writer.jsonGenerator.writeStringField(FIELD_NAME, value);
    } catch (TException e) {
      throw new SemanticException(ErrorMsg.ERROR_SERIALIZE_METASTORE.getMsg(), e);
    }
  }
}


