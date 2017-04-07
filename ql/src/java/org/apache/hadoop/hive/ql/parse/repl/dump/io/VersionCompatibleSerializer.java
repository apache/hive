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

import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.IOException;

import static org.apache.hadoop.hive.ql.parse.EximUtil.METADATA_FORMAT_FORWARD_COMPATIBLE_VERSION;

/**
 * This is not used as of now as the conditional which lead to its usage is always false
 * hence we have removed the conditional and the usage of this class, but might be required in future.
 */
public class VersionCompatibleSerializer implements JsonWriter.Serializer {
  @Override
  public void writeTo(JsonWriter writer, ReplicationSpec additionalPropertiesProvider)
      throws SemanticException, IOException {
    writer.jsonGenerator.writeStringField("fcversion", METADATA_FORMAT_FORWARD_COMPATIBLE_VERSION);
  }
}
