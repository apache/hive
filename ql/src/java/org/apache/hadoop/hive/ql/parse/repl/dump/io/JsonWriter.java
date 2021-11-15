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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import static org.apache.hadoop.hive.ql.parse.EximUtil.METADATA_FORMAT_VERSION;

public class JsonWriter implements Closeable {

  final JsonGenerator jsonGenerator;

  public JsonWriter(FileSystem fs, Path writePath) throws IOException {
    OutputStream out = fs.create(writePath);
    jsonGenerator = new JsonFactory().createGenerator(out);
    jsonGenerator.writeStartObject();
    jsonGenerator.writeStringField("version", METADATA_FORMAT_VERSION);
  }

  @Override
  public void close() throws IOException {
    jsonGenerator.writeEndObject();
    jsonGenerator.close();
  }

  public interface Serializer {
    String UTF_8 = "UTF-8";
    void writeTo(JsonWriter writer, ReplicationSpec additionalPropertiesProvider) throws
        SemanticException, IOException, MetaException;
  }
}
