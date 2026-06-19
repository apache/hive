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

package org.apache.hadoop.hive.ql.optimizer.signature;

import java.io.IOException;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Enables to encode/decode runtime statistics values into textual form.
 */
public class RuntimeStatsPersister {
  public static final RuntimeStatsPersister INSTANCE = new RuntimeStatsPersister();

  private final ObjectMapper om;

  RuntimeStatsPersister() {
    om = new ObjectMapper();
    om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
    om.configure(SerializationFeature.INDENT_OUTPUT, true);
    om.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
  }

  public <T> String encode(T input) throws IOException {
    return om.writeValueAsString(input);
  }

  public <T> T decode(String input, Class<T> clazz) throws IOException {
    return om.readValue(input, clazz);
  }

  public <T> T decode(byte[] input, Class<T> clazz) throws IOException {
    return om.readValue(input, clazz);
  }

}
