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

package org.apache.hadoop.hive.ql.anon.simple;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestIndexContext {

  private static final Logger LOG = LoggerFactory.getLogger(TestIndexContext.class.getName());
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void test1() throws IOException {
    Path path = Paths.get("./src/test/resources_anon/indexContext.json");
    final JsonNode node = mapper.readTree(path.toFile());
    Assertions.assertNotNull(node, "indexContext.json must parse to a non-null JSON node");
    Assertions.assertTrue(node.isContainerNode(),
        "indexContext.json must be a container (object) node");
    Assertions.assertTrue(node.size() > 0,
        "indexContext.json must have at least one field");
  }
}
