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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.common.jsonexplain.tez;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestOp {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testInlineJoinOpJsonShouldMatch() throws Exception {
    String jsonString = "{\"Map Join Operator\":{" +
            "\"input vertices:\":{\"a\":\"AVERTEX\"}," +
            "\"keys:\":{\"a\":\"AKEY\",\"b\":\"BKEY\"}}}";
    JSONObject mapJoin = new JSONObject(jsonString);

    Vertex vertex = new Vertex("vertex-name", null);

    List<Attr> attrs = new ArrayList<>();

    Op uut = new Op("Map Join Operator", "op-id", "output-vertex-name", Collections.EMPTY_LIST,
            attrs, mapJoin, vertex);
    uut.inlineJoinOp();

    assertEquals(1, attrs.size());

    JsonNode result = objectMapper.readTree(attrs.get(0).value);
    JsonNode expected = objectMapper.readTree("{\"vertex-name\":\"BKEY\",\"AVERTEX\":\"AKEY\"}");

    assertEquals(expected, result);
  }
}