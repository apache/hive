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

package org.apache.hadoop.hive.common.jsonexplain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.common.jsonexplain.tez.TezJsonParser;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class TestOp {

  private ObjectMapper objectMapper = new ObjectMapper();
  private TezJsonParser tezJsonParser;

  @Before
  public void setUp() throws Exception {
    this.tezJsonParser = new TezJsonParser();
  }


  @Test
  public void testInlineJoinOpJsonHandling() throws Exception {
    String jsonString = "{" +
            "\"input vertices:\":{\"a\":\"AVERTEX\"}," + "\"condition map:\": [" +
            "{\"c1\": \"{\\\"type\\\": \\\"type\\\", \\\"left\\\": \\\"left\\\", " +
            "\\\"right\\\": \\\"right\\\"}\"}]," +
            "\"keys:\":{\"left\":\"AKEY\", \"right\":\"BKEY\"}}";
    JSONObject mapJoin = new JSONObject(jsonString);

    Vertex vertexB = new Vertex("vertex-b", null, null, tezJsonParser);
    Op dummyOp = new Op("Dummy Op", "dummy-id", "output-vertex-name", null, Collections.EMPTY_LIST,
            null, mapJoin, null, tezJsonParser);
    vertexB.outputOps.add(dummyOp);

    Vertex vertexC = new Vertex("vertex-c", null, null, tezJsonParser);
    vertexC.outputOps.add(dummyOp);


    Vertex vertexA = new Vertex("vertex-a", null, null, tezJsonParser);
    vertexA.tagToInput = new HashMap<>();
    vertexA.tagToInput.put("left", "vertex-b");
    vertexA.tagToInput.put("right", "vertex-c");
    vertexA.parentConnections.add(new Connection("left", vertexB));
    vertexA.parentConnections.add(new Connection("right", vertexC));


    Map<String, String> attrs = new HashMap<>();

    Op uut = new Op("Map Join Operator", "op-id", "output-vertex-name", null, Collections.EMPTY_LIST,
            attrs, mapJoin, vertexA, tezJsonParser);
    uut.inlineJoinOp();

    assertEquals(1, attrs.size());

    String result = attrs.get("Conds:");
    String expected = "dummy-id.AKEY=dummy-id.BKEY(type)";
    assertEquals(expected, result);
  }
}
