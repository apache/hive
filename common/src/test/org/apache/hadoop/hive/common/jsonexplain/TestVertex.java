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

import org.apache.hadoop.hive.common.jsonexplain.tez.TezJsonParser;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestVertex {

  private TezJsonParser tezJsonParser;

  @Before
  public void setUp() throws Exception {
    this.tezJsonParser = new TezJsonParser();
  }


  @Test
  public void testExtractOpTree() throws Exception {
    JSONObject object = new JSONObject("{\"Join:\":[{},{}]}");

    Vertex uut = new Vertex("name", object, null, tezJsonParser);
    uut.extractOpTree();

    assertEquals(2, uut.mergeJoinDummyVertexs.size());
  }

  @Test
  public void testExtractOpNonJsonChildrenShouldThrow() throws Exception {
    String jsonString = "{\"opName\":{\"children\":\"not-json\"}}";
    JSONObject operator = new JSONObject(jsonString);

    Vertex uut = new Vertex("name", null, null, tezJsonParser);

    try {
      uut.extractOp(operator, null);
    } catch (Exception e) {
      assertEquals("Unsupported operator name's children operator is neither a jsonobject nor a jsonarray", e.getMessage());
    }
  }

  @Test
  public void testExtractOpNoChildrenOperatorId() throws Exception {
    String jsonString = "{\"opName\":{\"OperatorId:\":\"operator-id\"}}";
    JSONObject operator = new JSONObject(jsonString);

    Vertex uut = new Vertex("name", null, null, tezJsonParser);

    Op result = uut.extractOp(operator, null);
    assertEquals("opName", result.name);
    assertEquals("operator-id", result.operatorId);
    assertEquals(0, result.children.size());
    assertEquals(0, result.attrs.size());
  }

  @Test
  public void testExtractOpOneChild() throws Exception {
    String jsonString = "{\"opName\":{\"children\":{\"childName\":" +
            "{\"OperatorId:\":\"child-operator-id\"}}}}";
    JSONObject operator = new JSONObject(jsonString);

    Vertex uut = new Vertex("name", null, null, tezJsonParser);

    Op result = uut.extractOp(operator, null);
    assertEquals("opName", result.name);
    assertEquals(1, result.children.size());
    assertEquals("childName", result.children.get(0).name);
    assertEquals("child-operator-id", result.children.get(0).operatorId);
  }

  @Test
  public void testExtractOpMultipleChildren() throws Exception {
    String jsonString = "{\"opName\":{\"children\":[" +
            "{\"childName1\":{\"OperatorId:\":\"child-operator-id1\"}}," +
            "{\"childName2\":{\"OperatorId:\":\"child-operator-id2\"}}]}}";
    JSONObject operator = new JSONObject(jsonString);

    Vertex uut = new Vertex("name", null, null, tezJsonParser);

    Op result = uut.extractOp(operator, null);
    assertEquals("opName", result.name);
    assertEquals(2, result.children.size());
    assertEquals("childName1", result.children.get(0).name);
    assertEquals("child-operator-id1", result.children.get(0).operatorId);
    assertEquals("childName2", result.children.get(1).name);
    assertEquals("child-operator-id2", result.children.get(1).operatorId);
  }
}
