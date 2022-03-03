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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestStage {

  private Stage uut;
  private Stage stageA;
  private Stage stageB;
  private TezJsonParser tezJsonParser;

  @Before
  public void setUp() {
    this.tezJsonParser = new TezJsonParser();
    this.uut = new Stage("uut", tezJsonParser);
    this.stageA = new Stage("stage-a", tezJsonParser);
    this.stageB = new Stage("stage-b", tezJsonParser);
  }

  @Test
  public void testAddDependencyNonRoot() throws Exception {
    Map<String, Stage> children = new LinkedHashMap<>();
    children.put("a", stageA);
    children.put("b", stageB);


    String jsonString = "{\"DEPENDENT STAGES\":\"a,b\"}";
    JSONObject names = new JSONObject(jsonString);

    uut.addDependency(names, children);

    assertEquals(2, uut.parentStages.size());
    assertEquals(stageA, uut.parentStages.get(0));
    assertEquals(stageB, uut.parentStages.get(1));

    assertEquals(1, stageA.childStages.size());
    assertEquals(uut, stageA.childStages.get(0));

    assertEquals(1, stageB.childStages.size());
    assertEquals(uut, stageB.childStages.get(0));
  }

  @Test
  public void testAddDependencyRoot() throws Exception {
    Map<String, Stage> children = new LinkedHashMap<>();
    children.put("a", stageA);
    children.put("b", stageB);

    String jsonString = "{\"ROOT STAGE\":\"X\",\"DEPENDENT STAGES\":\"a,b\"}";
    JSONObject names = new JSONObject(jsonString);

    uut.addDependency(names, children);

    assertEquals(2, uut.parentStages.size());
    assertEquals(1, stageA.childStages.size());
    assertEquals(1, stageB.childStages.size());
  }


  @Test
  public void testExtractVertexNonTez() throws Exception {
    String jsonString = "{\"OperatorName\":{\"a\":\"A\",\"b\":\"B\"}," +
            "\"attr1\":\"ATTR1\"}";
    JSONObject object = new JSONObject(jsonString);

    uut.extractVertex(object);

    assertEquals("OperatorName", uut.op.name);
    assertEquals(1, uut.attrs.size());
    assertEquals("ATTR1", uut.attrs.get("attr1"));
  }

  @Test
  public void testExtractVertexTezNoEdges() throws Exception {
    String jsonString = "{\"Tez\":{\"a\":\"A\",\"Vertices:\":{\"v1\":{}}}}";
    JSONObject object = new JSONObject(jsonString);
    uut.extractVertex(object);

    assertEquals(1, uut.vertices.size());
    assertTrue(uut.vertices.containsKey("v1"));
  }

  @Test
  public void testExtractVertexTezWithOneEdge() throws Exception {
    String jsonString = "{\"Tez\":{\"a\":\"A\"," +
            "\"Vertices:\":{\"v1\":{},\"v2\":{}}," +
            "\"Edges:\":{\"v2\":{\"parent\":\"v1\",\"type\":\"TYPE\"}}}}";
    JSONObject object = new JSONObject(jsonString);
    uut.extractVertex(object);

    assertEquals(2, uut.vertices.size());
    assertTrue(uut.vertices.containsKey("v1"));
    assertTrue(uut.vertices.containsKey("v2"));

    assertEquals(0, uut.vertices.get("v1").parentConnections.size());
    assertEquals(1, uut.vertices.get("v2").parentConnections.size());
    assertEquals("v1", uut.vertices.get("v2").parentConnections.get(0).from.name);
    assertEquals("TYPE", uut.vertices.get("v2").parentConnections.get(0).type);

  }


  @Test
  public void testExtractVertexTezWithOneToManyEdge() throws Exception {
    String jsonString = "{\"Tez\":{\"a\":\"A\"," +
            "\"Vertices:\":{\"v1\":{},\"v2\":{},\"v3\":{}}," +
            "\"Edges:\":{\"v1\":[{\"parent\":\"v2\",\"type\":\"TYPE1\"}," +
            "{\"parent\":\"v3\",\"type\":\"TYPE2\"}]}}}";
    JSONObject object = new JSONObject(jsonString);

    uut.extractVertex(object);

    assertEquals(3, uut.vertices.size());
    assertTrue(uut.vertices.containsKey("v1"));
    assertTrue(uut.vertices.containsKey("v2"));
    assertTrue(uut.vertices.containsKey("v3"));

    assertEquals(2, uut.vertices.get("v1").parentConnections.size());
    assertEquals(1, uut.vertices.get("v2").children.size());
    assertEquals(1, uut.vertices.get("v3").children.size());
    assertEquals("v1", uut.vertices.get("v2").children.get(0).name);
    assertEquals("v1", uut.vertices.get("v3").children.get(0).name);
    assertEquals("TYPE1", uut.vertices.get("v1").parentConnections.get(0).type);
    assertEquals("TYPE2", uut.vertices.get("v1").parentConnections.get(1).type);

  }

  @Test
  public void testExtractOpEmptyObject() throws Exception {
    JSONObject object = new JSONObject();
    Op result = uut.extractOp("op-name", object);

    assertEquals("op-name", result.name);
    assertEquals(0, result.attrs.size());
    assertNull(result.vertex);
  }

  @Test
  public void testExtractOpSimple() throws Exception {
    String jsonString = "{\"a\":\"A\",\"b\":\"B\"}";
    JSONObject object = new JSONObject(jsonString);

    Op result = uut.extractOp("op-name", object);

    assertEquals("op-name", result.name);
    assertEquals(2, result.attrs.size());
    assertNull(result.vertex);
  }

  @Test
  public void testExtract() throws Exception {
    String jsonString = "{\"b\":{\"b2\":\"B2\",\"b1\":\"B1\"}," +
            "\"Processor Tree:\":{\"a1\":{\"t1\":\"T1\"}}}";
    JSONObject object = new JSONObject(jsonString);

    Op result = uut.extractOp("op-name", object);
    assertEquals("op-name", result.name);
    assertEquals(2, result.attrs.size());

    List<String> attrs = new ArrayList<>();
    assertEquals("B1", result.attrs.get("b1"));
    assertEquals("B2", result.attrs.get("b2"));
    assertNotNull(result.vertex);
  }

}
