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

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Stage {
  String name;
  // upstream stages, e.g., root stage
  List<Stage> parentStages;
  // downstream stages.
  List<Stage> childStages;
  Map<String, Vertex> vertexs;
  List<Attr> attrs;
  LinkedHashMap<Vertex, List<Connection>> tezStageDependency;
  // some stage may contain only a single operator, e.g., create table operator,
  // fetch operator.
  Op op;

  public Stage(String name) {
    super();
    this.name = name;
    parentStages = new ArrayList<>();
    childStages = new ArrayList<>();
    attrs = new ArrayList<>();
    vertexs = new LinkedHashMap<>();
  }

  public void addDependency(JSONObject object, Map<String, Stage> stages) throws JSONException {
    if (!object.has("ROOT STAGE")) {
      String names = object.getString("DEPENDENT STAGES");
      for (String name : names.split(",")) {
        Stage parent = stages.get(name.trim());
        this.parentStages.add(parent);
        parent.childStages.add(this);
      }
    }
  }

  /**
   * @param object
   * @throws Exception
   *           If the object of stage contains "Tez", we need to extract the
   *           vertices and edges Else we need to directly extract operators
   *           and/or attributes.
   */
  public void extractVertex(JSONObject object) throws Exception {
    if (object.has("Tez")) {
      this.tezStageDependency = new LinkedHashMap<>();
      JSONObject tez = (JSONObject) object.get("Tez");
      JSONObject vertices = tez.getJSONObject("Vertices:");
      if (tez.has("Edges:")) {
        JSONObject edges = tez.getJSONObject("Edges:");
        // iterate for the first time to get all the vertices
        for (String to : JSONObject.getNames(edges)) {
          vertexs.put(to, new Vertex(to, vertices.getJSONObject(to)));
        }
        // iterate for the second time to get all the vertex dependency
        for (String to : JSONObject.getNames(edges)) {
          Object o = edges.get(to);
          Vertex v = vertexs.get(to);
          // 1 to 1 mapping
          if (o instanceof JSONObject) {
            JSONObject obj = (JSONObject) o;
            String parent = obj.getString("parent");
            Vertex parentVertex = vertexs.get(parent);
            if (parentVertex == null) {
              parentVertex = new Vertex(parent, vertices.getJSONObject(parent));
              vertexs.put(parent, parentVertex);
            }
            String type = obj.getString("type");
            // for union vertex, we reverse the dependency relationship
            if (!"CONTAINS".equals(type)) {
              v.addDependency(new Connection(type, parentVertex));
              parentVertex.children.add(v);
            } else {
              parentVertex.addDependency(new Connection(type, v));
              v.children.add(parentVertex);
            }
            this.tezStageDependency.put(v, Arrays.asList(new Connection(type, parentVertex)));
          } else {
            // 1 to many mapping
            JSONArray from = (JSONArray) o;
            List<Connection> list = new ArrayList<>();
            for (int index = 0; index < from.length(); index++) {
              JSONObject obj = from.getJSONObject(index);
              String parent = obj.getString("parent");
              Vertex parentVertex = vertexs.get(parent);
              if (parentVertex == null) {
                parentVertex = new Vertex(parent, vertices.getJSONObject(parent));
                vertexs.put(parent, parentVertex);
              }
              String type = obj.getString("type");
              if (!"CONTAINS".equals(type)) {
                v.addDependency(new Connection(type, parentVertex));
                parentVertex.children.add(v);
              } else {
                parentVertex.addDependency(new Connection(type, v));
                v.children.add(parentVertex);
              }
              list.add(new Connection(type, parentVertex));
            }
            this.tezStageDependency.put(v, list);
          }
        }
      } else {
        for (String vertexName : JSONObject.getNames(vertices)) {
          vertexs.put(vertexName, new Vertex(vertexName, vertices.getJSONObject(vertexName)));
        }
      }
      // The opTree in vertex is extracted
      for (Vertex v : vertexs.values()) {
        if (!v.union) {
          v.extractOpTree();
          v.checkMultiReduceOperator();
        }
      }
    } else {
      String[] names = JSONObject.getNames(object);
      for (String name : names) {
        if (name.contains("Operator")) {
          this.op = extractOp(name, object.getJSONObject(name));
        } else {
          attrs.add(new Attr(name, object.get(name).toString()));
        }
      }
    }
  }

  /**
   * @param opName
   * @param opObj
   * @return
   * @throws JSONException
   * @throws JsonParseException
   * @throws JsonMappingException
   * @throws IOException
   * @throws Exception
   *           This method address the create table operator, fetch operator,
   *           etc
   */
  Op extractOp(String opName, JSONObject opObj) throws JSONException, JsonParseException,
      JsonMappingException, IOException, Exception {
    List<Attr> attrs = new ArrayList<>();
    Vertex v = null;
    if (opObj.length() > 0) {
      String[] names = JSONObject.getNames(opObj);
      for (String name : names) {
        Object o = opObj.get(name);
        if (isPrintable(o)) {
          attrs.add(new Attr(name, o.toString()));
        } else if (o instanceof JSONObject) {
          JSONObject attrObj = (JSONObject) o;
          if (attrObj.length() > 0) {
            if (name.equals("Processor Tree:")) {
              JSONObject object = new JSONObject();
              object.put(name, attrObj);
              v = new Vertex(null, object);
              v.extractOpTree();
            } else {
              for (String attrName : JSONObject.getNames(attrObj)) {
                attrs.add(new Attr(attrName, attrObj.get(attrName).toString()));
              }
            }
          }
        } else {
          throw new Exception("Unsupported object in " + this.name);
        }
      }
    }
    Op op = new Op(opName, null, null, null, attrs, null, v);
    if (v != null) {
      TezJsonParser.addInline(op, new Connection(null, v));
    }
    return op;
  }

  private boolean isPrintable(Object val) {
    if (val instanceof Boolean || val instanceof String || val instanceof Integer
        || val instanceof Long || val instanceof Byte || val instanceof Float
        || val instanceof Double || val instanceof Path) {
      return true;
    }
    if (val != null && val.getClass().isPrimitive()) {
      return true;
    }
    return false;
  }

  public void print(PrintStream out, List<Boolean> indentFlag) throws JSONException, Exception {
    // print stagename
    if (TezJsonParser.printSet.contains(this)) {
      out.println(TezJsonParser.prefixString(indentFlag) + " Please refer to the previous "
          + this.name);
      return;
    }
    TezJsonParser.printSet.add(this);
    out.println(TezJsonParser.prefixString(indentFlag) + this.name);
    // print vertexes
    List<Boolean> nextIndentFlag = new ArrayList<>();
    nextIndentFlag.addAll(indentFlag);
    nextIndentFlag.add(false);
    for (Vertex candidate : this.vertexs.values()) {
      if (!TezJsonParser.isInline(candidate) && candidate.children.isEmpty()) {
        candidate.print(out, nextIndentFlag, null, null);
      }
    }
    if (!attrs.isEmpty()) {
      Collections.sort(attrs);
      for (Attr attr : attrs) {
        out.println(TezJsonParser.prefixString(nextIndentFlag) + attr.toString());
      }
    }
    if (op != null) {
      op.print(out, nextIndentFlag, false);
    }
    nextIndentFlag.add(false);
    // print dependent stages
    for (Stage stage : this.parentStages) {
      stage.print(out, nextIndentFlag);
    }
  }
}
