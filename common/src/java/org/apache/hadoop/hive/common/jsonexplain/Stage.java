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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.jsonexplain.Vertex.VertexType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public final class Stage {
  //external name is used to show at the console
  String externalName;
  //internal name is used to track the stages
  public final String internalName;
  //tezJsonParser
  public final DagJsonParser parser;
  // upstream stages, e.g., root stage
  public final List<Stage> parentStages = new ArrayList<>();
  // downstream stages.
  public final List<Stage> childStages = new ArrayList<>();
  public final Map<String, Vertex> vertexs =new LinkedHashMap<>();
  public final Map<String, String> attrs = new TreeMap<>();
  Map<Vertex, List<Connection>> tezStageDependency;
  // some stage may contain only a single operator, e.g., create table operator,
  // fetch operator.
  Op op;

  public Stage(String name, DagJsonParser tezJsonParser) {
    super();
    internalName = name;
    externalName = name;
    parser = tezJsonParser;
  }

  public void addDependency(JSONObject object, Map<String, Stage> stages) throws JSONException {
    if (object.has("DEPENDENT STAGES")) {
      String names = object.getString("DEPENDENT STAGES");
      for (String name : names.split(",")) {
        Stage parent = stages.get(name.trim());
        this.parentStages.add(parent);
        parent.childStages.add(this);
      }
    }
    if (object.has("CONDITIONAL CHILD TASKS")) {
      String names = object.getString("CONDITIONAL CHILD TASKS");
      this.externalName = this.internalName + "(CONDITIONAL CHILD TASKS: " + names + ")";
      for (String name : names.split(",")) {
        Stage child = stages.get(name.trim());
        child.externalName = child.internalName + "(CONDITIONAL)";
        child.parentStages.add(this);
        this.childStages.add(child);
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
    if (object.has(this.parser.getFrameworkName())) {
      this.tezStageDependency = new TreeMap<>();
      JSONObject tez = (JSONObject) object.get(this.parser.getFrameworkName());
      JSONObject vertices = tez.getJSONObject("Vertices:");
      if (tez.has("Edges:")) {
        JSONObject edges = tez.getJSONObject("Edges:");
        // iterate for the first time to get all the vertices
        for (String to : JSONObject.getNames(edges)) {
          vertexs.put(to, new Vertex(to, vertices.getJSONObject(to), this, parser));
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
              parentVertex = new Vertex(parent, vertices.getJSONObject(parent), this, parser);
              vertexs.put(parent, parentVertex);
            }
            String type = obj.getString("type");
            // for union vertex, we reverse the dependency relationship
            if (!"CONTAINS".equals(type)) {
              v.addDependency(new Connection(type, parentVertex));
              parentVertex.setType(type);
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
                parentVertex = new Vertex(parent, vertices.getJSONObject(parent), this, parser);
                vertexs.put(parent, parentVertex);
              }
              String type = obj.getString("type");
              if (!"CONTAINS".equals(type)) {
                v.addDependency(new Connection(type, parentVertex));
                parentVertex.setType(type);
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
          vertexs.put(vertexName, new Vertex(vertexName, vertices.getJSONObject(vertexName), this, parser));
        }
      }

      // iterate for the first time to extract opTree in vertex
      for (Vertex v : vertexs.values()) {
        if (v.vertexType == VertexType.MAP || v.vertexType == VertexType.REDUCE) {
          v.extractOpTree();
        }
      }
      // iterate for the second time to rewrite object
      for (Vertex v : vertexs.values()) {
        v.checkMultiReduceOperator(parser.rewriteObject);
      }
    } else {
      String[] names = JSONObject.getNames(object);
      if (names != null) {
        for (String name : names) {
          if (name.contains("Operator")) {
            this.op = extractOp(name, object.getJSONObject(name));
          } else {
            if (!object.get(name).toString().isEmpty()) {
              attrs.put(name, object.get(name).toString());
            }
          }
        }
      }
    }
  }

  /**
   * @param opName
   * @param opObj
   * @return
   * @throws Exception
   *           This method address the create table operator, fetch operator,
   *           etc
   */
  Op extractOp(String opName, JSONObject opObj) throws Exception {
    Map<String, String> attrs = new TreeMap<>();
    Vertex v = null;
    if (opObj.length() > 0) {
      String[] names = JSONObject.getNames(opObj);
      for (String name : names) {
        Object o = opObj.get(name);
        if (isPrintable(o) && !o.toString().isEmpty()) {
          attrs.put(name, o.toString());
        } else if (o instanceof JSONObject) {
          JSONObject attrObj = (JSONObject) o;
          if (attrObj.length() > 0) {
            if (name.equals("Processor Tree:")) {
              JSONObject object = new JSONObject(new LinkedHashMap<>());
              object.put(name, attrObj);
              v = new Vertex(null, object, this, parser);
              v.extractOpTree();
            } else {
              for (String attrName : JSONObject.getNames(attrObj)) {
                if (!attrObj.get(attrName).toString().isEmpty()) {
                  attrs.put(attrName, attrObj.get(attrName).toString());
                }
              }
            }
          }
        } else {
          throw new Exception("Unsupported object in " + this.internalName);
        }
      }
    }
    Op op = new Op(opName, null, null, null, null, attrs, null, v, parser);
    if (v != null) {
      parser.addInline(op, new Connection(null, v));
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

  public void print(Printer printer, int indentFlag) throws Exception {
    // print stagename
    if (parser.printSet.contains(this)) {
      printer.println(DagJsonParser.prefixString(indentFlag) + " Please refer to the previous "
          + externalName);
      return;
    }
    parser.printSet.add(this);
    printer.println(DagJsonParser.prefixString(indentFlag) + externalName);
    // print vertexes
    indentFlag++;
    for (Vertex candidate : this.vertexs.values()) {
      if (!parser.isInline(candidate) && candidate.children.isEmpty()) {
        candidate.print(printer, indentFlag, null, null);
      }
    }
    if (!attrs.isEmpty()) {
      printer.println(DagJsonParser.prefixString(indentFlag)
          + DagJsonParserUtils.attrsToString(attrs));
    }
    if (op != null) {
      op.print(printer, indentFlag, false);
    }
    indentFlag++;
    // print dependent stages
    for (Stage stage : this.parentStages) {
      stage.print(printer, indentFlag);
    }
  }
}
