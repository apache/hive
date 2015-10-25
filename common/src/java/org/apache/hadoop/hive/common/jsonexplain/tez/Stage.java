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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public final class Stage {
  //external name is used to show at the console
  String externalName;
  //internal name is used to track the stages
  public final String internalName;
  //tezJsonParser
  public final TezJsonParser parser;
  // upstream stages, e.g., root stage
  public final List<Stage> parentStages = new ArrayList<>();
  // downstream stages.
  public final List<Stage> childStages = new ArrayList<>();
  public final Map<String, Vertex> vertexs =new LinkedHashMap<>();
  public final List<Attr> attrs = new ArrayList<>();
  Map<Vertex, List<Connection>> tezStageDependency;
  // some stage may contain only a single operator, e.g., create table operator,
  // fetch operator.
  Op op;

  public Stage(String name, TezJsonParser tezJsonParser) {
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
    if (object.has("Tez")) {
      this.tezStageDependency = new TreeMap<>();
      JSONObject tez = (JSONObject) object.get("Tez");
      JSONObject vertices = tez.getJSONObject("Vertices:");
      if (tez.has("Edges:")) {
        JSONObject edges = tez.getJSONObject("Edges:");
        // iterate for the first time to get all the vertices
        for (String to : JSONObject.getNames(edges)) {
          vertexs.put(to, new Vertex(to, vertices.getJSONObject(to), parser));
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
              parentVertex = new Vertex(parent, vertices.getJSONObject(parent), parser);
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
                parentVertex = new Vertex(parent, vertices.getJSONObject(parent), parser);
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
          vertexs.put(vertexName, new Vertex(vertexName, vertices.getJSONObject(vertexName), parser));
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
      if (names != null) {
        for (String name : names) {
          if (name.contains("Operator")) {
            this.op = extractOp(name, object.getJSONObject(name));
          } else {
            attrs.add(new Attr(name, object.get(name).toString()));
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
              JSONObject object = new JSONObject(new LinkedHashMap<>());
              object.put(name, attrObj);
              v = new Vertex(null, object, parser);
              v.extractOpTree();
            } else {
              for (String attrName : JSONObject.getNames(attrObj)) {
                attrs.add(new Attr(attrName, attrObj.get(attrName).toString()));
              }
            }
          }
        } else {
          throw new Exception("Unsupported object in " + this.internalName);
        }
      }
    }
    Op op = new Op(opName, null, null, null, attrs, null, v, parser);
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

  public void print(Printer printer, List<Boolean> indentFlag) throws Exception {
    // print stagename
    if (parser.printSet.contains(this)) {
      printer.println(TezJsonParser.prefixString(indentFlag) + " Please refer to the previous "
          + externalName);
      return;
    }
    parser.printSet.add(this);
    printer.println(TezJsonParser.prefixString(indentFlag) + externalName);
    // print vertexes
    List<Boolean> nextIndentFlag = new ArrayList<>();
    nextIndentFlag.addAll(indentFlag);
    nextIndentFlag.add(false);
    for (Vertex candidate : this.vertexs.values()) {
      if (!parser.isInline(candidate) && candidate.children.isEmpty()) {
        candidate.print(printer, nextIndentFlag, null, null);
      }
    }
    if (!attrs.isEmpty()) {
      Collections.sort(attrs);
      for (Attr attr : attrs) {
        printer.println(TezJsonParser.prefixString(nextIndentFlag) + attr.toString());
      }
    }
    if (op != null) {
      op.print(printer, nextIndentFlag, false);
    }
    nextIndentFlag.add(false);
    // print dependent stages
    for (Stage stage : this.parentStages) {
      stage.print(printer, nextIndentFlag);
    }
  }
}
