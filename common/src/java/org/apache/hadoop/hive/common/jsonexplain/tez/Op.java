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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public final class Op {
  public final String name;
  //tezJsonParser
  public final TezJsonParser parser;
  public final String operatorId;
  public Op parent;
  public final List<Op> children;
  public final List<Attr> attrs;
  // the jsonObject for this operator
  public final JSONObject opObject;
  // the vertex that this operator belongs to
  public final Vertex vertex;
  // the vertex that this operator output to if this operator is a
  // ReduceOutputOperator
  public final String outputVertexName;

  public Op(String name, String id, String outputVertexName, List<Op> children, List<Attr> attrs,
      JSONObject opObject, Vertex vertex, TezJsonParser tezJsonParser) throws JSONException {
    super();
    this.name = name;
    this.operatorId = id;
    this.outputVertexName = outputVertexName;
    this.children = children;
    this.attrs = attrs;
    this.opObject = opObject;
    this.vertex = vertex;
    this.parser = tezJsonParser;
  }

  private void inlineJoinOp() throws Exception {
    // inline map join operator
    if (this.name.equals("Map Join Operator")) {
      JSONObject mapjoinObj = opObject.getJSONObject("Map Join Operator");
      // get the map for posToVertex
      JSONObject verticeObj = mapjoinObj.getJSONObject("input vertices:");
      Map<String, String> posToVertex = new HashMap<>();
      for (String pos : JSONObject.getNames(verticeObj)) {
        String vertexName = verticeObj.getString(pos);
        posToVertex.put(pos, vertexName);
        // update the connection
        Connection c = null;
        for (Connection connection : vertex.parentConnections) {
          if (connection.from.name.equals(vertexName)) {
            c = connection;
            break;
          }
        }
        if (c != null) {
          parser.addInline(this, c);
        }
      }
      // update the attrs
      removeAttr("input vertices:");
      // update the keys to use vertex name
      JSONObject keys = mapjoinObj.getJSONObject("keys:");
      if (keys.length() != 0) {
        JSONObject newKeys = new JSONObject();
        for (String key : JSONObject.getNames(keys)) {
          String vertexName = posToVertex.get(key);
          if (vertexName != null) {
            newKeys.put(vertexName, keys.get(key));
          } else {
            newKeys.put(this.vertex.name, keys.get(key));
          }
        }
        // update the attrs
        removeAttr("keys:");
        this.attrs.add(new Attr("keys:", newKeys.toString()));
      }
    }
    // inline merge join operator in a self-join
    else {
      if (this.vertex != null) {
        for (Vertex v : this.vertex.mergeJoinDummyVertexs) {
          parser.addInline(this, new Connection(null, v));
        }
      }
    }
  }

  private String getNameWithOpId() {
    if (operatorId != null) {
      return this.name + " [" + operatorId + "]";
    } else {
      return this.name;
    }
  }

  /**
   * @param out
   * @param indentFlag
   * @param branchOfJoinOp
   *          This parameter is used to show if it is a branch of a Join
   *          operator so that we can decide the corresponding indent.
   * @throws Exception
   */
  public void print(Printer printer, List<Boolean> indentFlag, boolean branchOfJoinOp)
      throws Exception {
    // print name
    if (parser.printSet.contains(this)) {
      printer.println(TezJsonParser.prefixString(indentFlag) + " Please refer to the previous "
          + this.getNameWithOpId());
      return;
    }
    parser.printSet.add(this);
    if (!branchOfJoinOp) {
      printer.println(TezJsonParser.prefixString(indentFlag) + this.getNameWithOpId());
    } else {
      printer.println(TezJsonParser.prefixString(indentFlag, "|<-") + this.getNameWithOpId());
    }
    branchOfJoinOp = false;
    // if this operator is a Map Join Operator or a Merge Join Operator
    if (this.name.equals("Map Join Operator") || this.name.equals("Merge Join Operator")) {
      inlineJoinOp();
      branchOfJoinOp = true;
    }
    // if this operator is the last operator, we summarize the non-inlined
    // vertex
    List<Connection> noninlined = new ArrayList<>();
    if (this.parent == null) {
      if (this.vertex != null) {
        for (Connection connection : this.vertex.parentConnections) {
          if (!parser.isInline(connection.from)) {
            noninlined.add(connection);
          }
        }
      }
    }
    // print attr
    List<Boolean> attFlag = new ArrayList<>();
    attFlag.addAll(indentFlag);
    // should print | if (1) it is branchOfJoinOp or (2) it is the last op and
    // has following non-inlined vertex
    if (branchOfJoinOp || (this.parent == null && !noninlined.isEmpty())) {
      attFlag.add(true);
    } else {
      attFlag.add(false);
    }
    Collections.sort(attrs);
    for (Attr attr : attrs) {
      printer.println(TezJsonParser.prefixString(attFlag) + attr.toString());
    }
    // print inline vertex
    if (parser.inlineMap.containsKey(this)) {
      for (int index = 0; index < parser.inlineMap.get(this).size(); index++) {
        Connection connection = parser.inlineMap.get(this).get(index);
        List<Boolean> vertexFlag = new ArrayList<>();
        vertexFlag.addAll(indentFlag);
        if (branchOfJoinOp) {
          vertexFlag.add(true);
        }
        // if there is an inline vertex but the operator itself is not on a join
        // branch,
        // then it means it is from a vertex created by an operator tree,
        // e.g., fetch operator, etc.
        else {
          vertexFlag.add(false);
        }
        connection.from.print(printer, vertexFlag, connection.type, this.vertex);
      }
    }
    // print parent op, i.e., where data comes from
    if (this.parent != null) {
      List<Boolean> parentFlag = new ArrayList<>();
      parentFlag.addAll(indentFlag);
      parentFlag.add(false);
      this.parent.print(printer, parentFlag, branchOfJoinOp);
    }
    // print next vertex
    else {
      for (int index = 0; index < noninlined.size(); index++) {
        Vertex v = noninlined.get(index).from;
        List<Boolean> vertexFlag = new ArrayList<>();
        vertexFlag.addAll(indentFlag);
        if (index != noninlined.size() - 1) {
          vertexFlag.add(true);
        } else {
          vertexFlag.add(false);
        }
        v.print(printer, vertexFlag, noninlined.get(index).type, this.vertex);
      }
    }
  }

  public void removeAttr(String name) {
    int removeIndex = -1;
    for (int index = 0; index < attrs.size(); index++) {
      if (attrs.get(index).name.equals(name)) {
        removeIndex = index;
        break;
      }
    }
    if (removeIndex != -1) {
      attrs.remove(removeIndex);
    }
  }
}
