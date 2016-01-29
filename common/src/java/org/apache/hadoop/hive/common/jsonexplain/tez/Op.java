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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hive.common.jsonexplain.tez.Vertex.VertexType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public final class Op {
  public final String name;
  // tezJsonParser
  public final TezJsonParser parser;
  public final String operatorId;
  public Op parent;
  public final List<Op> children;
  public final Map<String, String> attrs;
  // the jsonObject for this operator
  public final JSONObject opObject;
  // the vertex that this operator belongs to
  public final Vertex vertex;
  // the vertex that this operator output to
  public final String outputVertexName;
  // the Operator type
  public final OpType type;

  public enum OpType {
    MAPJOIN, MERGEJOIN, RS, OTHERS
  };

  public Op(String name, String id, String outputVertexName, List<Op> children,
      Map<String, String> attrs, JSONObject opObject, Vertex vertex, TezJsonParser tezJsonParser)
      throws JSONException {
    super();
    this.name = name;
    this.operatorId = id;
    this.type = deriveOpType(operatorId);
    this.outputVertexName = outputVertexName;
    this.children = children;
    this.attrs = attrs;
    this.opObject = opObject;
    this.vertex = vertex;
    this.parser = tezJsonParser;
  }

  private OpType deriveOpType(String operatorId) {
    if (operatorId != null) {
      if (operatorId.startsWith(OpType.MAPJOIN.toString())) {
        return OpType.MAPJOIN;
      } else if (operatorId.startsWith(OpType.MERGEJOIN.toString())) {
        return OpType.MERGEJOIN;
      } else if (operatorId.startsWith(OpType.RS.toString())) {
        return OpType.RS;
      } else {
        return OpType.OTHERS;
      }
    } else {
      return OpType.OTHERS;
    }
  }

  private void inlineJoinOp() throws Exception {
    // inline map join operator
    if (this.type == OpType.MAPJOIN) {
      JSONObject joinObj = opObject.getJSONObject(this.name);
      // get the map for posToVertex
      JSONObject verticeObj = joinObj.getJSONObject("input vertices:");
      Map<String, Vertex> posToVertex = new LinkedHashMap<>();
      for (String pos : JSONObject.getNames(verticeObj)) {
        String vertexName = verticeObj.getString(pos);
        // update the connection
        Connection c = null;
        for (Connection connection : vertex.parentConnections) {
          if (connection.from.name.equals(vertexName)) {
            posToVertex.put(pos, connection.from);
            c = connection;
            break;
          }
        }
        if (c != null) {
          parser.addInline(this, c);
        }
      }
      // update the attrs
      this.attrs.remove("input vertices:");
      // update the keys to use operator name
      JSONObject keys = joinObj.getJSONObject("keys:");
      // find out the vertex for the big table
      Set<Vertex> parentVertexes = new HashSet<>();
      for (Connection connection : vertex.parentConnections) {
        parentVertexes.add(connection.from);
      }
      parentVertexes.removeAll(posToVertex.values());
      Map<String, String> posToOpId = new LinkedHashMap<>();
      if (keys.length() != 0) {
        for (String key : JSONObject.getNames(keys)) {
          // first search from the posToVertex
          if (posToVertex.containsKey(key)) {
            Vertex vertex = posToVertex.get(key);
            if (vertex.rootOps.size() == 1) {
              posToOpId.put(key, vertex.rootOps.get(0).operatorId);
            } else if ((vertex.rootOps.size() == 0 && vertex.vertexType == VertexType.UNION)) {
              posToOpId.put(key, vertex.name);
            } else {
              Op singleRSOp = vertex.getSingleRSOp();
              if (singleRSOp != null) {
                posToOpId.put(key, singleRSOp.operatorId);
              } else {
                throw new Exception(
                    "There are none or more than one root operators in a single vertex "
                        + vertex.name
                        + " when hive explain user is trying to identify the operator id.");
              }
            }
          }
          // then search from parent
          else if (parent != null) {
            posToOpId.put(key, parent.operatorId);
          }
          // then assume it is from its own vertex
          else if (parentVertexes.size() == 1) {
            Vertex vertex = parentVertexes.iterator().next();
            parentVertexes.clear();
            if (vertex.rootOps.size() == 1) {
              posToOpId.put(key, vertex.rootOps.get(0).operatorId);
            } else if ((vertex.rootOps.size() == 0 && vertex.vertexType == VertexType.UNION)) {
              posToOpId.put(key, vertex.name);
            } else {
              Op singleRSOp = vertex.getSingleRSOp();
              if (singleRSOp != null) {
                posToOpId.put(key, singleRSOp.operatorId);
              } else {
                throw new Exception(
                    "There are none or more than one root operators in a single vertex "
                        + vertex.name
                        + " when hive explain user is trying to identify the operator id.");
              }
            }
          }
          // finally throw an exception
          else {
            throw new Exception(
                "Can not find the source operator on one of the branches of map join.");
          }
        }
      }
      this.attrs.remove("keys:");
      StringBuffer sb = new StringBuffer();
      JSONArray conditionMap = joinObj.getJSONArray("condition map:");
      for (int index = 0; index < conditionMap.length(); index++) {
        JSONObject cond = conditionMap.getJSONObject(index);
        String k = (String) cond.keys().next();
        JSONObject condObject = new JSONObject((String)cond.get(k));
        String type = condObject.getString("type");
        String left = condObject.getString("left");
        String right = condObject.getString("right");
        if (keys.length() != 0) {
          sb.append(posToOpId.get(left) + "." + keys.get(left) + "=" + posToOpId.get(right) + "."
              + keys.get(right) + "(" + type + "),");
        } else {
          // probably a cross product
          sb.append("(" + type + "),");
        }
      }
      this.attrs.remove("condition map:");
      this.attrs.put("Conds:", sb.substring(0, sb.length() - 1));
    }
    // should be merge join
    else {
      Map<String, String> posToOpId = new LinkedHashMap<>();
      if (vertex.mergeJoinDummyVertexs.size() == 0) {
        if (vertex.tagToInput.size() != vertex.parentConnections.size()) {
          throw new Exception("tagToInput size " + vertex.tagToInput.size()
              + " is different from parentConnections size " + vertex.parentConnections.size());
        }
        for (Entry<String, String> entry : vertex.tagToInput.entrySet()) {
          Connection c = null;
          for (Connection connection : vertex.parentConnections) {
            if (connection.from.name.equals(entry.getValue())) {
              Vertex v = connection.from;
              if (v.rootOps.size() == 1) {
                posToOpId.put(entry.getKey(), v.rootOps.get(0).operatorId);
              } else if ((v.rootOps.size() == 0 && v.vertexType == VertexType.UNION)) {
                posToOpId.put(entry.getKey(), v.name);
              } else {
                Op singleRSOp = v.getSingleRSOp();
                if (singleRSOp != null) {
                  posToOpId.put(entry.getKey(), singleRSOp.operatorId);
                } else {
                  throw new Exception(
                      "There are none or more than one root operators in a single vertex " + v.name
                          + " when hive explain user is trying to identify the operator id.");
                }
              }
              c = connection;
              break;
            }
          }
          if (c == null) {
            throw new Exception("Can not find " + entry.getValue()
                + " while parsing keys of merge join operator");
          }
        }
      } else {
        posToOpId.put(vertex.tag, this.parent.operatorId);
        for (Vertex v : vertex.mergeJoinDummyVertexs) {
          if (v.rootOps.size() != 1) {
            throw new Exception("Can not find a single root operators in a single vertex " + v.name
                + " when hive explain user is trying to identify the operator id.");
          }
          posToOpId.put(v.tag, v.rootOps.get(0).operatorId);
        }
      }
      JSONObject joinObj = opObject.getJSONObject(this.name);
      // update the keys to use operator name
      JSONObject keys = joinObj.getJSONObject("keys:");
      if (keys.length() != 0) {
        for (String key : JSONObject.getNames(keys)) {
          if (!posToOpId.containsKey(key)) {
            throw new Exception(
                "Can not find the source operator on one of the branches of merge join.");
          }
        }
        // inline merge join operator in a self-join
        if (this.vertex != null) {
          for (Vertex v : this.vertex.mergeJoinDummyVertexs) {
            parser.addInline(this, new Connection(null, v));
          }
        }
      }
      // update the attrs
      this.attrs.remove("keys:");
      StringBuffer sb = new StringBuffer();
      JSONArray conditionMap = joinObj.getJSONArray("condition map:");
      for (int index = 0; index < conditionMap.length(); index++) {
        JSONObject cond = conditionMap.getJSONObject(index);
        String k = (String) cond.keys().next();
        JSONObject condObject = new JSONObject((String)cond.get(k));
        String type = condObject.getString("type");
        String left = condObject.getString("left");
        String right = condObject.getString("right");
        if (keys.length() != 0) {
          sb.append(posToOpId.get(left) + "." + keys.get(left) + "=" + posToOpId.get(right) + "."
              + keys.get(right) + "(" + type + "),");
        } else {
          // probably a cross product
          sb.append("(" + type + "),");
        }
      }
      this.attrs.remove("condition map:");
      this.attrs.put("Conds:", sb.substring(0, sb.length() - 1));
    }
  }

  private String getNameWithOpIdStats() {
    StringBuffer sb = new StringBuffer();
    sb.append(TezJsonParserUtils.renameReduceOutputOperator(name, vertex));
    if (operatorId != null) {
      sb.append(" [" + operatorId + "]");
    }
    if (!TezJsonParserUtils.OperatorNoStats.contains(name) && attrs.containsKey("Statistics:")) {
      sb.append(" (" + attrs.get("Statistics:") + ")");
    }
    attrs.remove("Statistics:");
    return sb.toString();
  }

  /**
   * @param printer
   * @param indentFlag
   * @param branchOfJoinOp
   *          This parameter is used to show if it is a branch of a Join
   *          operator so that we can decide the corresponding indent.
   * @throws Exception
   */
  public void print(Printer printer, int indentFlag, boolean branchOfJoinOp) throws Exception {
    // print name
    if (parser.printSet.contains(this)) {
      printer.println(TezJsonParser.prefixString(indentFlag) + " Please refer to the previous "
          + this.getNameWithOpIdStats());
      return;
    }
    parser.printSet.add(this);
    if (!branchOfJoinOp) {
      printer.println(TezJsonParser.prefixString(indentFlag) + this.getNameWithOpIdStats());
    } else {
      printer.println(TezJsonParser.prefixString(indentFlag, "<-") + this.getNameWithOpIdStats());
    }
    branchOfJoinOp = false;
    // if this operator is a Map Join Operator or a Merge Join Operator
    if (this.type == OpType.MAPJOIN || this.type == OpType.MERGEJOIN) {
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
    indentFlag++;
    if (!attrs.isEmpty()) {
      printer.println(TezJsonParser.prefixString(indentFlag)
          + TezJsonParserUtils.attrsToString(attrs));
    }
    // print inline vertex
    if (parser.inlineMap.containsKey(this)) {
      for (int index = 0; index < parser.inlineMap.get(this).size(); index++) {
        Connection connection = parser.inlineMap.get(this).get(index);
        connection.from.print(printer, indentFlag, connection.type, this.vertex);
      }
    }
    // print parent op, i.e., where data comes from
    if (this.parent != null) {
      this.parent.print(printer, indentFlag, branchOfJoinOp);
    }
    // print next vertex
    else {
      for (int index = 0; index < noninlined.size(); index++) {
        Vertex v = noninlined.get(index).from;
        v.print(printer, indentFlag, noninlined.get(index).type, this.vertex);
      }
    }
  }
}
