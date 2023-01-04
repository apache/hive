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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.hadoop.hive.common.jsonexplain.Op.OpType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Vertex implements Comparable<Vertex>{
  public final String name;
  // the stage that this vertex belongs to
  public final Stage stage;
  //tezJsonParser
  public final DagJsonParser parser;
  // vertex's parent connections.
  public final List<Connection> parentConnections = new ArrayList<>();
  // vertex's children vertex.
  public final List<Vertex> children = new ArrayList<>();
  // the jsonObject for this vertex
  public final JSONObject vertexObject;
  // whether this vertex is dummy (which does not really exists but is created),
  // e.g., a dummy vertex for a mergejoin branch
  public boolean dummy;
  // the outputOps in this vertex.
  public final List<Op> outputOps= new ArrayList<>();
  // the inputOps in this vertex.
  public final List<Op> inputOps= new ArrayList<>();
  // we create a dummy vertex for a mergejoin branch for a self join if this
  // vertex is a mergejoin
  public final List<Vertex> mergeJoinDummyVertices = new ArrayList<>();
  // this vertex has multiple reduce operators
  public int numReduceOp = 0;
  // execution mode
  public String executionMode = "";
  // tagToInput for reduce work
  public Map<String, String> tagToInput = new LinkedHashMap<>();
  // tag
  public String tag;
  protected final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

  public static enum VertexType {
    MAP, REDUCE, UNION, UNKNOWN
  };
  public VertexType vertexType;

  public static enum EdgeType {
    BROADCAST, SHUFFLE, MULTICAST, PARTITION_ONLY_SHUFFLE, FORWARD, XPROD_EDGE, UNKNOWN
  };
  public String edgeType;

  public Vertex(String name, JSONObject vertexObject, Stage stage, DagJsonParser dagJsonParser) {
    super();
    this.name = name;
    if (this.name != null) {
      if (this.name.contains("Map")) {
        this.vertexType = VertexType.MAP;
      } else if (this.name.contains("Reduce")) {
        this.vertexType = VertexType.REDUCE;
      } else if (this.name.contains("Union")) {
        this.vertexType = VertexType.UNION;
      } else {
        this.vertexType = VertexType.UNKNOWN;
      }
    } else {
      this.vertexType = VertexType.UNKNOWN;
    }
    this.dummy = false;
    this.vertexObject = vertexObject;
    this.stage = stage;
    this.parser = dagJsonParser;
  }

  public void addDependency(Connection connection) throws JSONException {
    this.parentConnections.add(connection);
  }

  /**
   * @throws Exception
   *           We assume that there is a single top-level Map Operator Tree or a
   *           Reduce Operator Tree in a vertex
   */
  public void extractOpTree() throws Exception {
    if (vertexObject.length() != 0) {
      for (String key : JSONObject.getNames(vertexObject)) {
        if (key.equals("Map Operator Tree:")) {
          extractOp(vertexObject.getJSONArray(key).getJSONObject(0), null);
        } else if (key.equals("Reduce Operator Tree:") || key.equals("Processor Tree:")) {
          extractOp(vertexObject.getJSONObject(key), null);
        } else if (key.equals("Join:")) {
          // this is the case when we have a map-side SMB join
          // one input of the join is treated as a dummy vertex
          JSONArray array = vertexObject.getJSONArray(key);
          for (int index = 0; index < array.length(); index++) {
            JSONObject mpOpTree = array.getJSONObject(index);
            Vertex v = new Vertex(null, mpOpTree, this.stage, parser);
            v.extractOpTree();
            v.dummy = true;
            mergeJoinDummyVertices.add(v);
          }
        } else if (key.equals("Merge File Operator")) {
          JSONObject opTree = vertexObject.getJSONObject(key);
          if (opTree.has("Map Operator Tree:")) {
            extractOp(opTree.getJSONArray("Map Operator Tree:").getJSONObject(0), null);
          } else {
            throw new Exception("Merge File Operator does not have a Map Operator Tree");
          }
        } else if (key.equals("Execution mode:")) {
          executionMode = " " + vertexObject.getString(key);
        } else if (key.equals("tagToInput:")) {
          JSONObject tagToInput = vertexObject.getJSONObject(key);
          for (String tag : JSONObject.getNames(tagToInput)) {
            this.tagToInput.put(tag, (String) tagToInput.get(tag));
          }
        } else if (key.equals("tag:")) {
          this.tag = vertexObject.getString(key);
        } else if (key.equals("Local Work:")) {
          extractOp(vertexObject.getJSONObject(key), null);
        } else {
          LOG.warn("Skip unsupported " + key + " in vertex " + this.name);
        }
      }
    }
  }

  /**
   * @param object
   * @param parent
   * @return
   * @throws Exception
   *           assumption: each operator only has one parent but may have many
   *           children
   */
  Op extractOp(JSONObject object, Op parent) throws Exception {
    String[] names = JSONObject.getNames(object);
    if (names.length != 1) {
      throw new Exception("Expect only one operator in " + object.toString());
    } else {
      String opName = names[0];
      JSONObject attrObj = (JSONObject) object.get(opName);
      Map<String, String> attrs = new TreeMap<>();
      List<Op> children = new ArrayList<>();
      Op op = new Op(opName, null, null, parent, children, attrs, attrObj, this, parser);

      if (JSONObject.getNames(attrObj) != null) {
        for (String attrName : JSONObject.getNames(attrObj)) {
          if (attrName.equals("children")) {
            Object childrenObj = attrObj.get(attrName);
            if (childrenObj instanceof JSONObject) {
              if (((JSONObject) childrenObj).length() != 0) {
                children.add(extractOp((JSONObject) childrenObj, op));
              }
            } else if (childrenObj instanceof JSONArray) {
              if (((JSONArray) childrenObj).length() != 0) {
                JSONArray array = ((JSONArray) childrenObj);
                for (int index = 0; index < array.length(); index++) {
                  children.add(extractOp(array.getJSONObject(index), op));
                }
              }
            } else {
              throw new Exception("Unsupported operator " + this.name
                      + "'s children operator is neither a jsonobject nor a jsonarray");
            }
          } else {
            if (attrName.equals("OperatorId:")) {
              op.setOperatorId(attrObj.get(attrName).toString());
            } else if (attrName.equals("outputname:")) {
              op.outputVertexName = attrObj.get(attrName).toString();
            } else {
              if (!attrObj.get(attrName).toString().isEmpty()) {
                attrs.put(attrName, attrObj.get(attrName).toString());
              }
            }
          }
        }
      }
      if (parent == null) {
        this.inputOps.add(op);
      }
      if (children.isEmpty()) {
        this.outputOps.add(op);
      }
      return op;
    }
  }

  public void print(Printer printer, int indentFlag, String type, Vertex callingVertex) throws Exception {
    // print vertexname
    if (parser.printSet.contains(this) && numReduceOp <= 1) {
      if (type != null) {
        printer.println(DagJsonParser.prefixString(indentFlag, "<-")
            + " Please refer to the previous " + this.name + " [" + type + "]");
      } else {
        printer.println(DagJsonParser.prefixString(indentFlag, "<-")
            + " Please refer to the previous " + this.name);
      }
      return;
    }
    parser.printSet.add(this);
    if (type != null) {
      printer.println(DagJsonParser.prefixString(indentFlag, "<-") + this.name + " [" + type + "]"
          + this.executionMode);
    } else if (this.name != null) {
      printer.println(DagJsonParser.prefixString(indentFlag) + this.name + this.executionMode);
    }
    // print operators
    if (numReduceOp > 1 && !(callingVertex.vertexType == VertexType.UNION)) {
      // find the right op
      Op choose = null;
      for (Op op : this.outputOps) {
        // op.outputVertexName may be null
        if (callingVertex.name.equals(op.outputVertexName)) {
          choose = op;
        }
      }
      if (choose != null) {
        choose.print(printer, indentFlag, false);
      } else {
        throw new Exception("Can not find the right reduce output operator for vertex " + this.name);
      }
    } else {
      for (Op op : this.outputOps) {
        // dummy vertex is treated as a branch of a join operator
        if (this.dummy) {
          op.print(printer, indentFlag, true);
        } else {
          op.print(printer, indentFlag, false);
        }
      }
    }
    if (vertexType == VertexType.UNION) {
      // print dependent vertices
      indentFlag++;
      for (int index = 0; index < this.parentConnections.size(); index++) {
        Connection connection = this.parentConnections.get(index);
        connection.from.print(printer, indentFlag, connection.type, this);
      }
    }
  }

  /**
   * We check if a vertex has multiple reduce operators.
   * @throws JSONException
   */
  public void checkMultiReduceOperator(boolean rewriteObject) throws JSONException {
    // check if it is a reduce vertex and its children is more than 1;
    // check if all the child ops are reduce output operators
    numReduceOp = 0;
    for (Op op : this.outputOps) {
      if (op.type == OpType.RS) {
        if (rewriteObject) {
          Vertex outputVertex = this.stage.vertices.get(op.outputVertexName);
          if (outputVertex != null && outputVertex.inputOps.size() > 0) {
            JSONArray array = new JSONArray();
            for (Op inputOp : outputVertex.inputOps) {
              array.put(inputOp.operatorId);
            }
            op.opObject.put("outputOperator:", array);
          }
        }
        numReduceOp++;
      }
    }
  }

  public void setType(String type) {
    this.edgeType = this.parser.mapEdgeType(type);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Vertex vertex = (Vertex) o;
    return Objects.equals(name, vertex.name) &&
            Objects.equals(stage, vertex.stage) &&
            Objects.equals(vertexObject, vertex.vertexObject);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, stage, vertexObject);
  }

  // The following code should be gone after HIVE-11075 using topological order
  @Override
  public int compareTo(Vertex o) {
    // we print the vertex that has more rs before the vertex that has fewer rs.
    if (numReduceOp != o.numReduceOp) {
      return -(numReduceOp - o.numReduceOp);
    } else {
      return this.name.compareTo(o.name);
    }
  }

  public Op getJoinRSOp(Vertex joinVertex) {
    if (outputOps.size() == 0) {
      return null;
    } else if (outputOps.size() == 1) {
      if (outputOps.get(0).type == OpType.RS) {
        return outputOps.get(0);
      } else {
        return null;
      }
    } else {
      for (Op op : outputOps) {
        if (op.type == OpType.RS) {
          if (op.outputVertexName.equals(joinVertex.name)) {
            return op;
          }
        }
      }
      return null;
    }
  }
}
