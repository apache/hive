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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hive.common.jsonexplain.JsonParser;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DagJsonParser implements JsonParser {
  public final Map<String, Stage> stages = new LinkedHashMap<>();
  protected final Logger LOG;
  // the objects that have been printed.
  public final Set<Object> printSet = new LinkedHashSet<>();
  // the vertex that should be inlined. <Operator, list of Vertex that is
  // inlined>
  public final Map<Op, List<Connection>> inlineMap = new LinkedHashMap<>();
  public boolean rewriteObject;

  public DagJsonParser() {
    super();
    LOG = LoggerFactory.getLogger(this.getClass().getName());
  }

  public void extractStagesAndPlans(JSONObject inputObject) throws Exception {
    // extract stages
    JSONObject dependency = inputObject.getJSONObject("STAGE DEPENDENCIES");
    if (dependency != null && dependency.length() > 0) {
      // iterate for the first time to get all the names of stages.
      for (String stageName : JSONObject.getNames(dependency)) {
        this.stages.put(stageName, new Stage(stageName, this));
      }
      // iterate for the second time to get all the dependency.
      for (String stageName : JSONObject.getNames(dependency)) {
        JSONObject dependentStageNames = dependency.getJSONObject(stageName);
        this.stages.get(stageName).addDependency(dependentStageNames, this.stages);
      }
    }
    // extract stage plans
    JSONObject stagePlans = inputObject.getJSONObject("STAGE PLANS");
    if (stagePlans != null && stagePlans.length() > 0) {
      for (String stageName : JSONObject.getNames(stagePlans)) {
        JSONObject stagePlan = stagePlans.getJSONObject(stageName);
        this.stages.get(stageName).extractVertex(stagePlan);
      }
    }
  }

  /**
   * @param indentFlag
   *          help to generate correct indent
   * @return
   */
  public static String prefixString(int indentFlag) {
    StringBuilder sb = new StringBuilder();
    for (int index = 0; index < indentFlag; index++) {
      sb.append("  ");
    }
    return sb.toString();
  }

  /**
   * @param indentFlag
   * @param tail
   *          help to generate correct indent with a specific tail
   * @return
   */
  public static String prefixString(int indentFlag, String tail) {
    StringBuilder sb = new StringBuilder();
    for (int index = 0; index < indentFlag; index++) {
      sb.append("  ");
    }
    int len = sb.length();
    return sb.replace(len - tail.length(), len, tail).toString();
  }

  @Override
  public void print(JSONObject inputObject, PrintStream outputStream) throws Exception {
    LOG.info("JsonParser is parsing:" + inputObject.toString());
    this.rewriteObject = outputStream == null;
    // outputStream == null means we need to process it for explain formatted
    this.extractStagesAndPlans(inputObject);
    if (rewriteObject) {
      return;
    }
    Printer printer = new Printer();
    // print out the cbo info
    if (inputObject.has("cboInfo")) {
      printer.println(inputObject.getString("cboInfo"));
      printer.println();
    }
    // print out the vertex dependency in root stage
    for (Stage candidate : this.stages.values()) {
      if (candidate.tezStageDependency != null && candidate.tezStageDependency.size() > 0) {
        if (candidate.parentStages.size() == 0) {
          printer.println("Vertex dependency in root stage");
        } else {
          printer.println("Vertex dependency in " + candidate.externalName);
        }
        for (Entry<Vertex, List<Connection>> entry : candidate.tezStageDependency.entrySet()) {
          StringBuilder sb = new StringBuilder();
          sb.append(entry.getKey().name);
          sb.append(" <- ");
          boolean printcomma = false;
          for (Connection connection : entry.getValue()) {
            if (printcomma) {
              sb.append(", ");
            } else {
              printcomma = true;
            }
            sb.append(connection.from.name + " (" + connection.type + ")");
          }
          printer.println(sb.toString());
        }
        printer.println();
      }
    }
    // print out all the stages that have no childStages.
    for (Stage candidate : this.stages.values()) {
      if (candidate.childStages.isEmpty()) {
        candidate.print(printer, 0);
      }
    }
    outputStream.println(printer.toString());
  }

  public void addInline(Op op, Connection connection) {
    List<Connection> list = inlineMap.get(op);
    if (list == null) {
      list = new ArrayList<>();
      list.add(connection);
      inlineMap.put(op, list);
    } else {
      list.add(connection);
    }
  }

  public boolean isInline(Vertex v) {
    for (List<Connection> list : inlineMap.values()) {
      for (Connection connection : list) {
        if (connection.from.equals(v)) {
          return true;
        }
      }
    }
    return false;
  }

  public abstract String mapEdgeType(String edgeName);

  public abstract String getFrameworkName();
}
