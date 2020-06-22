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

import org.apache.hive.common.util.SuppressFBWarnings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class DagJsonParserUtils {

  private static final List<String> operatorNoStats = Arrays.asList(new String[] { "File Output Operator",
      "Reduce Output Operator" });

  public static List<String> getOperatorNoStats() {
    return operatorNoStats;
  }

  public static String renameReduceOutputOperator(String operatorName, Vertex vertex) {
    if (operatorName.equals("Reduce Output Operator") && vertex.edgeType != null) {
      return vertex.edgeType;
    } else {
      return operatorName;
    }
  }

  public static String attrsToString(Map<String, String> attrs) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Entry<String, String> entry : attrs.entrySet()) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(entry.getKey() + entry.getValue());
    }
    return sb.toString();
  }
}
