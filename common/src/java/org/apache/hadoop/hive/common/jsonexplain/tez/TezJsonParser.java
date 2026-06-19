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

package org.apache.hadoop.hive.common.jsonexplain.tez;

import org.apache.hadoop.hive.common.jsonexplain.DagJsonParser;


public class TezJsonParser extends DagJsonParser {

  @Override
  public String mapEdgeType(String edgeName) {
    switch (edgeName) {
      case "BROADCAST_EDGE":
        return "BROADCAST";
      case "SIMPLE_EDGE":
        return "SHUFFLE";
      case "CUSTOM_SIMPLE_EDGE":
        return "PARTITION_ONLY_SHUFFLE";
      case "CUSTOM_EDGE":
        return "MULTICAST";
      case "ONE_TO_ONE_EDGE":
        return "FORWARD";
      case "XPROD_EDGE":
        return "XPROD_EDGE";
      default:
        return "UNKNOWN";
    }
  }

  @Override
  public String getFrameworkName() {
    return "Tez";
  }
}