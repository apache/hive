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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VectorizationCondition {

  private final boolean flag;
  private final String conditionName;

  public VectorizationCondition(boolean flag, String conditionName) {
    this.flag = flag;
    this.conditionName = conditionName;
  }

  public boolean getFlag() {
    return flag;
  }

  public String getConditionName() {
    return conditionName;
  }

  public static List<String> getConditionsMet(VectorizationCondition[] conditions) {
    List<String> metList = new ArrayList<String>();
    for (VectorizationCondition condition : conditions) {
      if (condition.getFlag()) {
        metList.add(condition.getConditionName() + " IS true");
      }
    }
    return metList;
  }

  public static List<String> getConditionsNotMet(VectorizationCondition[] conditions) {
    List<String> notMetList = new ArrayList<String>();
    for (VectorizationCondition condition : conditions) {
      if (!condition.getFlag()) {
        notMetList.add(condition.getConditionName() + " IS false");
      }
    }
    return notMetList;
  }

  public static List<String> addBooleans(List<String> conditions, boolean flag) {
    ArrayList<String> result = new ArrayList<String>(conditions.size());
    for (String condition : conditions) {
      result.add(condition + " IS " + flag);
    }
    return result;
  }

  // Helper method.
  public static List<String> getConditionsSupported(boolean isSupported) {
    return Arrays.asList("Supported IS " + isSupported);
  }

}
