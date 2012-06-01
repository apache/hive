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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * PartitionSpec
 *
 */
@Explain(displayName = "Partition specification")
public class PartitionSpec {

  private class PredicateSpec {
    private String operator;
    private String value;

    public PredicateSpec() {
    }

    public PredicateSpec(String operator, String value) {
      this.operator = operator;
      this.value = value;
    }

    public String getOperator() {
      return this.operator;
    }

    public String getValue() {
      return this.value;
    }

    public void setOperator(String operator) {
      this.operator = operator;
    }

    public void setValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return (((this.operator.equals("!="))? "<>": this.operator) + " " + this.value);
    }
  }

  private final Map<String, PredicateSpec> partSpec;

  public PartitionSpec() {
    this.partSpec = new LinkedHashMap<String, PredicateSpec>();
  }

  /**
   * @param key
   *          partition key name for one partition key compare in the spec
   * @param operator
   *          the operator that is used for the comparison
   * @param value
   *          the value to be compared against
   */
  public void addPredicate(String key, String operator, String value) {
    partSpec.put(key, new PredicateSpec(operator, value));
  }

  /**
   * @param key
   *          partition key to look for in the partition spec
   * @return true if key exists in the partition spec, false otherwise
   */
  public boolean existsKey(String key) {
    return (partSpec.get(key) != null);
  }

  @Override
  public String toString() {
    StringBuilder filterString = new StringBuilder();
    int count = 0;
    for (Map.Entry<String, PredicateSpec> entry: this.partSpec.entrySet()) {
      if (count > 0) {
        filterString.append(" AND ");
      }
      filterString.append(entry.getKey() + " " + entry.getValue().toString());
      count++;
    }
    return filterString.toString();
  }

  // getParitionsByFilter only works for string columns due to a JDO limitation.
  // The operator is only useful if it can be passed as a filter to the metastore.
  // For compatibility with other non-string partition columns, this function
  // returns the key, value mapping assuming that the operator is equality.
  public Map<String, String> getPartSpecWithoutOperator() {
    Map<String, String> partSpec = new LinkedHashMap<String, String>();
    for (Map.Entry<String, PredicateSpec> entry: this.partSpec.entrySet()) {
      partSpec.put(entry.getKey(), PlanUtils.stripQuotes(entry.getValue().getValue()));
    }

    return partSpec;
  }

  // Again, for the same reason as the above function - getPartSpecWithoutOperator
  public boolean isNonEqualityOperator() {
    Iterator<PredicateSpec> iter = partSpec.values().iterator();
    while (iter.hasNext()) {
      PredicateSpec predSpec = iter.next();
      if (!predSpec.operator.equals("=")) {
        return true;
      }
    }
    return false;
  }
}
