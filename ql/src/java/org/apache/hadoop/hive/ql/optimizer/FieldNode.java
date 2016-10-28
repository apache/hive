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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FieldNode {
  private String fieldName;
  private List<FieldNode> nodes;

  public FieldNode(String fieldName) {
    this.fieldName = fieldName;
    nodes = new ArrayList<>();
  }

  public String getFieldName() {
    return fieldName;
  }

  public void addFieldNodes(FieldNode... nodes) {
    if (nodes != null || nodes.length > 0) {
      this.nodes.addAll(Arrays.asList(nodes));
    }
  }

  public List<FieldNode> getNodes() {
    return nodes;
  }

  @Override
  public String toString() {
    String res = fieldName;
    if (nodes.size() > 0) {
      res += "[";
      for (int i = 0; i < nodes.size(); i++) {
        if (i == nodes.size() - 1) {
          res += nodes.get(i).toString();
        } else {
          res += nodes.get(i).toString() + ",";
        }
      }
      res += "]";
    }
    return res;
  }

  @Override
  public boolean equals(Object object) {
    FieldNode fieldNode = (FieldNode) object;
    if (!fieldName.equals(fieldNode.getFieldName()) || fieldNode.getNodes().size() != fieldNode
      .getNodes().size()) {
      return false;
    }

    for (int i = 0; i < fieldNode.getNodes().size(); i++) {
      if (fieldNode.getNodes().get(i).equals(nodes.get(i))) {
        return false;
      }
    }
    return true;
  }
}
