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

package org.apache.hadoop.hive.ql.optimizer;

import com.google.common.base.Preconditions;

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

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public void addFieldNodes(FieldNode... nodes) {
    if (nodes != null) {
      addFieldNodes(Arrays.asList(nodes));
    }
  }

  public void addFieldNodes(List<FieldNode> nodes) {
    for (FieldNode fn : nodes) {
      if (fn != null) {
        this.nodes.add(fn);
      }
    }
  }

  public List<FieldNode> getNodes() {
    return nodes;
  }

  public void setNodes(List<FieldNode> nodes) {
    this.nodes = nodes;
  }

  public List<String> toPaths() {
    List<String> result = new ArrayList<>();
    if (nodes.isEmpty()) {
      result.add(fieldName);
    } else {
      for (FieldNode child : nodes) {
        for (String rest : child.toPaths()) {
          result.add(fieldName + "." + rest);
        }
      }
    }
    return result;
  }

  public static FieldNode fromPath(String path) {
    String[] parts = path.split("\\.");
    return fromPath(parts, 0);
  }

  private static FieldNode fromPath(String[] parts, int index) {
    if (index == parts.length) {
      return null;
    }
    FieldNode fn = new FieldNode(parts[index]);
    fn.addFieldNodes(fromPath(parts, index + 1));
    return fn;
  }

  /**
   * Merge the field node 'fn' into list 'nodes', and return the result list.
   */
  public static List<FieldNode> mergeFieldNodes(List<FieldNode> nodes, FieldNode fn) {
    List<FieldNode> result = new ArrayList<>(nodes);
    for (int i = 0; i < nodes.size(); ++i) {
      FieldNode mfn = mergeFieldNode(nodes.get(i), fn);
      if (mfn != null) {
        result.set(i, mfn);
        return result;
      }
    }
    result.add(fn);
    return result;
  }

  public static List<FieldNode> mergeFieldNodes(List<FieldNode> left, List<FieldNode> right) {
    List<FieldNode> result = new ArrayList<>(left);
    for (FieldNode fn : right) {
      result = mergeFieldNodes(result, fn);
    }
    return result;
  }

  /**
   * Merge the field nodes 'left' and 'right' and return the merged node.
   * Return null if the two nodes cannot be merged.
   *
   * There are basically 3 cases here:
   * 1. 'left' and 'right' have the same depth, e.g., 'left' is s[b[c]] and
   *   'right' is s[b[d]]. In this case, the merged node is s[b[c,d]]
   * 2. 'left' has larger depth than 'right', e.g., 'left' is s[b] while
   *   'right' is s[b[d]]. In this case, the merged node is s[b]
   * 3. 'left' has smaller depth than 'right', e.g., 'left' is s[b[c]] while
   *   'right' is s[b]. This is the opposite case of 2), and similarly,
   *   the merged node is s[b].
   *
   * A example where the two inputs cannot be merged is, 'left' is s[b] while
   *   'right' is p[c].
   */
  public static FieldNode mergeFieldNode(FieldNode left, FieldNode right) {
    Preconditions.checkArgument(left.getFieldName() != null && right.getFieldName() != null);
    if (!left.getFieldName().equals(right.getFieldName())) {
      return null;
    }
    if (left.getNodes().isEmpty()) {
      return left;
    } else if (right.getNodes().isEmpty()) {
      return right;
    } else {
      // Both are not empty. Merge two lists.
      FieldNode result = new FieldNode(left.getFieldName());
      result.setNodes(mergeFieldNodes(left.getNodes(), right.getNodes()));
      return result;
    }
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FieldNode fieldNode = (FieldNode) o;

    if (fieldName != null ? !fieldName.equals(fieldNode.fieldName) : fieldNode.fieldName != null) {
      return false;
    }
    return nodes != null ? nodes.equals(fieldNode.nodes) : fieldNode.nodes == null;

  }

  @Override
  public int hashCode() {
    int result = fieldName != null ? fieldName.hashCode() : 0;
    result = 31 * result + (nodes != null ? nodes.hashCode() : 0);
    return result;
  }
}
