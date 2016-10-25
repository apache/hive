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

import java.util.Arrays;
import java.util.List;

public class NestedColumnFieldPruningUtils {

  /**
   * Add a leaf node to the field tree if the specified path is not contained by
   * current tree specified by the passed parameter field node.
   *
   * @param fieldNode the root of the column tree
   * @param path contains the path from root to leaf
   * @return the root of the newly built tree
   */
  public static FieldNode addNodeByPath(
    FieldNode fieldNode,
    String path) {
    if (path == null || path.isEmpty()) {
      return fieldNode;
    }
    boolean found = false;
    int index = 0;
    String[] ps = path.split("\\.");
    FieldNode c = fieldNode;
    if (fieldNode != null) {
      List<FieldNode> currentList = Arrays.asList(c);
      while (index < ps.length) {
        found = false;
        for (FieldNode n : currentList) {
          if (n.getFieldName().equals(ps[index])) {
            found = true;
            // If the matched field is leaf which means all leaves are required, not need to go
            // deeper.
            if (n.getNodes().isEmpty()) {
              return fieldNode;
            }
            c = n;
            currentList = c.getNodes();
            break;
          }
        }
        if (found) {
          index++;
        } else {
          break;
        }
      }
    }

    if (!found) {
      while (index < ps.length) {
        FieldNode n = new FieldNode(ps[index]);
        if (fieldNode == null) {
          // rebuild the tree since original is empty
          fieldNode = n;
        }
        if (c != null) {
          c.addFieldNodes(n);
        }
        c = n;
        index++;
      }
    } else {
      if (index == ps.length) {
        // Consolidation since all leaves are required.
        c.getNodes().clear();
      }
    }
    return fieldNode;
  }

}
