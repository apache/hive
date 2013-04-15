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

package org.apache.hadoop.hive.ql.lib;

import java.util.Stack;

/**
 * Contains common utility functions to manipulate nodes, walkers etc.
 */
public class Utils {

  /**
   * Gets the nth ancestor (the parent being the 1st ancestor) in the traversal
   * path. n=0 returns the currently visited node.
   * 
   * @param st The stack that encodes the traversal path.
   * @param n The value of n (n=0 is the currently visited node).
   * 
   * @return Node The Nth ancestor in the path with respect to the current node.
   */
  public static Node getNthAncestor(Stack<Node> st, int n) {
    assert(st.size() - 1 >= n);
    
    Stack<Node> tmpStack = new Stack<Node>();
    for(int i=0; i<=n; i++)
      tmpStack.push(st.pop());
   
    Node ret_nd = tmpStack.peek();
    
    for(int i=0; i<=n; i++)
      st.push(tmpStack.pop());
    
    assert(tmpStack.isEmpty());
    
    return ret_nd;
  }

  /**
   * Find the first node of a type from ancestor stack, starting from parents.
   * Returns null if not found.
   */
  @SuppressWarnings("unchecked")
  public static <T> T findNode(Stack<Node> stack, Class<T> target) {
    for (int i = stack.size() - 2; i >= 0; i--) {
      if (target.isInstance(stack.get(i))) {
        return (T) stack.get(i);
      }
    }
    return null;
  }
}
