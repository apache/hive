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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.lib.Node;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple node iterating utils
 */
public class NodeUtils {

  public static <T> void iterateTask(Collection<Task<?>> tasks, Class<T> clazz, Function<T> function) {
    Set<Task> visited = new HashSet<Task>();
    for (Task<?> task : tasks) {
      iterateTask(task, clazz, function, visited);
    }
    return;
  }

  private static <T> void iterateTask(Task<?> task, Class<T> clazz, Function<T> function, Set<Task> visited) {
    if (!visited.add(task)) {
      return;
    }
    if (clazz.isInstance(task)) {
      function.apply(clazz.cast(task));
    }
    // this is for ConditionalTask
    if (task.getDependentTasks() != null) {
      for (Task<?> dependent : task.getDependentTasks()) {
        iterateTask(dependent, clazz, function, visited);
      }
    }
  }

  public static <T> void iterate(Collection<? extends Node> nodes, Class<T> clazz, Function<T> function) {
    Set<Node> visited = new HashSet<Node>();
    for (Node task : nodes) {
      iterate(task, clazz, function, visited);
    }
    return;
  }

  private static <T> void iterate(Node node, Class<T> clazz, Function<T> function, Set<Node> visited) {
    if (!visited.add(node)) {
      return;
    }
    if (clazz.isInstance(node)) {
      function.apply(clazz.cast(node));
    }
    if (node.getChildren() != null) {
      for (Node child : node.getChildren()) {
        iterate(child, clazz, function, visited);
      }
    }
  }

  public static interface Function<T> {
    void apply(T argument);
  }
}
