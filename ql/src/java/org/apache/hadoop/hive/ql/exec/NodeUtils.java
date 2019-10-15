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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.lib.Node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Simple node iterating utils
 */
public class NodeUtils {


  public static <T> void iterateTask(Collection<Task<?>> tasks, Class<T> clazz, Function<T> function) {
    // Does a breadth first traversal of the tasks
    Set<Task> visited = new HashSet<Task>();
    while (!tasks.isEmpty()) {
      tasks = iterateTask(tasks, clazz, function, visited);
    }
    return;
  }

  private static <T> Collection<Task<?>> iterateTask(Collection<Task<?>> tasks,
                                                     Class<T> clazz,
                                                     Function<T> function,
                                                     Set<Task> visited) {
    Collection<Task<?>> childTasks = new ArrayList<>();
    for (Task<?> task : tasks) {
      if (!visited.add(task)) {
        continue;
      }
      if (clazz.isInstance(task)) {
        function.apply(clazz.cast(task));
      }
      // this is for ConditionalTask
      if (task.getDependentTasks() != null) {
        childTasks.addAll(task.getDependentTasks());
      }
    }
    return childTasks;
  }

  public static <T> void iterate(Collection<? extends Node> nodes, Class<T> clazz, Function<T> function) {
    Set<Node> visited = new HashSet<Node>();
    List<Collection<? extends Node>> listNodes = Collections.singletonList(nodes);
    while (!listNodes.isEmpty()) {
      listNodes = iterate(listNodes, clazz, function, visited);
    }
    return;
  }

  private static <T> List<Collection<? extends Node>> iterate(List<Collection<? extends Node>> listNodes,
                                                              Class<T> clazz,
                                                              Function<T> function,
                                                              Set<Node> visited) {
    List<Collection<? extends Node>> childListNodes = new ArrayList<>();
    for (Collection<? extends Node> nodes : listNodes) {
      for (Node node : nodes) {
        if (!visited.add(node)) {
          continue;
        }
        if (clazz.isInstance(node)) {
          function.apply(clazz.cast(node));
        }
        if (node.getChildren() != null) {
          childListNodes.add(node.getChildren());
        }
      }
    }
    return childListNodes;
  }

  public interface Function<T> {
    void apply(T argument);
  }
}
