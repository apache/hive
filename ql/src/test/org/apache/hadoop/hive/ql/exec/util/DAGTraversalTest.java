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
package org.apache.hadoop.hive.ql.exec.util;

import org.apache.hadoop.hive.ql.exec.Task;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DAGTraversalTest {

  static class CountLeafFunction implements DAGTraversal.Function {
    int count = 0;

    @Override
    public void process(Task<?> task) {
      if (task.getDependentTasks() == null || task.getDependentTasks().isEmpty()) {
        count++;
      }
    }

    @Override
    public boolean skipProcessing(Task<?> task) {
      return false;
    }
  }

  @Test
  public void shouldCountNumberOfLeafNodesCorrectly() {
    Task<?> taskWith5NodeTree = linearTree(5);
    Task<?> taskWith1NodeTree = linearTree(1);
    Task<?> taskWith3NodeTree = linearTree(3);
    @SuppressWarnings("unchecked") Task<?> rootTask = mock(Task.class);
    when(rootTask.getDependentTasks())
        .thenReturn(Arrays.asList(taskWith1NodeTree, taskWith3NodeTree, taskWith5NodeTree));

    CountLeafFunction function = new CountLeafFunction();
    DAGTraversal.traverse(Collections.singletonList(rootTask), function);
    assertEquals(3, function.count);
  }

  private Task<?> linearTree(int numOfNodes) {
    Task<?> current = null, head = null;
    for (int i = 0; i < numOfNodes; i++) {
      @SuppressWarnings("unchecked") Task<?> task = mock(Task.class);
      if (current != null) {
        when(current.getDependentTasks()).thenReturn(Collections.singletonList(task));
      }
      if (head == null) {
        head = task;
      }
      current = task;
    }
    return head;
  }

}
