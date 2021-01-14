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

package org.apache.hadoop.hive.ql.exec.repl.bootstrap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.util.AddDependencyToLeaves;
import org.apache.hadoop.hive.ql.exec.util.DAGTraversal;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AddDependencyToLeavesTest {
  @Mock
  private HiveConf hiveConf;

  @Test
  public void shouldNotSkipIntermediateDependencyCollectionTasks() {
    Task<DependencyCollectionWork> collectionWorkTaskOne =
        TaskFactory.get(new DependencyCollectionWork());
    Task<DependencyCollectionWork> collectionWorkTaskTwo =
        TaskFactory.get(new DependencyCollectionWork());
    Task<DependencyCollectionWork> collectionWorkTaskThree =
        TaskFactory.get(new DependencyCollectionWork());

    @SuppressWarnings("unchecked") Task<?> rootTask = mock(Task.class);
    when(rootTask.getDependentTasks())
        .thenReturn(
            Arrays.asList(collectionWorkTaskOne, collectionWorkTaskTwo, collectionWorkTaskThree));
    @SuppressWarnings("unchecked") List<Task<?>> tasksPostCurrentGraph =
        Arrays.asList(mock(Task.class), mock(Task.class));

    DAGTraversal.traverse(Collections.singletonList(rootTask),
        new AddDependencyToLeaves(tasksPostCurrentGraph));

    List<Task<?>> dependentTasksForOne =
        collectionWorkTaskOne.getDependentTasks();
    List<Task<?>> dependentTasksForTwo =
        collectionWorkTaskTwo.getDependentTasks();
    List<Task<?>> dependentTasksForThree =
        collectionWorkTaskThree.getDependentTasks();

    assertEquals(dependentTasksForOne.size(), 2);
    assertEquals(dependentTasksForTwo.size(), 2);
    assertEquals(dependentTasksForThree.size(), 2);
    assertTrue(tasksPostCurrentGraph.containsAll(dependentTasksForOne));
    assertTrue(tasksPostCurrentGraph.containsAll(dependentTasksForTwo));
    assertTrue(tasksPostCurrentGraph.containsAll(dependentTasksForThree));

//    assertTrue(dependentTasksForOne.iterator().next() instanceof DependencyCollectionTask);
//    assertTrue(dependentTasksForTwo.iterator().next() instanceof DependencyCollectionTask);
//    assertTrue(dependentTasksForThree.iterator().next() instanceof DependencyCollectionTask);
  }


}
