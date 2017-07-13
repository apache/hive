package org.apache.hadoop.hive.ql.exec.repl.bootstrap.load;

import org.apache.hadoop.hive.ql.exec.Task;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.Serializable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
  public class TaskTrackerTest {
  @Mock
  private Task<? extends Serializable> task;

  @Test
  public void taskTrackerCompositionInitializesTheMaxTasksCorrectly() {
    TaskTracker taskTracker = new TaskTracker(1);
    assertTrue(taskTracker.canAddMoreTasks());
    taskTracker.addTask(task);
    assertFalse(taskTracker.canAddMoreTasks());

    TaskTracker taskTracker2 = new TaskTracker(taskTracker);
    assertFalse(taskTracker2.canAddMoreTasks());
  }
}