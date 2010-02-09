package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;

import org.apache.hadoop.mapred.Counters;

/**
 * TaskHandle.
 *
 */
public class TaskHandle {
  // The eventual goal is to monitor the progress of all the tasks, not only the
  // map reduce task.
  // The execute() method of the tasks will return immediately, and return a
  // task specific handle to
  // monitor the progress of that task.
  // Right now, the behavior is kind of broken, ExecDriver's execute method
  // calls progress - instead it should
  // be invoked by Driver
  public Counters getCounters() throws IOException {
    // default implementation
    return null;
  }
}
