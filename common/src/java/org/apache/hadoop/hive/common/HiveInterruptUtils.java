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

package org.apache.hadoop.hive.common;

import java.util.ArrayList;
import java.util.List;

public class HiveInterruptUtils {

  /**
   * A list of currently running comments that needs cleanup when the command is canceled
   */
  private static List<HiveInterruptCallback> interruptCallbacks = new ArrayList<HiveInterruptCallback>();

  public static HiveInterruptCallback add(HiveInterruptCallback command) {
    synchronized (interruptCallbacks) {
      interruptCallbacks.add(command);
    }
    return command;
  }

  public static HiveInterruptCallback remove(HiveInterruptCallback command) {
    synchronized (interruptCallbacks) {
      interruptCallbacks.remove(command);
    }
    return command;
  }

  /**
   * Request interruption of current hive command
   */
  public static void interrupt() {
    synchronized (interruptCallbacks) {
      for (HiveInterruptCallback resource : new ArrayList<HiveInterruptCallback>(interruptCallbacks)) {
        resource.interrupt();
      }
    }
  }

  /**
   * Checks if the current thread has been interrupted and throws RuntimeException is it has.
   */
  public static void checkInterrupted() {
    if (Thread.currentThread().isInterrupted()) {
      InterruptedException interrupt = null;
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        interrupt = e;
      }
      throw new RuntimeException("Interrupted", interrupt);
    }
  }
}
