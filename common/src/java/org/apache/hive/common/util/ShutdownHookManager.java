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

package org.apache.hive.common.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The <code>ShutdownHookManager</code> enables running shutdownHook
 * in a deterministic order, higher priority first.
 * <p/>
 * The JVM runs ShutdownHooks in a non-deterministic order or in parallel.
 * This class registers a single JVM shutdownHook and run all the
 * shutdownHooks registered to it (to this class) in order based on their
 * priority.
 *
 * Originally taken from o.a.hadoop.util.ShutdownHookManager
 */
public class ShutdownHookManager {

  private static final ShutdownHookManager MGR = new ShutdownHookManager();

  private static final Log LOG = LogFactory.getLog(ShutdownHookManager.class);

  static {
    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        @Override
        public void run() {
          MGR.shutdownInProgress.set(true);
          for (Runnable hook: MGR.getShutdownHooksInOrder()) {
            try {
              hook.run();
            } catch (Throwable ex) {
              LOG.warn("ShutdownHook '" + hook.getClass().getSimpleName() +
                       "' failed, " + ex.toString(), ex);
            }
          }
        }
      }
    );
  }


  /**
   * Private structure to store ShutdownHook and its priority.
   */
  private static class HookEntry {
    Runnable hook;
    int priority;

    public HookEntry(Runnable hook, int priority) {
      this.hook = hook;
      this.priority = priority;
    }

    @Override
    public int hashCode() {
      return hook.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      boolean eq = false;
      if (obj != null) {
        if (obj instanceof HookEntry) {
          eq = (hook == ((HookEntry)obj).hook);
        }
      }
      return eq;
    }

  }

  private final Set<HookEntry> hooks =
    Collections.synchronizedSet(new HashSet<HookEntry>());

  private final AtomicBoolean shutdownInProgress = new AtomicBoolean(false);

  //private to constructor to ensure singularity
  private ShutdownHookManager() {
  }

  /**
   * Returns the list of shutdownHooks in order of execution,
   * Highest priority first.
   *
   * @return the list of shutdownHooks in order of execution.
   */
  static List<Runnable> getShutdownHooksInOrder() {
    return MGR.getShutdownHooksInOrderInternal();
  }

  List<Runnable> getShutdownHooksInOrderInternal() {
    List<HookEntry> list;
    synchronized (MGR.hooks) {
      list = new ArrayList<HookEntry>(MGR.hooks);
    }
    Collections.sort(list, new Comparator<HookEntry>() {

      //reversing comparison so highest priority hooks are first
      @Override
      public int compare(HookEntry o1, HookEntry o2) {
        return o2.priority - o1.priority;
      }
    });
    List<Runnable> ordered = new ArrayList<Runnable>();
    for (HookEntry entry: list) {
      ordered.add(entry.hook);
    }
    return ordered;
  }


  /**
   * Adds a shutdownHook with a priority, the higher the priority
   * the earlier will run. ShutdownHooks with same priority run
   * in a non-deterministic order.
   *
   * @param shutdownHook shutdownHook <code>Runnable</code>
   * @param priority priority of the shutdownHook.
   */
  public static void addShutdownHook(Runnable shutdownHook, int priority) {
    MGR.addShutdownHookInternal(shutdownHook, priority);
  }

  private void addShutdownHookInternal(Runnable shutdownHook, int priority) {
    if (shutdownHook == null) {
      throw new IllegalArgumentException("shutdownHook cannot be NULL");
    }
    if (shutdownInProgress.get()) {
      throw new IllegalStateException("Shutdown in progress, cannot add a shutdownHook");
    }
    hooks.add(new HookEntry(shutdownHook, priority));
  }

  /**
   * Removes a shutdownHook.
   *
   * @param shutdownHook shutdownHook to remove.
   * @return TRUE if the shutdownHook was registered and removed,
   * FALSE otherwise.
   */
  public static boolean removeShutdownHook(Runnable shutdownHook) {
    return MGR.removeShutdownHookInternal(shutdownHook);
  }

  private boolean removeShutdownHookInternal(Runnable shutdownHook) {
    if (shutdownInProgress.get()) {
      throw new IllegalStateException("Shutdown in progress, cannot remove a shutdownHook");
    }
    return hooks.remove(new HookEntry(shutdownHook, 0));
  }

  /**
   * Indicates if a shutdownHook is registered or not.
   *
   * @param shutdownHook shutdownHook to check if registered.
   * @return TRUE/FALSE depending if the shutdownHook is is registered.
   */
  public static boolean hasShutdownHook(Runnable shutdownHook) {
    return MGR.hasShutdownHookInternal(shutdownHook);
  }

  public boolean hasShutdownHookInternal(Runnable shutdownHook) {
    return hooks.contains(new HookEntry(shutdownHook, 0));
  }

  /**
   * Indicates if shutdown is in progress or not.
   *
   * @return TRUE if the shutdown is in progress, otherwise FALSE.
   */
  public static boolean isShutdownInProgress() {
    return MGR.isShutdownInProgressInternal();
  }

  private boolean isShutdownInProgressInternal() {
    return shutdownInProgress.get();
  }
}
