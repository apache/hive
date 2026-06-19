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

package org.apache.hive.common.util;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This is just a wrapper around hadoop's ShutdownHookManager but also manages delete on exit hook for temp files.
 */
public class ShutdownHookManager {

  private static final org.apache.hadoop.util.ShutdownHookManager MGR = org.apache.hadoop.util.ShutdownHookManager.get();

  private static final DeleteOnExitHook DELETE_ON_EXIT_HOOK = new DeleteOnExitHook();

  static final private Logger LOG = LoggerFactory.getLogger(ShutdownHookManager.class.getName());

  // The graceful shutdown hook's priority must be higher than other hooks'
  public static final int GRACEFUL_SHUTDOWN_HOOK_PRIORITY = 1000;

  // The higher the priority the earlier will run. ShutdownHooks with same priority run
  // in a non-deterministic order.
  public static final int DEFAULT_SHUTDOWN_HOOK_PRIORITY = FileSystem.SHUTDOWN_HOOK_PRIORITY;

  public static final int DELETE_ON_EXIT_HOOK_PRIORITY = -1;

  static {
    MGR.addShutdownHook(DELETE_ON_EXIT_HOOK, DELETE_ON_EXIT_HOOK_PRIORITY);
  }

  /**
   * Adds shutdown hook with default priority (10)
   * @param shutdownHook - shutdown hook
   */
  public static void addShutdownHook(Runnable shutdownHook) {
    addShutdownHook(shutdownHook, DEFAULT_SHUTDOWN_HOOK_PRIORITY);
  }

  /**
   * Adds shutdown hook with a priority and default timeout (30s)
   * @param shutdownHook shutdown hook
   * @param priority priority of the shutdownHook
   */
  public static void addShutdownHook(Runnable shutdownHook, int priority) {
    addShutdownHook(shutdownHook, priority, 30);
  }

  /**
   * Adds a server's graceful shutdown hook with the highest priority
   * and the given timeout, so the hook runs earlier than other kinds of hooks.
   * @param shutdownHook shutdown hook
   * @param timeout timeout of the shutdownHook
   */
  public static void addGracefulShutDownHook(Runnable shutdownHook, long timeout) {
    addShutdownHook(shutdownHook, GRACEFUL_SHUTDOWN_HOOK_PRIORITY, timeout);
  }

  /**
   * Adds a shutdownHook with a priority, the higher the priority
   * the earlier will run. ShutdownHooks with same priority run
   * in a non-deterministic order.
   *
   * @param shutdownHook shutdownHook <code>Runnable</code>
   * @param priority priority of the shutdownHook.
   * @param timeout timeout of the shutdownHook.
   */
  private static void addShutdownHook(Runnable shutdownHook, int priority, long timeout) {
    if (priority < 0 || priority > GRACEFUL_SHUTDOWN_HOOK_PRIORITY) {
      throw new IllegalArgumentException("Priority should be ranged between 0 and " +
          GRACEFUL_SHUTDOWN_HOOK_PRIORITY);
    }
    if (!isShutdownInProgress()) {
      MGR.addShutdownHook(shutdownHook, priority, timeout, TimeUnit.SECONDS);
    }
  }

  /**
   * Indicates if shutdown is in progress or not.
   *
   * @return TRUE if the shutdown is in progress, otherwise FALSE.
   */
  public static boolean isShutdownInProgress() {
    return MGR.isShutdownInProgress();
  }

  /**
   * Removes a shutdownHook.
   *
   * @param shutdownHook shutdownHook to remove.
   * @return TRUE if the shutdownHook was registered and removed,
   * FALSE otherwise (including when shutdownHook == null)
   */
  public static boolean removeShutdownHook(Runnable shutdownHook) {
    if (shutdownHook == null || isShutdownInProgress()) {
      return false;
    }
    return MGR.removeShutdownHook(shutdownHook);
  }

  /**
   * register file to delete-on-exit hook
   *
   * {@link org.apache.hadoop.hive.common.FileUtils#createTempFile}
   */
  public static void deleteOnExit(File file) {
    if (MGR.isShutdownInProgress()) {
      LOG.warn("Shutdown in progress, cannot add a deleteOnExit");
    }
    DELETE_ON_EXIT_HOOK.deleteTargets.add(file);
  }

  /**
   * deregister file from delete-on-exit hook
   */
  public static void cancelDeleteOnExit(File file) {
    if (MGR.isShutdownInProgress()) {
      LOG.warn("Shutdown in progress, cannot cancel a deleteOnExit");
    }
    DELETE_ON_EXIT_HOOK.deleteTargets.remove(file);
  }

  @VisibleForTesting
  static boolean isRegisteredToDeleteOnExit(File file) {
    return DELETE_ON_EXIT_HOOK.deleteTargets.contains(file);
  }

  private static class DeleteOnExitHook implements Runnable {
    private final Set<File> deleteTargets = Collections.synchronizedSet(new HashSet<File>());

    @Override
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE", justification = "Intended")
    public void run() {
      for (File deleteTarget : deleteTargets) {
        deleteTarget.delete();
      }
      deleteTargets.clear();
    }
  }
}
