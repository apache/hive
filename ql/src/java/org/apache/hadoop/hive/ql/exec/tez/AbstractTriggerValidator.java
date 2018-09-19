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

package org.apache.hadoop.hive.ql.exec.tez;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public abstract class AbstractTriggerValidator {
  private ScheduledExecutorService scheduledExecutorService = null;
  abstract Runnable getTriggerValidatorRunnable();

  void startTriggerValidator(long triggerValidationIntervalMs) {
    if (scheduledExecutorService == null) {
      scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TriggerValidator").build());
      Runnable triggerValidatorRunnable = getTriggerValidatorRunnable();
      scheduledExecutorService.scheduleWithFixedDelay(triggerValidatorRunnable, triggerValidationIntervalMs,
        triggerValidationIntervalMs, TimeUnit.MILLISECONDS);
      TezSessionPoolSession.LOG.info("Started trigger validator with interval: {} ms", triggerValidationIntervalMs);
    }
  }

  void stopTriggerValidator() {
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
      scheduledExecutorService = null;
      TezSessionPoolSession.LOG.info("Stopped trigger validator");
    }
  }
}