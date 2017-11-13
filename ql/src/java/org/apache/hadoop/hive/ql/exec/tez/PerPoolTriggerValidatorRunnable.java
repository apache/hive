/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.ql.wm.SessionTriggerProvider;
import org.apache.hadoop.hive.ql.wm.TriggerActionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerPoolTriggerValidatorRunnable implements Runnable {
  protected static transient Logger LOG = LoggerFactory.getLogger(PerPoolTriggerValidatorRunnable.class);
  private final Map<String, SessionTriggerProvider> sessionTriggerProviders;
  private final TriggerActionHandler triggerActionHandler;
  private final Map<String, TriggerValidatorRunnable> poolValidators;
  private final long triggerValidationIntervalMs;

  PerPoolTriggerValidatorRunnable(final Map<String, SessionTriggerProvider> sessionTriggerProviders,
    final TriggerActionHandler triggerActionHandler,
    final long triggerValidationIntervalMs) {
    this.sessionTriggerProviders = sessionTriggerProviders;
    this.triggerActionHandler = triggerActionHandler;
    this.poolValidators = new HashMap<>();
    this.triggerValidationIntervalMs = triggerValidationIntervalMs;
  }

  @Override
  public void run() {
    try {
      ScheduledExecutorService validatorExecutorService = Executors
        .newScheduledThreadPool(sessionTriggerProviders.size());
      for (Map.Entry<String, SessionTriggerProvider> entry : sessionTriggerProviders.entrySet()) {
        String poolName = entry.getKey();
        if (!poolValidators.containsKey(poolName)) {
          LOG.info("Creating trigger validator for pool: {}", poolName);
          TriggerValidatorRunnable poolValidator = new TriggerValidatorRunnable(entry.getValue(), triggerActionHandler);
          validatorExecutorService.scheduleWithFixedDelay(poolValidator, triggerValidationIntervalMs,
            triggerValidationIntervalMs, TimeUnit.MILLISECONDS);
          poolValidators.put(poolName, poolValidator);
        }
      }
    } catch (Throwable t) {
      // if exception is thrown in scheduled tasks, no further tasks will be scheduled, hence this ugly catch
      LOG.warn(PerPoolTriggerValidatorRunnable.class.getSimpleName() + " caught exception.", t);
    }
  }
}
