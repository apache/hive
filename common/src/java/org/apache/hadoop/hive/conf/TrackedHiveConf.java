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
package org.apache.hadoop.hive.conf;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TrackedHiveConf is a HiveConf object where property changes are logged.
 */
public class TrackedHiveConf extends HiveConf {
  private static final Logger LOG = LoggerFactory.getLogger(TrackedHiveConf.class);

  protected TrackedHiveConf() {
    super();
  }

  @Deprecated
  protected TrackedHiveConf(Class<?> cls) {
    super(cls);
  }

  @Deprecated
  protected TrackedHiveConf(Configuration other, Class<?> cls) {
    super(other, cls);
  }

  @Deprecated
  protected TrackedHiveConf(HiveConf other) {
    super(other);
  }

  @Override
  public void set(String name, String value, String source) {
    LOG.info("'{}' changed: '{}' -> '{}' (thread: {})", name, get(name), value, Thread.currentThread().getId());
    LOG.info("Change stack", new RuntimeException("Fake exception, only for easy stack logging (set)"));
    super.set(name, value, source);
  }

  @Override
  public void unset(String name) {
    LOG.info("'{}' unset, current value: '{}' (thread: {})", name, get(name), Thread.currentThread().getId());
    LOG.info("Unset stack", new RuntimeException("Fake exception, only for easy stack logging (unset)"));
    super.unset(name);
  }

  @Override
  public void clear() {
    LOG.info("Configuration is cleared (thread: {})", Thread.currentThread().getId());
    LOG.info("Clear stack", new RuntimeException("Fake exception, only for easy stack logging (clear)"));
    super.clear();
  }
}
