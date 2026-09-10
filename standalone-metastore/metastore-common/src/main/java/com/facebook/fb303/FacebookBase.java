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

package com.facebook.fb303;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.thrift.TException;

/**
 * Server-side base class for FacebookService implementations.
 *
 * <p>This class was historically shipped as part of Apache Thrift's
 * {@code libfb303} artifact.  As of THRIFT-4613, upstream Apache Thrift no
 * longer publishes {@code libfb303}, so a minimal implementation is vendored
 * here alongside the vendored {@code fb303.thrift} IDL.  The behavior mirrors
 * the historical {@code libfb303} implementation: it tracks a service name,
 * a startup timestamp, and an in-memory counter map, and provides no-op
 * defaults for the remaining {@link FacebookService.Iface} methods so
 * subclasses only need to override the ones they care about.
 */
public abstract class FacebookBase implements FacebookService.Iface {

  private final String name;
  private final long aliveSince;
  private final ConcurrentMap<String, Long> counters = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, String> options = new ConcurrentHashMap<>();

  protected FacebookBase(String name) {
    this.name = name;
    this.aliveSince = System.currentTimeMillis() / 1000L;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getVersion() throws TException {
    return "";
  }

  @Override
  public fb_status getStatus() {
    return fb_status.ALIVE;
  }

  @Override
  public String getStatusDetails() {
    return "";
  }

  /**
   * Increments the named counter by one and returns the new value.
   * Kept public to match the historical libfb303 signature so subclasses
   * in other packages (see {@code BaseHandler}) can call it.
   */
  public long incrementCounter(String key) {
    return counters.merge(key, 1L, Long::sum);
  }

  /**
   * Explicitly sets the value of a counter.
   */
  public void setCounter(String key, long value) {
    counters.put(key, value);
  }

  @Override
  public AbstractMap<String, Long> getCounters() {
    // Return a snapshot so callers can iterate without seeing concurrent updates.
    return new HashMap<>(counters);
  }

  @Override
  public long getCounter(String key) {
    Long value = counters.get(key);
    return value == null ? 0L : value;
  }

  @Override
  public void setOption(String key, String value) {
    options.put(key, value);
  }

  @Override
  public String getOption(String key) {
    String value = options.get(key);
    return value == null ? "" : value;
  }

  @Override
  public Map<String, String> getOptions() {
    return new HashMap<>(options);
  }

  @Override
  public String getCpuProfile(int profileDurationInSec) throws TException {
    return "";
  }

  @Override
  public long aliveSince() {
    return aliveSince;
  }

  @Override
  public void reinitialize() {
    // No-op by default; subclasses may override.
  }

  @Override
  public void shutdown() {
    // No-op by default; subclasses may override.
  }
}
