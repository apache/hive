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

package org.apache.hive.kubernetes.operator.autoscaling;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * HPA-like stabilization window that smooths scaling decisions.
 * Keeps a sliding window of desired replica samples and returns
 * the max (for scale-up) or min (for scale-down) over the window.
 */
public class StabilizationWindow {

  private record Sample(Instant timestamp, int value) {
  }

  private final Deque<Sample> samples = new ArrayDeque<>();
  private final Duration window;

  public StabilizationWindow(Duration window) {
    this.window = window;
  }

  /** Record a new desired replica sample. */
  public void record(int desiredReplicas) {
    Instant now = Instant.now();
    evictExpired(now);
    samples.addLast(new Sample(now, desiredReplicas));
  }

  /** Returns the maximum value in the window (used for scale-up decisions). */
  public int stabilizedMax() {
    evictExpired(Instant.now());
    return samples.stream().mapToInt(Sample::value).max().orElse(0);
  }

  /** Returns the minimum value in the window (used for scale-down decisions). */
  public int stabilizedMin() {
    evictExpired(Instant.now());
    return samples.stream().mapToInt(Sample::value).min().orElse(0);
  }

  /** Returns true if the window has at least one sample. */
  public boolean hasSamples() {
    evictExpired(Instant.now());
    return !samples.isEmpty();
  }

  private void evictExpired(Instant now) {
    Instant cutoff = now.minus(window);
    while (!samples.isEmpty() && samples.peekFirst().timestamp().isBefore(cutoff)) {
      samples.pollFirst();
    }
  }
}
