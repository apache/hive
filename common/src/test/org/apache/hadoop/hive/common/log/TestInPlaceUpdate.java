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
package org.apache.hadoop.hive.common.log;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for InPlaceUpdate
 * <p>
 * We capture stdout via a ByteArrayOutputStream and inspect the rendered output.
 */

public class TestInPlaceUpdate {

  /**
   * Minimal ProgressMonitor stub — returns empty headers/rows/footer.
   */
  private static ProgressMonitor makeMonitor(String queueMetrics) {
    return new ProgressMonitor() {
      @Override
      public List<String> headers() {
        return Arrays.asList("VERTICES", "MODE", "STATUS", "TOTAL", "COMPLETED", "RUNNING", "PENDING", "FAILED",
            "KILLED");
      }

      @Override
      public List<List<String>> rows() {
        return Collections.emptyList();
      }

      @Override
      public String footerSummary() {
        return "VERTICES: 00/00";
      }

      @Override
      public long startTime() {
        return System.currentTimeMillis();
      }

      @Override
      public double progressedPercentage() {
        return 0.0;
      }

      @Override
      public String executionStatus() {
        return "RUNNING";
      }

      @Override
      public String queueMetrics() {
        return queueMetrics;
      }
    };
  }

  /**
   * Expected separator: 94 dashes.
   */
  private static final String SEPARATOR = new String(new char[InPlaceUpdate.MIN_TERMINAL_WIDTH]).replace("\0", "-");

  /**
   * When queueMetrics() returns a non-empty string, InPlaceUpdate.render() must
   * print a separator line immediately after the metrics block — so total separators
   * = 4 (VERTICES table) + 1 (after queue metrics) = 5.
   */
  @Test
  public void testSeparatorPrintedAfterQueueMetrics() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    InPlaceUpdate inPlace = new InPlaceUpdate(ps);

    String metrics = """
        QUEUE: default (1s ago)
        MEMORY: 2.0/8.0 GB (25.00%) | VCORES: 4/16 (25.00%)
        CAPACITY: 60.00% | ACTIVE_APPS: 1 | PENDING: 0""";

    inPlace.render(makeMonitor(metrics));
    ps.flush();

    String output = baos.toString();

    // The metrics content should appear
    assertTrue("Output should contain QUEUE: line", output.contains("QUEUE: default"));
    assertTrue("Output should contain MEMORY: line", output.contains("MEMORY: 2.0/8.0 GB"));
    assertTrue("Output should contain CAPACITY: line", output.contains("CAPACITY: 60.00%"));

    // The separator must appear AFTER the CAPACITY line in the rendered output
    int capacityIdx = output.indexOf("CAPACITY:");
    int separatorIdx = output.indexOf(SEPARATOR, capacityIdx);
    assertTrue(
        "Separator must appear after CAPACITY: line (separatorIdx=" + separatorIdx + ", capacityIdx="
            + capacityIdx + ")",
        separatorIdx > capacityIdx);

    // Total separators = 4 (VERTICES table) + 1 (after queue metrics) = 5
    int count = StringUtils.countMatches(output, SEPARATOR);
    assertEquals("With queue metrics, total separators should be 5 (4 VERTICES + 1 after metrics)", 5, count);
  }

  /**
   * When queueMetrics() returns an empty string, InPlaceUpdate.render() must NOT
   * print an extra separator — so total remains 4 (the VERTICES table separators only).
   */
  @Test
  public void testNoExtraSeparatorWhenQueueMetricsEmpty() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    InPlaceUpdate inPlace = new InPlaceUpdate(ps);

    inPlace.render(makeMonitor(""));
    ps.flush();

    String output = baos.toString();

    // VERTICES table renders 4 separators (before-header, after-header, before-footer, after-footer)
    // With empty queueMetrics there should be exactly 4, not 5.
    int count = StringUtils.countMatches(output, SEPARATOR);
    assertEquals("With empty queue metrics, only 4 VERTICES-table separators should appear", 4, count);
  }

  /**
   * When queueMetrics() returns null, behaviour should be identical to empty.
   */
  @Test
  public void testNoExtraSeparatorWhenQueueMetricsNull() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    InPlaceUpdate inPlace = new InPlaceUpdate(ps);

    inPlace.render(makeMonitor(null));
    ps.flush();

    String output = baos.toString();

    int count = StringUtils.countMatches(output, SEPARATOR);
    assertEquals("With null queue metrics, only 4 VERTICES-table separators should appear", 4, count);
  }

  // ── Verify separator constant length ────────────────────────────────────────

  @Test
  public void testSeparatorLengthEqualsMinTerminalWidth() {
    assertTrue("Separator should consist only of dashes",
        SEPARATOR.matches("-{" + InPlaceUpdate.MIN_TERMINAL_WIDTH + "}"));
  }
}
