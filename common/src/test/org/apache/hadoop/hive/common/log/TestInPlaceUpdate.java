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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
 * These tests verify the rendering layer integration between ProgressMonitor and
 * InPlaceUpdate.
 */
public class TestInPlaceUpdate {

  /**
   * Minimal ProgressMonitor stub — returns empty headers/rows/footer.
   * Queue metrics can be customized per test.
   */
  private static ProgressMonitor makeMonitor(String queueMetrics) {
    return new ProgressMonitor() {
      @Override
      public List<String> headers() {
        return Arrays.asList("VERTICES",
            "MODE",
            "STATUS",
            "TOTAL",
            "COMPLETED",
            "RUNNING",
            "PENDING",
            "FAILED",
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
   * Expected separator: 94 dashes (matches MIN_TERMINAL_WIDTH).
   */
  private static final String SEPARATOR =
      new String(new char[InPlaceUpdate.MIN_TERMINAL_WIDTH]).replace("\0", "-");


  /**
   * Test #1: When queueMetrics() returns a non-empty string, InPlaceUpdate.render() must
   * print a separator line immediately after the metrics block — so total separators
   * = 4 (VERTICES table) + 1 (after queue metrics) = 5.
   *
   * This is the MOST CRITICAL test - verifies the separator is printed after the metrics block.
   */
  @Test
  public void testSeparatorPrintedAfterQueueMetrics() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);

    // Updated to new 4-line format (no staleness in line 1)
    String metrics = """
        QUEUE: default
        MEMORY: 2.0/8.0 GB (25.00% used) | VCORES: 4/16 (25.00% used)
        CAPACITY: 60.00% (allocated) | 25.00% (used)
        APPS: 1 running, 0 pending | CONTAINERS: 2 allocated, 0 pending""";

    new InPlaceUpdate(ps).render(makeMonitor(metrics));
    ps.flush();

    String output = baos.toString();

    // The metrics content should appear
    assertTrue("Output should contain QUEUE: line", output.contains("QUEUE: default"));
    assertTrue("Output should contain MEMORY: line", output.contains("MEMORY: 2.0/8.0 GB"));
    assertTrue("Output should contain CAPACITY: line", output.contains("CAPACITY:"));
    assertTrue("Output should contain APPS: line", output.contains("APPS:"));

    // The separator must appear AFTER the queue metrics block in the rendered output
    // Find the last line of queue metrics (APPS: line)
    int appsIdx = output.indexOf("APPS:");
    assertTrue("APPS: line should be found in output", appsIdx > 0);

    // Separator should appear after APPS line
    assertTrue("Separator must appear after APPS: line",
        output.indexOf(SEPARATOR, appsIdx) > appsIdx);

    // Total separators = 4 (VERTICES table) + 1 (after queue metrics) = 5
    assertEquals("With queue metrics, total separators should be 5 (4 VERTICES + 1 after metrics)",
        5, StringUtils.countMatches(output, SEPARATOR));
  }


  /**
   * Test #2: When queueMetrics() returns an empty string, InPlaceUpdate.render() must NOT
   * print an extra separator — so total remains 4 (the VERTICES table separators only).
   */
  @Test
  public void testNoExtraSeparatorWhenQueueMetricsEmpty() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);

    new InPlaceUpdate(ps).render(makeMonitor(""));
    ps.flush();

    String output = baos.toString();

    // VERTICES table renders 4 separators (before-header, after-header, before-footer, after-footer)
    // With empty queueMetrics there should be exactly 4, not 5.
    assertEquals("With empty queue metrics, only 4 VERTICES-table separators should appear",
        4, StringUtils.countMatches(output, SEPARATOR));
  }


  /**
   * Test #3: When queueMetrics() returns null, behaviour should be identical to empty.
   */
  @Test
  public void testNoExtraSeparatorWhenQueueMetricsNull() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);

    new InPlaceUpdate(ps).render(makeMonitor(null));
    ps.flush();

    String output = baos.toString();

    assertEquals("With null queue metrics, only 4 VERTICES-table separators should appear",
        4, StringUtils.countMatches(output, SEPARATOR));
  }

  /**
   * Test #4: Verify separator constant length matches MIN_TERMINAL_WIDTH.
   */
  @Test
  public void testSeparatorLengthEqualsMinTerminalWidth() {
    assertTrue("Separator should consist only of dashes with length = MIN_TERMINAL_WIDTH",
        SEPARATOR.matches("-{" + InPlaceUpdate.MIN_TERMINAL_WIDTH + "}"));
  }
}

