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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import jline.TerminalFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.fusesource.jansi.Ansi;

import javax.annotation.Nullable;
import java.io.PrintStream;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.util.List;

import static org.fusesource.jansi.Ansi.ansi;
import static org.fusesource.jansi.internal.CLibrary.*;

/**
 * Renders information from ProgressMonitor to the stream provided.
 */
public class InPlaceUpdate {

  public static final int MIN_TERMINAL_WIDTH = 94;

  // keep this within 80 chars width. If more columns needs to be added then update min terminal
  // width requirement and SEPARATOR width accordingly
  private static final String HEADER_FORMAT = "%16s%10s %13s  %5s  %9s  %7s  %7s  %6s  %6s  ";
  private static final String VERTEX_FORMAT = "%-16s%10s %13s  %5s  %9s  %7s  %7s  %6s  %6s  ";
  private static final String FOOTER_FORMAT = "%-15s  %-30s %-4s  %-25s";

  private static final int PROGRESS_BAR_CHARS = 30;
  private static final String SEPARATOR = new String(new char[MIN_TERMINAL_WIDTH]).replace("\0", "-");

  /* Pretty print the values */
  private final DecimalFormat secondsFormatter = new DecimalFormat("#0.00");
  private int lines = 0;
  private PrintStream out;

  public InPlaceUpdate(PrintStream out) {
    this.out = out;
  }

  public InPlaceUpdate() {
    this(System.out);
  }

  public static void reprintLine(PrintStream out, String line) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).a(line).a('\n').toString());
    out.flush();
  }

  public static void rePositionCursor(PrintStream ps) {
    ps.print(ansi().cursorUp(0).toString());
    ps.flush();
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given line.
   *
   * @param line - line to print
   */
  private void reprintLine(String line) {
    reprintLine(out, line);
    lines++;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given line with the specified color.
   *
   * @param line  - line to print
   * @param color - color for the line
   */
  private void reprintLineWithColorAsBold(String line, Ansi.Color color) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).fg(color).bold().a(line).a('\n').boldOff().reset()
      .toString());
    out.flush();
    lines++;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given multiline. Make sure the specified line is not
   * terminated by linebreak.
   *
   * @param line - line to print
   */
  private void reprintMultiLine(String line) {
    int numLines = line.split("\r\n|\r|\n").length;
    out.print(ansi().eraseLine(Ansi.Erase.ALL).a(line).a('\n').toString());
    out.flush();
    lines += numLines;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Repositions the cursor back to line 0.
   */
  private void repositionCursor() {
    if (lines > 0) {
      out.print(ansi().cursorUp(lines).toString());
      out.flush();
      lines = 0;
    }
  }


  // [==================>>-----]
  private String getInPlaceProgressBar(double percent) {
    StringWriter bar = new StringWriter();
    bar.append("[");
    int remainingChars = PROGRESS_BAR_CHARS - 4;
    int completed = (int) (remainingChars * percent);
    int pending = remainingChars - completed;
    for (int i = 0; i < completed; i++) {
      bar.append("=");
    }
    bar.append(">>");
    for (int i = 0; i < pending; i++) {
      bar.append("-");
    }
    bar.append("]");
    return bar.toString();
  }

  public void render(ProgressMonitor monitor) {
    if (monitor == null) return;
    // position the cursor to line 0
    repositionCursor();

    // print header
    // -------------------------------------------------------------------------------
    //         VERTICES     STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
    // -------------------------------------------------------------------------------
    reprintLine(SEPARATOR);
    reprintLineWithColorAsBold(String.format(HEADER_FORMAT, monitor.headers().toArray()),
      Ansi.Color.CYAN);
    reprintLine(SEPARATOR);


    // Map 1 .......... container  SUCCEEDED      7          7        0        0       0       0
    List<String> printReady = Lists.transform(monitor.rows(), new Function<List<String>, String>() {
      @Nullable
      @Override
      public String apply(@Nullable List<String> row) {
        return String.format(VERTEX_FORMAT, row.toArray());
      }
    });
    reprintMultiLine(StringUtils.join(printReady, "\n"));

    // -------------------------------------------------------------------------------
    // VERTICES: 03/04            [=================>>-----] 86%  ELAPSED TIME: 1.71 s
    // -------------------------------------------------------------------------------
    String progressStr = "" + (int) (monitor.progressedPercentage() * 100) + "%";
    float et = (float) (System.currentTimeMillis() - monitor.startTime()) / (float) 1000;
    String elapsedTime = "ELAPSED TIME: " + secondsFormatter.format(et) + " s";
    String footer = String.format(
      FOOTER_FORMAT,
      monitor.footerSummary(),
      getInPlaceProgressBar(monitor.progressedPercentage()),
      progressStr,
      elapsedTime);

    reprintLine(SEPARATOR);
    reprintLineWithColorAsBold(footer, Ansi.Color.RED);
    reprintLine(SEPARATOR);
  }


  public static boolean canRenderInPlace(HiveConf conf) {
    String engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
    boolean inPlaceUpdates = false;

    if (engine.equals("tez")) {
      inPlaceUpdates = HiveConf.getBoolVar(conf, HiveConf.ConfVars.TEZ_EXEC_INPLACE_PROGRESS);
    }

    if (engine.equals("spark")) {
      inPlaceUpdates = HiveConf.getBoolVar(conf, HiveConf.ConfVars.SPARK_EXEC_INPLACE_PROGRESS);
    }

    // we need at least 80 chars wide terminal to display in-place updates properly
    return inPlaceUpdates && isUnixTerminal() && TerminalFactory.get().getWidth() >= MIN_TERMINAL_WIDTH;
  }

  private static boolean isUnixTerminal() {

    String os = System.getProperty("os.name");
    if (os.startsWith("Windows")) {
      // we do not support Windows, we will revisit this if we really need it for windows.
      return false;
    }

    // We must be on some unix variant..
    // check if standard out is a terminal
    try {
      // isatty system call will return 1 if the file descriptor is terminal else 0
      if (isatty(STDOUT_FILENO) == 0) {
        return false;
      }
      if (isatty(STDERR_FILENO) == 0) {
        return false;
      }
    } catch (NoClassDefFoundError | UnsatisfiedLinkError ignore) {
      // These errors happen if the JNI lib is not available for your platform.
      return false;
    }
    return true;
  }
}
