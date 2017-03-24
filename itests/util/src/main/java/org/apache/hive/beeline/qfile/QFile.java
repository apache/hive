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

package org.apache.hive.beeline.qfile;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.util.Shell;
import org.apache.hive.common.util.StreamPrinter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Class for representing a Query and the connected files. It provides accessors for the specific
 * input and output files, and provides methods for filtering the output of the runs.
 */
public final class QFile {
  private static final String DEBUG_HINT =
      "The following files can help you identifying the problem:\n"
      + " - Query file: %1\n"
      + " - Raw output file: %2\n"
      + " - Filtered output file: %3\n"
      + " - Expected output file: %4\n"
      + " - Client log file: %5\n"
      + " - Client log files before the test: %6\n"
      + " - Client log files after the test: %7\n"
      + " - Hiveserver2 log file: %8\n";

  private String name;
  private File inputFile;
  private File rawOutputFile;
  private File outputFile;
  private File expectedOutputFile;
  private File logFile;
  private File beforeExecuteLogFile;
  private File afterExecuteLogFile;
  private static RegexFilterSet staticFilterSet = getStaticFilterSet();
  private RegexFilterSet specificFilterSet;

  private QFile() {}

  public String getName() {
    return name;
  }

  public File getInputFile() {
    return inputFile;
  }

  public File getRawOutputFile() {
    return rawOutputFile;
  }

  public File getOutputFile() {
    return outputFile;
  }

  public File getExpectedOutputFile() {
    return expectedOutputFile;
  }

  public File getLogFile() {
    return logFile;
  }

  public File getBeforeExecuteLogFile() {
    return beforeExecuteLogFile;
  }

  public File getAfterExecuteLogFile() {
    return afterExecuteLogFile;
  }

  public String getDebugHint() {
    return String.format(DEBUG_HINT, inputFile, rawOutputFile, outputFile, expectedOutputFile,
        logFile, beforeExecuteLogFile, afterExecuteLogFile, "./itests/qtest/target/tmp/hive.log");
  }

  public void filterOutput() throws IOException {
    String rawOutput = FileUtils.readFileToString(rawOutputFile);
    String filteredOutput = staticFilterSet.filter(specificFilterSet.filter(rawOutput));
    FileUtils.writeStringToFile(outputFile, filteredOutput);
  }

  public QTestProcessExecResult compareResults() throws IOException, InterruptedException {
    if (!expectedOutputFile.exists()) {
      throw new IOException("Expected results file does not exist: " + expectedOutputFile);
    }
    return executeDiff();
  }

  public void overwriteResults() throws IOException {
    FileUtils.copyFile(outputFile, expectedOutputFile);
  }

  private QTestProcessExecResult executeDiff() throws IOException, InterruptedException {
    List<String> diffCommandArgs = new ArrayList<String>();
    diffCommandArgs.add("diff");

    // Text file comparison
    diffCommandArgs.add("-a");

    if (Shell.WINDOWS) {
      // Ignore changes in the amount of white space
      diffCommandArgs.add("-b");

      // Files created on Windows machines have different line endings
      // than files created on Unix/Linux. Windows uses carriage return and line feed
      // ("\r\n") as a line ending, whereas Unix uses just line feed ("\n").
      // Also StringBuilder.toString(), Stream to String conversions adds extra
      // spaces at the end of the line.
      diffCommandArgs.add("--strip-trailing-cr"); // Strip trailing carriage return on input
      diffCommandArgs.add("-B"); // Ignore changes whose lines are all blank
    }

    // Add files to compare to the arguments list
    diffCommandArgs.add(getQuotedString(expectedOutputFile));
    diffCommandArgs.add(getQuotedString(outputFile));

    System.out.println("Running: " + org.apache.commons.lang.StringUtils.join(diffCommandArgs,
        ' '));
    Process executor = Runtime.getRuntime().exec(diffCommandArgs.toArray(
        new String[diffCommandArgs.size()]));

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bos, true, "UTF-8");

    StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);
    StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out, out);

    outPrinter.start();
    errPrinter.start();

    int result = executor.waitFor();

    outPrinter.join();
    errPrinter.join();

    executor.waitFor();

    return QTestProcessExecResult.create(result, new String(bos.toByteArray(),
        StandardCharsets.UTF_8));
  }

  private static String getQuotedString(File file) {
    return Shell.WINDOWS ? String.format("\"%s\"", file.getAbsolutePath()) : file.getAbsolutePath();
  }

  private static class RegexFilterSet {
    private final Map<Pattern, String> regexFilters = new LinkedHashMap<Pattern, String>();

    public RegexFilterSet addFilter(String regex, String replacement) {
      regexFilters.put(Pattern.compile(regex), replacement);
      return this;
    }

    public String filter(String input) {
      for (Pattern pattern : regexFilters.keySet()) {
        input = pattern.matcher(input).replaceAll(regexFilters.get(pattern));
      }
      return input;
    }
  }

  // These are the filters which are common for every QTest.
  // Check specificFilterSet for QTest specific ones.
  private static RegexFilterSet getStaticFilterSet() {
    // Extract the leading four digits from the unix time value.
    // Use this as a prefix in order to increase the selectivity
    // of the unix time stamp replacement regex.
    String currentTimePrefix = Long.toString(System.currentTimeMillis()).substring(0, 4);

    String userName = System.getProperty("user.name");

    String timePattern = "(Mon|Tue|Wed|Thu|Fri|Sat|Sun) "
        + "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) "
        + "\\d{2} \\d{2}:\\d{2}:\\d{2} \\w+ 20\\d{2}";
    // Pattern to remove the timestamp and other infrastructural info from the out file
    String logPattern = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d*\\s+\\S+\\s+\\[" +
        ".*\\]\\s+\\S+:\\s+";
    String operatorPattern = "\"(CONDITION|COPY|DEPENDENCY_COLLECTION|DDL"
        + "|EXPLAIN|FETCH|FIL|FS|FUNCTION|GBY|HASHTABLEDUMMY|HASTTABLESINK|JOIN"
        + "|LATERALVIEWFORWARD|LIM|LVJ|MAP|MAPJOIN|MAPRED|MAPREDLOCAL|MOVE|OP|RS"
        + "|SCR|SEL|STATS|TS|UDTF|UNION)_\\d+\"";

    return new RegexFilterSet()
        .addFilter(logPattern, "")
        .addFilter("(?s)\nWaiting to acquire compile lock:.*?Acquired the compile lock.\n",
            "\nAcquired the compile lock.\n")
        .addFilter("Getting log thread is interrupted, since query is done!\n", "")
        .addFilter("going to print operations logs\n", "")
        .addFilter("printed operations logs\n", "")
        .addFilter("\\(queryId=[^\\)]*\\)", "queryId=(!!{queryId}!!)")
        .addFilter("file:/\\w\\S+", "file:/!!ELIDED!!")
        .addFilter("pfile:/\\w\\S+", "pfile:/!!ELIDED!!")
        .addFilter("hdfs:/\\w\\S+", "hdfs:/!!ELIDED!!")
        .addFilter("last_modified_by=\\w+", "last_modified_by=!!ELIDED!!")
        .addFilter(timePattern, "!!TIMESTAMP!!")
        .addFilter("(\\D)" + currentTimePrefix + "\\d{6}(\\D)", "$1!!UNIXTIME!!$2")
        .addFilter("(\\D)" + currentTimePrefix + "\\d{9}(\\D)", "$1!!UNIXTIMEMILLIS!!$2")
        .addFilter(userName, "!!{user.name}!!")
        .addFilter(operatorPattern, "\"$1_!!ELIDED!!\"")
        .addFilter("Time taken: [0-9\\.]* seconds", "Time taken: !!ELIDED!! seconds");
  }

  /**
   * Builder to generate QFile objects. After initializing the builder it is possible the
   * generate the next QFile object using it's name only.
   */
  public static class QFileBuilder {
    private File queryDirectory;
    private File logDirectory;
    private File resultsDirectory;
    private String scratchDirectoryString;
    private String warehouseDirectoryString;
    private File hiveRootDirectory;

    public QFileBuilder() {
    }

    public QFileBuilder setQueryDirectory(File queryDirectory) {
      this.queryDirectory = queryDirectory;
      return this;
    }

    public QFileBuilder setLogDirectory(File logDirectory) {
      this.logDirectory = logDirectory;
      return this;
    }

    public QFileBuilder setResultsDirectory(File resultsDirectory) {
      this.resultsDirectory = resultsDirectory;
      return this;
    }

    public QFileBuilder setScratchDirectoryString(String scratchDirectoryString) {
      this.scratchDirectoryString = scratchDirectoryString;
      return this;
    }

    public QFileBuilder setWarehouseDirectoryString(String warehouseDirectoryString) {
      this.warehouseDirectoryString = warehouseDirectoryString;
      return this;
    }

    public QFileBuilder setHiveRootDirectory(File hiveRootDirectory) {
      this.hiveRootDirectory = hiveRootDirectory;
      return this;
    }

    public QFile getQFile(String name) throws IOException {
      QFile result = new QFile();
      result.name = name;
      result.inputFile = new File(queryDirectory, name + ".q");
      result.rawOutputFile = new File(logDirectory, name + ".q.out.raw");
      result.outputFile = new File(logDirectory, name + ".q.out");
      result.expectedOutputFile = new File(resultsDirectory, name + ".q.out");
      result.logFile = new File(logDirectory, name + ".q.beeline");
      result.beforeExecuteLogFile = new File(logDirectory, name + ".q.beforeExecute.log");
      result.afterExecuteLogFile = new File(logDirectory, name + ".q.afterExecute.log");
      // These are the filters which are specific for the given QTest.
      // Check staticFilterSet for common filters.
      result.specificFilterSet = new RegexFilterSet()
          .addFilter(scratchDirectoryString + "[\\w\\-/]+", "!!{hive.exec.scratchdir}!!")
          .addFilter(warehouseDirectoryString, "!!{hive.metastore.warehouse.dir}!!")
          .addFilter(resultsDirectory.getAbsolutePath(), "!!{expectedDirectory}!!")
          .addFilter(logDirectory.getAbsolutePath(), "!!{outputDirectory}!!")
          .addFilter(queryDirectory.getAbsolutePath(), "!!{qFileDirectory}!!")
          .addFilter(hiveRootDirectory.getAbsolutePath(), "!!{hive.root}!!");
      return result;
    }
  }
}
