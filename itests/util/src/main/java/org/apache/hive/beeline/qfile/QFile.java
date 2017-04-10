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

package org.apache.hive.beeline;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hive.common.util.StreamPrinter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Class for representing a Query and the connected files. It provides accessors for the specific
 * input and output files, and provides methods for filtering the output of the runs.
 */
public final class QFile {
  private static final Set<String> srcTables = QTestUtil.getSrcTables();
  private static final String DEBUG_HINT =
      "The following files can help you identifying the problem:%n"
      + " - Query file: %1s%n"
      + " - Raw output file: %2s%n"
      + " - Filtered output file: %3s%n"
      + " - Expected output file: %4s%n"
      + " - Client log file: %5s%n"
      + " - Client log files before the test: %6s%n"
      + " - Client log files after the test: %7s%n"
      + " - Hiveserver2 log file: %8s%n";
  private static final String USE_COMMAND_WARNING =
      "The query file %1s contains \"%2s\" command.%n"
      + "The source table name rewrite is turned on, so this might cause problems when the used "
      + "database contains tables named any of the following: " + srcTables + "%n"
      + "To turn off the table name rewrite use -Dtest.rewrite.source.tables=false%n";

  private static final Pattern USE_PATTERN =
      Pattern.compile("^\\s*use\\s.*", Pattern.CASE_INSENSITIVE);

  private String name;
  private File inputFile;
  private File rawOutputFile;
  private File outputFile;
  private File expectedOutputFile;
  private File logFile;
  private File beforeExecuteLogFile;
  private File afterExecuteLogFile;
  private static RegexFilterSet filterSet = getFilterSet();
  private boolean rewriteSourceTables;

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
        logFile, beforeExecuteLogFile, afterExecuteLogFile,
        "./itests/qtest/target/tmp/log/hive.log");
  }

  /**
   * Filters the sql commands if necessary.
   * @param commands The array of the sql commands before filtering
   * @return The filtered array of the sql command strings
   * @throws IOException File read error
   */
  public String[] filterCommands(String[] commands) throws IOException {
    if (rewriteSourceTables) {
      for (int i=0; i<commands.length; i++) {
        if (USE_PATTERN.matcher(commands[i]).matches()) {
          System.err.println(String.format(USE_COMMAND_WARNING, inputFile, commands[i]));
        }
        commands[i] = replaceTableNames(commands[i]);
      }
    }
    return commands;
  }

  /**
   * Replace the default src database TABLE_NAMEs in the queries with default.TABLE_NAME, like
   * src->default.src, srcpart->default.srcpart, so the queries could be run even if the used
   * database is query specific. This change is only a best effort, since we do not want to parse
   * the queries, we could not be sure that we do not replace other strings which are not
   * tablenames. Like 'select src from othertable;'. The q files containing these commands should
   * be excluded. Only replace the tablenames, if rewriteSourceTables is set.
   * @param source The original query string
   * @return The query string where the tablenames are replaced
   */
  private String replaceTableNames(String source) {
    for (String table : srcTables) {
      source = source.replaceAll("(?is)(\\s+)" + table + "([\\s;\\n\\)])", "$1default." + table
          + "$2");
    }
    return source;
  }

  public void filterOutput() throws IOException {
    String rawOutput = FileUtils.readFileToString(rawOutputFile, "UTF-8");
    String filteredOutput = filterSet.filter(rawOutput);
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

  private static class Filter {
    private final Pattern pattern;
    private final String replacement;

    public Filter(Pattern pattern, String replacement) {
      this.pattern = pattern;
      this.replacement = replacement;
    }
  }

  private static class RegexFilterSet {
    private final List<Filter> regexFilters = new ArrayList<Filter>();

    public RegexFilterSet addFilter(String regex, String replacement) {
      regexFilters.add(new Filter(Pattern.compile(regex), replacement));
      return this;
    }

    public String filter(String input) {
      for (Filter filter : regexFilters) {
        input = filter.pattern.matcher(input).replaceAll(filter.replacement);
      }
      return input;
    }
  }

  // These are the filters which are common for every QTest.
  // Check specificFilterSet for QTest specific ones.
  private static RegexFilterSet getFilterSet() {
    // Extract the leading four digits from the unix time value.
    // Use this as a prefix in order to increase the selectivity
    // of the unix time stamp replacement regex.
    String currentTimePrefix = Long.toString(System.currentTimeMillis()).substring(0, 4);

    String userName = System.getProperty("user.name");

    String timePattern = "(Mon|Tue|Wed|Thu|Fri|Sat|Sun) "
        + "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) "
        + "\\d{2} \\d{2}:\\d{2}:\\d{2} \\w+ 20\\d{2}";
    String operatorPattern = "\"(CONDITION|COPY|DEPENDENCY_COLLECTION|DDL"
        + "|EXPLAIN|FETCH|FIL|FS|FUNCTION|GBY|HASHTABLEDUMMY|HASTTABLESINK|JOIN"
        + "|LATERALVIEWFORWARD|LIM|LVJ|MAP|MAPJOIN|MAPRED|MAPREDLOCAL|MOVE|OP|RS"
        + "|SCR|SEL|STATS|TS|UDTF|UNION)_\\d+\"";

    return new RegexFilterSet()
        .addFilter("(?s)\n[^\n]*Waiting to acquire compile lock.*?Acquired the compile lock.\n",
            "\n")
        .addFilter(".*Acquired the compile lock.\n", "")
        .addFilter("Getting log thread is interrupted, since query is done!\n", "")
        .addFilter("going to print operations logs\n", "")
        .addFilter("printed operations logs\n", "")
        .addFilter("\\(queryId=[^\\)]*\\)", "queryId=(!!{queryId}!!)")
        .addFilter("Query ID = [\\w-]+", "Query ID = !!{queryId}!!")
        .addFilter("file:/\\w\\S+", "file:/!!ELIDED!!")
        .addFilter("pfile:/\\w\\S+", "pfile:/!!ELIDED!!")
        .addFilter("hdfs:/\\w\\S+", "hdfs:/!!ELIDED!!")
        .addFilter("last_modified_by=\\w+", "last_modified_by=!!ELIDED!!")
        .addFilter(timePattern, "!!TIMESTAMP!!")
        .addFilter("(\\D)" + currentTimePrefix + "\\d{6}(\\D)", "$1!!UNIXTIME!!$2")
        .addFilter("(\\D)" + currentTimePrefix + "\\d{9}(\\D)", "$1!!UNIXTIMEMILLIS!!$2")
        .addFilter(userName, "!!{user.name}!!")
        .addFilter(operatorPattern, "\"$1_!!ELIDED!!\"")
        .addFilter("(?i)Time taken: [0-9\\.]* sec", "Time taken: !!ELIDED!! sec")
        .addFilter(" job(:?) job_\\w+([\\s\n])", " job$1 !!{jobId}}!!$2")
        .addFilter("Ended Job = job_\\w+([\\s\n])", "Ended Job = !!{jobId}!!$1")
        .addFilter(".*\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}.* map = .*\n", "")
        .addFilter("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\s+", "")
        .addFilter("maximum memory = \\d*", "maximum memory = !!ELIDED!!");
  }

  /**
   * Builder to generate QFile objects. After initializing the builder it is possible the
   * generate the next QFile object using it's name only.
   */
  public static class QFileBuilder {
    private File queryDirectory;
    private File logDirectory;
    private File resultsDirectory;
    private boolean rewriteSourceTables;

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

    public QFileBuilder setRewriteSourceTables(boolean rewriteSourceTables) {
      this.rewriteSourceTables = rewriteSourceTables;
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
      result.rewriteSourceTables = rewriteSourceTables;
      return result;
    }
  }
}
