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

package org.apache.hadoop.hive.ql;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.io.DigestPrintStream;
import org.apache.hadoop.hive.common.io.SessionStream;
import org.apache.hadoop.hive.common.io.SortAndDigestPrintStream;
import org.apache.hadoop.hive.common.io.SortPrintStream;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.StreamPrinter;

/**
 * QTestResultProcessor: handles file-level q test result post-processing: sort, diff (similar to
 * QOutProcessor, but this works with files, and QOutProcessor is for text-processing within a qtest
 * result file)
 *
 */
public class QTestResultProcessor {
  private static final String SORT_SUFFIX = ".sorted";

  private enum Operation {
    /***/
    PRESORT("-- SORT_BEFORE_DIFF"),
    /***/
    SORT("-- SORT_QUERY_RESULTS"),
    /***/
    HASH("-- HASH_QUERY_RESULTS"),
    /***/
    SORT_N_HASH("-- SORT_AND_HASH_QUERY_RESULTS"),
    /***/
    NEW_SESSION("-- NO_SESSION_REUSE");
    private final Pattern pattern;

    Operation(String pattern) {
      this.pattern = Pattern.compile(pattern);
    }

    boolean existsIn(String query) {
      return pattern.matcher(query).find();
    }
  }

  /**
   * Operations present in a given file/test.
   */
  private final Set<Operation> operations = new HashSet<>();

  public void init(String query) {
    operations.clear();
    for (Operation op : Operation.values()) {
      if (op.existsIn(query)) {
        operations.add(op);
      }
    }
  }

  private boolean shouldSort() {
    return operations.contains(Operation.PRESORT);
  }

  public void setOutputs(CliSessionState ss, OutputStream fo) throws Exception {
    // Normally, only one of PRESORT, SORT, HASH, SORT_N_HASH, should be present
    // in a file. If there are multiple then the code will pick one in the order
    // specified below. This ensures the behavior remains the same as before this
    // refactoring.
    // It would be better to throw an error than silently pick one and ignore the
    // rest but it is out of the scope of the current change. 
    if (operations.contains(Operation.SORT)) {
      ss.out = new SortPrintStream(fo, "UTF-8");
    } else if (operations.contains(Operation.HASH)) {
      ss.out = new DigestPrintStream(fo, "UTF-8");
    } else if (operations.contains(Operation.SORT_N_HASH)) {
      ss.out = new SortAndDigestPrintStream(fo, "UTF-8");
    } else {
      ss.out = new SessionStream(fo, true, "UTF-8");
    }
  }

  public boolean canReuseSession() {
    return !operations.contains(Operation.NEW_SESSION);
  }

  public QTestProcessExecResult executeDiffCommand(String inFileName, String outFileName, boolean ignoreWhiteSpace) throws Exception {

    QTestProcessExecResult result;

    if (shouldSort()) {
      String inSorted = inFileName + SORT_SUFFIX;
      String outSorted = outFileName + SORT_SUFFIX;

      sortResult(inFileName, outFileName, inSorted, outSorted);

      inFileName = inSorted;
      outFileName = outSorted;
    }

    ArrayList<String> diffCommandArgs = new ArrayList<String>();
    diffCommandArgs.add("diff");

    // Text file comparison
    diffCommandArgs.add("-a");

    // Ignore changes in the amount of white space
    if (ignoreWhiteSpace) {
      diffCommandArgs.add("-b");
    }

    // Add files to compare to the arguments list
    diffCommandArgs.add(getQuotedString(inFileName));
    diffCommandArgs.add(getQuotedString(outFileName));

    result = executeCmd(diffCommandArgs);

    if (shouldSort()) {
      new File(inFileName).delete();
      new File(outFileName).delete();
    }

    return result;
  }

  public void overwriteResults(String inFileName, String outFileName) throws Exception {
    // This method can be replaced with Files.copy(source, target, REPLACE_EXISTING)
    // once Hive uses JAVA 7.
    System.out.println("Overwriting results " + inFileName + " to " + outFileName);
    int result = executeCmd(new String[]{
        "cp", getQuotedString(inFileName), getQuotedString(outFileName) }).getReturnCode();
    if (result != 0) {
      throw new IllegalStateException("Unexpected error while overwriting " + inFileName + " with " + outFileName);
    }
  }

  private void sortResult(String inFileName, String outFileName, String inSorted, String outSorted)
      throws Exception {
    // sort will try to open the output file in write mode on windows. We need to
    // close it first.
    SessionState ss = SessionState.get();
    if (ss != null && ss.out != null && ss.out != System.out) {
      ss.out.close();
    }

    sortFiles(inFileName, inSorted);
    sortFiles(outFileName, outSorted);
  }

  private void sortFiles(String in, String out) throws Exception {
    int result =
        executeCmd(new String[] { "sort", getQuotedString(in), }, out, null).getReturnCode();
    if (result != 0) {
      throw new IllegalStateException("Unexpected error while sorting " + in);
    }
  }

  private static QTestProcessExecResult executeCmd(Collection<String> args) throws Exception {
    return executeCmd(args, null, null);
  }

  private static QTestProcessExecResult executeCmd(String[] args) throws Exception {
    return executeCmd(args, null, null);
  }

  private static QTestProcessExecResult executeCmd(Collection<String> args, String outFile,
      String errFile) throws Exception {
    String[] cmdArray = args.toArray(new String[args.size()]);
    return executeCmd(cmdArray, outFile, errFile);
  }

  public static QTestProcessExecResult executeCmd(String[] args, String outFile, String errFile)
      throws Exception {
    System.out.println("Running: " + org.apache.commons.lang3.StringUtils.join(args, ' '));

    PrintStream out = outFile == null ? SessionState.getConsole().getChildOutStream()
      : new PrintStream(new FileOutputStream(outFile), true, "UTF-8");
    PrintStream err = errFile == null ? SessionState.getConsole().getChildErrStream()
      : new PrintStream(new FileOutputStream(errFile), true, "UTF-8");

    Process executor = Runtime.getRuntime().exec(args);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream str = new PrintStream(bos, true, "UTF-8");

    StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, err);
    StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, out, str);

    outPrinter.start();
    errPrinter.start();

    int result = executor.waitFor();

    outPrinter.join();
    errPrinter.join();

    if (outFile != null) {
      out.close();
    }

    if (errFile != null) {
      err.close();
    }

    return QTestProcessExecResult.create(result,
        new String(bos.toByteArray(), StandardCharsets.UTF_8));
  }

  private static String getQuotedString(String str) {
    return str;
  }
}
