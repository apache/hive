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

package org.apache.hadoop.hive.ql.exec;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkFiles;

/**
 * ScriptOperator.
 *
 */
public class ScriptOperator extends Operator<ScriptDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Counter.
   *
   */
  public static enum Counter {
    DESERIALIZE_ERRORS, SERIALIZE_ERRORS
  }

  private final transient LongWritable deserialize_error_count = new LongWritable();
  private final transient LongWritable serialize_error_count = new LongWritable();

  transient Thread outThread = null;
  transient Thread errThread = null;
  transient Process scriptPid = null;
  transient Configuration hconf;
  // Input to the script
  transient Serializer scriptInputSerializer;
  // Output from the script
  transient Deserializer scriptOutputDeserializer;
  transient volatile Throwable scriptError = null;
  transient RecordWriter scriptOutWriter = null;
  // List of conf entries not to turn into env vars
  transient Set<String> blackListedConfEntries = null;

  static final String IO_EXCEPTION_BROKEN_PIPE_STRING = "Broken pipe";
  static final String IO_EXCEPTION_STREAM_CLOSED = "Stream closed";

  /**
   * sends periodic reports back to the tracker.
   */
  transient AutoProgressor autoProgressor;

  // first row - the process should only be started if necessary, as it may
  // conflict with some
  // of the user assumptions.
  transient boolean firstRow;


  String safeEnvVarName(String name) {
    StringBuilder safe = new StringBuilder();
    int len = name.length();

    for (int i = 0; i < len; i++) {
      char c = name.charAt(i);
      char s;
      if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z')
          || (c >= 'a' && c <= 'z')) {
        s = c;
      } else {
        s = '_';
      }
      safe.append(s);
    }
    return safe.toString();
  }

  /**
   * Most UNIX implementations impose some limit on the total size of environment variables and
   * size of strings. To fit in this limit we need sometimes to truncate strings.  Also,
   * some values tend be long and are meaningless to scripts, so strain them out.
   * @param value environment variable value to check
   * @param name name of variable (used only for logging purposes)
   * @param truncate truncate value or not
   * @return original value, or truncated one if it's length is more then 20KB and
   * truncate flag is set
   * @see <a href="http://www.kernel.org/doc/man-pages/online/pages/man2/execve.2.html">Linux
   * Man page</a> for more details
   */
  String safeEnvVarValue(String value, String name, boolean truncate) {
    final int lenLimit = 20*1024;
    if (truncate && value.length() > lenLimit) {
      value = value.substring(0, lenLimit);
      LOG.warn("Length of environment variable " + name + " was truncated to " + lenLimit
          + " bytes to fit system limits.");
    }
    return value;
  }

  /**
   * Checks whether a given configuration name is blacklisted and should not be converted
   * to an environment variable.
   */
  boolean blackListed(Configuration conf, String name) {
    if (blackListedConfEntries == null) {
      blackListedConfEntries = new HashSet<String>();
      if (conf != null) {
        String bl = conf.get(HiveConf.ConfVars.HIVESCRIPT_ENV_BLACKLIST.toString(),
          HiveConf.ConfVars.HIVESCRIPT_ENV_BLACKLIST.getDefaultValue());
        if (bl != null && !bl.isEmpty()) {
          String[] bls = bl.split(",");
          Collections.addAll(blackListedConfEntries, bls);
        }
      }
    }
    return blackListedConfEntries.contains(name);
  }

  /**
   * addJobConfToEnvironment is mostly shamelessly copied from hadoop streaming. Added additional
   * check on environment variable length
   */
  void addJobConfToEnvironment(Configuration conf, Map<String, String> env) {
    Iterator<Map.Entry<String, String>> it = conf.iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> en = it.next();
      String name = en.getKey();
      if (!blackListed(conf, name)) {
        // String value = (String)en.getValue(); // does not apply variable
        // expansion
        String value = conf.get(name); // does variable expansion
        name = safeEnvVarName(name);
        boolean truncate = conf
            .getBoolean(HiveConf.ConfVars.HIVESCRIPTTRUNCATEENV.toString(), false);
        value = safeEnvVarValue(value, name, truncate);
        env.put(name, value);
      }
    }
  }

  /**
   * Maps a relative pathname to an absolute pathname using the PATH environment.
   */
  public class PathFinder {
    String pathenv; // a string of pathnames
    String pathSep; // the path separator
    String fileSep; // the file separator in a directory

    /**
     * Construct a PathFinder object using the path from the specified system
     * environment variable.
     */
    public PathFinder(String envpath) {
      pathenv = System.getenv(envpath);
      pathSep = System.getProperty("path.separator");
      fileSep = System.getProperty("file.separator");
    }

    /**
     * Appends the specified component to the path list.
     */
    public void prependPathComponent(String str) {
      pathenv = str + pathSep + pathenv;
    }

    /**
     * Returns the full path name of this file if it is listed in the path.
     */
    public File getAbsolutePath(String filename) {
      if (pathenv == null || pathSep == null || fileSep == null) {
        return null;
      }

      int val = -1;
      String classvalue = pathenv + pathSep;

      while (((val = classvalue.indexOf(pathSep)) >= 0)
          && classvalue.length() > 0) {
        //
        // Extract each entry from the pathenv
        //
        String entry = classvalue.substring(0, val).trim();
        File f = new File(entry);

        try {
          if (f.isDirectory()) {
            //
            // this entry in the pathenv is a directory.
            // see if the required file is in this directory
            //
            f = new File(entry + fileSep + filename);
          }
          //
          // see if the filename matches and we can read it
          //
          if (f.isFile() && f.canRead()) {
            return f;
          }
        } catch (Exception exp) {
        }
        classvalue = classvalue.substring(val + 1).trim();
      }
      return null;
    }
  }

  /** Kryo ctor. */
  protected ScriptOperator() {
    super();
  }

  public ScriptOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    firstRow = true;

    statsMap.put(Counter.DESERIALIZE_ERRORS.toString(), deserialize_error_count);
    statsMap.put(Counter.SERIALIZE_ERRORS.toString(), serialize_error_count);

    try {
      this.hconf = hconf;

      scriptOutputDeserializer = conf.getScriptOutputInfo()
          .getDeserializerClass().newInstance();
      SerDeUtils.initializeSerDe(scriptOutputDeserializer, hconf,
                                 conf.getScriptOutputInfo().getProperties(), null);

      scriptInputSerializer = (Serializer) conf.getScriptInputInfo()
          .getDeserializerClass().newInstance();
      scriptInputSerializer.initialize(hconf, conf.getScriptInputInfo()
          .getProperties());

      outputObjInspector = scriptOutputDeserializer.getObjectInspector();

    } catch (Exception e) {
      throw new HiveException(ErrorMsg.SCRIPT_INIT_ERROR.getErrorCodedMsg(), e);
    }
  }

  boolean isBrokenPipeException(IOException e) {
    return (e.getMessage().equalsIgnoreCase(IO_EXCEPTION_BROKEN_PIPE_STRING) ||
            e.getMessage().equalsIgnoreCase(IO_EXCEPTION_STREAM_CLOSED));
  }

  boolean allowPartialConsumption() {
    return HiveConf.getBoolVar(hconf, HiveConf.ConfVars.ALLOWPARTIALCONSUMP);
  }

  void displayBrokenPipeInfo() {
    if (LOG.isInfoEnabled()) {
      LOG.info("The script did not consume all input data. This is considered as an error.");
      LOG.info("set " + HiveConf.ConfVars.ALLOWPARTIALCONSUMP.toString() + "=true; to ignore it.");
    }
    return;
  }

  private transient String tableName;
  private transient String partitionName ;

  @Override
  public void setInputContext(String tableName, String partitionName) {
    this.tableName = tableName;
    this.partitionName = partitionName;
    super.setInputContext(tableName, partitionName);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    // initialize the user's process only when you receive the first row
    if (firstRow) {
      firstRow = false;
      SparkConf sparkConf = null;
      try {
        String[] cmdArgs = splitArgs(conf.getScriptCmd());

        String prog = cmdArgs[0];
        File currentDir = new File(".").getAbsoluteFile();

        if (!new File(prog).isAbsolute()) {
          PathFinder finder = new PathFinder("PATH");
          finder.prependPathComponent(currentDir.toString());

          // In spark local mode, we need to search added files in root directory.
          if (HiveConf.getVar(hconf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
            sparkConf = SparkEnv.get().conf();
            finder.prependPathComponent(SparkFiles.getRootDirectory());
          }
          File f = finder.getAbsolutePath(prog);
          if (f != null) {
            cmdArgs[0] = f.getAbsolutePath();
          }
          f = null;
        }

        String[] wrappedCmdArgs = addWrapper(cmdArgs);
        if (LOG.isInfoEnabled()) {
          LOG.info("Executing " + Arrays.asList(wrappedCmdArgs));
          LOG.info("tablename=" + tableName);
          LOG.info("partname=" + partitionName);
          LOG.info("alias=" + alias);
        }

        ProcessBuilder pb = new ProcessBuilder(wrappedCmdArgs);
        Map<String, String> env = pb.environment();
        addJobConfToEnvironment(hconf, env);
        env.put(safeEnvVarName(HiveConf.ConfVars.HIVEALIAS.varname), String
            .valueOf(alias));

        // Create an environment variable that uniquely identifies this script
        // operator
        String idEnvVarName = HiveConf.getVar(hconf,
            HiveConf.ConfVars.HIVESCRIPTIDENVVAR);
        String idEnvVarVal = getOperatorId();
        env.put(safeEnvVarName(idEnvVarName), idEnvVarVal);

        // For spark, in non-local mode, any added dependencies are stored at
        // SparkFiles::getRootDirectory, which is the executor's working directory.
        // In local mode, we need to manually point the process's working directory to it,
        // in order to make the dependencies accessible.
        if (sparkConf != null) {
          String master = sparkConf.get("spark.master");
          if (master.equals("local") || master.startsWith("local[")) {
            pb.directory(new File(SparkFiles.getRootDirectory()));
          }
        }

        scriptPid = pb.start(); // Runtime.getRuntime().exec(wrappedCmdArgs);

        DataOutputStream scriptOut = new DataOutputStream(
            new BufferedOutputStream(scriptPid.getOutputStream()));
        DataInputStream scriptIn = new DataInputStream(new BufferedInputStream(
            scriptPid.getInputStream()));
        DataInputStream scriptErr = new DataInputStream(
            new BufferedInputStream(scriptPid.getErrorStream()));

        scriptOutWriter = conf.getInRecordWriterClass().newInstance();
        scriptOutWriter.initialize(scriptOut, hconf);

        RecordReader scriptOutputReader = conf.getOutRecordReaderClass()
            .newInstance();
        scriptOutputReader.initialize(scriptIn, hconf, conf
            .getScriptOutputInfo().getProperties());

        outThread = new StreamThread(scriptOutputReader,
            new OutputStreamProcessor(scriptOutputDeserializer
            .getObjectInspector()), "OutputProcessor");

        RecordReader scriptErrReader = conf.getErrRecordReaderClass()
            .newInstance();
        scriptErrReader.initialize(scriptErr, hconf, conf.getScriptErrInfo()
            .getProperties());

        errThread = new StreamThread(scriptErrReader, new ErrorStreamProcessor(
            HiveConf.getIntVar(hconf, HiveConf.ConfVars.SCRIPTERRORLIMIT)),
            "ErrorProcessor");

        if (HiveConf
            .getBoolVar(hconf, HiveConf.ConfVars.HIVESCRIPTAUTOPROGRESS)) {
          autoProgressor = new AutoProgressor(this.getClass().getName(),
              reporter, Utilities.getDefaultNotificationInterval(hconf),
              HiveConf.getTimeVar(
                  hconf, HiveConf.ConfVars.HIVES_AUTO_PROGRESS_TIMEOUT, TimeUnit.MILLISECONDS));
          autoProgressor.go();
        }

        outThread.start();
        errThread.start();
      } catch (Exception e) {
        throw new HiveException(ErrorMsg.SCRIPT_INIT_ERROR.getErrorCodedMsg(), e);
      }
    }

    if (scriptError != null) {
      throw new HiveException(ErrorMsg.SCRIPT_GENERIC_ERROR.getErrorCodedMsg(), scriptError);
    }

    try {
      Writable res = scriptInputSerializer.serialize(row,
          inputObjInspectors[tag]);
      scriptOutWriter.write(res);
    } catch (SerDeException e) {
      LOG.error("Error in serializing the row: " + e.getMessage());
      scriptError = e;
      serialize_error_count.set(serialize_error_count.get() + 1);
      throw new HiveException(e);
    } catch (IOException e) {
      if (isBrokenPipeException(e) && allowPartialConsumption()) {
        // Give the outThread a chance to finish before marking the operator as done
        try {
          scriptPid.waitFor();
        } catch (InterruptedException interruptedException) {
        }
        // best effort attempt to write all output from the script before marking the operator
        // as done
        try {
          if (outThread != null) {
            outThread.join(0);
          }
        } catch (Exception e2) {
          LOG.warn("Exception in closing outThread: "
              + StringUtils.stringifyException(e2));
        }
        setDone(true);
        LOG.warn("Got broken pipe during write: ignoring exception and setting operator to done");
      } else {
        LOG.error("Error in writing to script: " + e.getMessage());
        if (isBrokenPipeException(e)) {
          displayBrokenPipeInfo();
        }
        scriptError = e;
        throw new HiveException(ErrorMsg.SCRIPT_IO_ERROR.getErrorCodedMsg(), e);
      }
    }
  }

  @Override
  public void close(boolean abort) throws HiveException {

    boolean new_abort = abort;
    if (!abort) {
      if (scriptError != null) {
        throw new HiveException(ErrorMsg.SCRIPT_GENERIC_ERROR.getErrorCodedMsg(), scriptError);
      }
      // everything ok. try normal shutdown
      try {
        try {
          if (scriptOutWriter != null) {
            scriptOutWriter.close();
          }
        } catch (IOException e) {
          if (isBrokenPipeException(e) && allowPartialConsumption()) {
            LOG.warn("Got broken pipe: ignoring exception");
          } else {
            if (isBrokenPipeException(e)) {
              displayBrokenPipeInfo();
            }
            throw e;
          }
        }
        int exitVal = 0;
        if (scriptPid != null) {
          exitVal = scriptPid.waitFor();
        }
        if (exitVal != 0) {
          LOG.error("Script failed with code " + exitVal);
          new_abort = true;
        }
      } catch (IOException e) {
        LOG.error("Got ioexception: " + e.getMessage());
        e.printStackTrace();
        new_abort = true;
      } catch (InterruptedException e) {
      }

    } else {

      // Error already occurred, but we still want to get the
      // error code of the child process if possible.
      try {
        // Interrupt the current thread after 1 second
        final Thread mythread = Thread.currentThread();
        Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            mythread.interrupt();
          }
        }, 1000);
        // Wait for the child process to finish
        int exitVal = 0;
        if (scriptPid != null) {
          scriptPid.waitFor();
        }
        // Cancel the timer
        timer.cancel();
        // Output the exit code
        LOG.error("Script exited with code " + exitVal);
      } catch (InterruptedException e) {
        // Ignore
        LOG.error("Script has not exited yet. It will be killed.");
      }
    }

    // try these best effort
    try {
      if (outThread != null) {
        outThread.join(0);
      }
    } catch (Exception e) {
      LOG.warn("Exception in closing outThread: "
          + StringUtils.stringifyException(e));
    }

    try {
      if (errThread != null) {
        errThread.join(0);
      }
    } catch (Exception e) {
      LOG.warn("Exception in closing errThread: "
          + StringUtils.stringifyException(e));
    }

    try {
      if (scriptPid != null) {
        scriptPid.destroy();
      }
    } catch (Exception e) {
      LOG.warn("Exception in destroying scriptPid: "
          + StringUtils.stringifyException(e));
    }

    super.close(new_abort);

    if (new_abort && !abort) {
      throw new HiveException(ErrorMsg.SCRIPT_CLOSING_ERROR.getErrorCodedMsg());
    }
  }

  interface StreamProcessor {
    void processLine(Writable line) throws HiveException;

    void close() throws HiveException;
  }

  class OutputStreamProcessor implements StreamProcessor {
    Object row;
    ObjectInspector rowInspector;

    public OutputStreamProcessor(ObjectInspector rowInspector) {
      this.rowInspector = rowInspector;
    }

    @Override
    public void processLine(Writable line) throws HiveException {
      try {
        row = scriptOutputDeserializer.deserialize(line);
      } catch (SerDeException e) {
        deserialize_error_count.set(deserialize_error_count.get() + 1);
        return;
      }
      forward(row, rowInspector);
    }

    @Override
    public void close() {
    }
  }

  class CounterStatusProcessor {

    private final String reporterPrefix;
    private final String counterPrefix;
    private final String statusPrefix;
    private final Reporter reporter;

    CounterStatusProcessor(Configuration hconf, Reporter reporter){
      this.reporterPrefix = HiveConf.getVar(hconf, HiveConf.ConfVars.STREAMREPORTERPERFIX);
      this.counterPrefix = reporterPrefix + "counter:";
      this.statusPrefix = reporterPrefix + "status:";
      this.reporter = reporter;
    }

    private boolean process(String line) {
      if (line.startsWith(reporterPrefix)){
        if (line.startsWith(counterPrefix)){
          incrCounter(line);
        }
        if (line.startsWith(statusPrefix)){
          setStatus(line);
        }
        return true;
      } else {
        return false;
      }
    }

    private void incrCounter(String line) {
      String  trimmedLine = line.substring(counterPrefix.length()).trim();
      String[] columns = trimmedLine.split(",");
      if (columns.length == 3) {
        try {
          reporter.incrCounter(columns[0], columns[1], Long.parseLong(columns[2]));
        } catch (NumberFormatException e) {
            LOG.warn("Cannot parse counter increment '" + columns[2] +
                "' from line " + line);
        }
      } else {
        LOG.warn("Cannot parse counter line: " + line);
      }
    }

    private void setStatus(String line) {
      reporter.setStatus(line.substring(statusPrefix.length()).trim());
    }
  }
  /**
   * The processor for stderr stream.
   */
  class ErrorStreamProcessor implements StreamProcessor {
    private long bytesCopied = 0;
    private final long maxBytes;
    private long lastReportTime;
    private CounterStatusProcessor counterStatus;

    public ErrorStreamProcessor(int maxBytes) {
      this.maxBytes = maxBytes;
      lastReportTime = 0;
      if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.STREAMREPORTERENABLED)){
        counterStatus = new CounterStatusProcessor(hconf, reporter);
      }
    }

    @Override
    public void processLine(Writable line) throws HiveException {

      String stringLine = line.toString();
      int len = 0;

      if (line instanceof Text) {
        len = ((Text) line).getLength();
      } else if (line instanceof BytesWritable) {
        len = ((BytesWritable) line).getSize();
      }

      // Report progress for each stderr line, but no more frequently than once
      // per minute.
      long now = System.currentTimeMillis();
      // reporter is a member variable of the Operator class.
      if (now - lastReportTime > 60 * 1000 && reporter != null) {
        if (LOG.isInfoEnabled()) {
          LOG.info("ErrorStreamProcessor calling reporter.progress()");
        }
        lastReportTime = now;
        reporter.progress();
      }

      if (reporter != null) {
        if (counterStatus != null) {
          if (counterStatus.process(stringLine)) {
            return;
          }
        }
      }

      if ((maxBytes < 0) || (bytesCopied < maxBytes)) {
        System.err.println(stringLine);
      }
      if (bytesCopied < maxBytes && bytesCopied + len >= maxBytes) {
        System.err.println("Operator " + id + " " + getName()
            + ": exceeding stderr limit of " + maxBytes
            + " bytes, will truncate stderr messages.");
      }
      bytesCopied += len;
    }

    @Override
    public void close() {
    }

  }

  class StreamThread extends Thread {

    RecordReader in;
    StreamProcessor proc;
    String name;

    StreamThread(RecordReader in, StreamProcessor proc, String name) {
      this.in = in;
      this.proc = proc;
      this.name = name;
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        Writable row = in.createRow();

        while (true) {
          long bytes = in.next(row);

          if (bytes <= 0) {
            break;
          }
          proc.processLine(row);
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("StreamThread " + name + " done");
        }

      } catch (Throwable th) {
        scriptError = th;
        LOG.warn("Exception in StreamThread.run(): " + th.getMessage() +
            "\nCause: " + th.getCause());
        LOG.warn(StringUtils.stringifyException(th));
      } finally {
        try {
          if (in != null) {
            in.close();
          }
        } catch (Exception e) {
          LOG.warn(name + ": error in closing ..");
          LOG.warn(StringUtils.stringifyException(e));
        }
        try
        {
          if (null != proc) {
            proc.close();
          }
        }catch (Exception e) {
          LOG.warn(": error in closing .."+StringUtils.stringifyException(e));
        }
      }
    }
  }

  /**
   * Wrap the script in a wrapper that allows admins to control.
   */
  protected String[] addWrapper(String[] inArgs) {
    String wrapper = HiveConf.getVar(hconf, HiveConf.ConfVars.SCRIPTWRAPPER);
    if (wrapper == null) {
      return inArgs;
    }

    String[] wrapComponents = splitArgs(wrapper);
    int totallength = wrapComponents.length + inArgs.length;
    String[] finalArgv = new String[totallength];
    for (int i = 0; i < wrapComponents.length; i++) {
      finalArgv[i] = wrapComponents[i];
    }
    for (int i = 0; i < inArgs.length; i++) {
      finalArgv[wrapComponents.length + i] = inArgs[i];
    }
    return finalArgv;
  }

  // Code below shameless borrowed from Hadoop Streaming

  public static String[] splitArgs(String args) {
    final int OUTSIDE = 1;
    final int SINGLEQ = 2;
    final int DOUBLEQ = 3;

    ArrayList argList = new ArrayList();
    char[] ch = args.toCharArray();
    int clen = ch.length;
    int state = OUTSIDE;
    int argstart = 0;
    for (int c = 0; c <= clen; c++) {
      boolean last = (c == clen);
      int lastState = state;
      boolean endToken = false;
      if (!last) {
        if (ch[c] == '\'') {
          if (state == OUTSIDE) {
            state = SINGLEQ;
          } else if (state == SINGLEQ) {
            state = OUTSIDE;
          }
          endToken = (state != lastState);
        } else if (ch[c] == '"') {
          if (state == OUTSIDE) {
            state = DOUBLEQ;
          } else if (state == DOUBLEQ) {
            state = OUTSIDE;
          }
          endToken = (state != lastState);
        } else if (ch[c] == ' ') {
          if (state == OUTSIDE) {
            endToken = true;
          }
        }
      }
      if (last || endToken) {
        if (c == argstart) {
          // unquoted space
        } else {
          String a;
          a = args.substring(argstart, c);
          argList.add(a);
        }
        argstart = c + 1;
        lastState = state;
      }
    }
    return (String[]) argList.toArray(new String[0]);
  }

  @Override
  public String getName() {
    return ScriptOperator.getOperatorName();
  }

  static public String getOperatorName() {
    return "SCR";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.SCRIPT;
  }
}
