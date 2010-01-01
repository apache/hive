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

package org.apache.hadoop.hive.ql.exec;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.scriptDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;


public class ScriptOperator extends Operator<scriptDesc> implements Serializable {

  private static final long serialVersionUID = 1L;


  public static enum Counter {DESERIALIZE_ERRORS, SERIALIZE_ERRORS}
  transient private LongWritable deserialize_error_count = new LongWritable ();
  transient private LongWritable serialize_error_count = new LongWritable ();

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

  static final String IO_EXCEPTION_BROKEN_PIPE_STRING= "Broken pipe";

  /**
   * sends periodic reports back to the tracker.
   */
  transient AutoProgressor autoProgressor;

  // first row - the process should only be started if necessary, as it may conflict with some
  // of the user assumptions.
  transient boolean firstRow;

  /**
   * addJobConfToEnvironment is shamelessly copied from hadoop streaming.
   */
  static String safeEnvVarName(String var) {
    StringBuffer safe = new StringBuffer();
    int len = var.length();
    for (int i = 0; i < len; i++) {
      char c = var.charAt(i);
      char s;
      if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
        s = c;
      } else {
        s = '_';
      }
      safe.append(s);
    }
    return safe.toString();
  }

  static void addJobConfToEnvironment(Configuration conf, Map<String, String> env) {
    Iterator<Map.Entry<String, String>> it = conf.iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> en = (Map.Entry<String, String>) it.next();
      String name = (String) en.getKey();
      //String value = (String)en.getValue(); // does not apply variable expansion
      String value = conf.get(name); // does variable expansion
      name = safeEnvVarName(name);
      env.put(name, value);
    }
  }


  /**
   * Maps a relative pathname to an absolute pathname using the
   * PATH enviroment.
   */
  public class PathFinder
  {
    String pathenv;        // a string of pathnames
    String pathSep;        // the path seperator
    String fileSep;        // the file seperator in a directory

    /**
     * Construct a PathFinder object using the path from
     * the specified system environment variable.
     */
    public PathFinder(String envpath)
    {
      pathenv = System.getenv(envpath);
      pathSep = System.getProperty("path.separator");
      fileSep = System.getProperty("file.separator");
    }

    /**
     * Appends the specified component to the path list
     */
    public void prependPathComponent(String str)
    {
      pathenv = str + pathSep + pathenv;
    }

    /**
     * Returns the full path name of this file if it is listed in the
     * path
     */
    public File getAbsolutePath(String filename)
    {
      if (pathenv == null || pathSep == null  || fileSep == null) {
        return null;
      }
      int     val = -1;
      String    classvalue = pathenv + pathSep;

      while (((val = classvalue.indexOf(pathSep)) >= 0) &&
             classvalue.length() > 0) {
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
          // see if the filename matches and  we can read it
          //
          if (f.isFile() && f.canRead()) {
            return f;
          }
        } catch (Exception exp){ }
        classvalue = classvalue.substring(val+1).trim();
      }
      return null;
    }
  }

  protected void initializeOp(Configuration hconf) throws HiveException {
    firstRow = true;

    statsMap.put(Counter.DESERIALIZE_ERRORS, deserialize_error_count);
    statsMap.put(Counter.SERIALIZE_ERRORS, serialize_error_count);

    try {
      this.hconf = hconf;

      scriptOutputDeserializer = conf.getScriptOutputInfo().getDeserializerClass().newInstance();
      scriptOutputDeserializer.initialize(hconf, conf.getScriptOutputInfo().getProperties());

      scriptInputSerializer = (Serializer)conf.getScriptInputInfo().getDeserializerClass().newInstance();
      scriptInputSerializer.initialize(hconf, conf.getScriptInputInfo().getProperties());

      outputObjInspector = scriptOutputDeserializer.getObjectInspector();

      // initialize all children before starting the script
      initializeChildren(hconf);
    } catch (Exception e) {
      throw new HiveException ("Cannot initialize ScriptOperator", e);
    }
  }

  boolean isBrokenPipeException(IOException e) {
    return (e.getMessage().compareToIgnoreCase(IO_EXCEPTION_BROKEN_PIPE_STRING) == 0);
  }

  boolean allowPartialConsumption() {
    return HiveConf.getBoolVar(hconf, HiveConf.ConfVars.ALLOWPARTIALCONSUMP);
  }

  void displayBrokenPipeInfo() {
    LOG.info("The script did not consume all input data. This is considered as an error.");
    LOG.info("set " + HiveConf.ConfVars.ALLOWPARTIALCONSUMP.toString() + "=true; to ignore it.");
    return;
  }

  public void processOp(Object row, int tag) throws HiveException {
    // initialize the user's process only when you recieve the first row
    if (firstRow) {
      firstRow = false;
      try {
        String [] cmdArgs = splitArgs(conf.getScriptCmd());

        String prog = cmdArgs[0];
        File currentDir = new File(".").getAbsoluteFile();

        if (!new File(prog).isAbsolute()) {
          PathFinder finder = new PathFinder("PATH");
          finder.prependPathComponent(currentDir.toString());
          File f = finder.getAbsolutePath(prog);
          if (f != null) {
            cmdArgs[0] = f.getAbsolutePath();
          }
          f = null;
        }

        String [] wrappedCmdArgs = addWrapper(cmdArgs);
        LOG.info("Executing " + Arrays.asList(wrappedCmdArgs));
        LOG.info("tablename=" + hconf.get(HiveConf.ConfVars.HIVETABLENAME.varname));
        LOG.info("partname=" + hconf.get(HiveConf.ConfVars.HIVEPARTITIONNAME.varname));
        LOG.info("alias=" + alias);

        ProcessBuilder pb = new ProcessBuilder(wrappedCmdArgs);
        Map<String, String> env = pb.environment();
        addJobConfToEnvironment(hconf, env);
        env.put(safeEnvVarName(HiveConf.ConfVars.HIVEALIAS.varname), String.valueOf(alias));

        // Create an environment variable that uniquely identifies this script operator
        String idEnvVarName = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVESCRIPTIDENVVAR);
        String idEnvVarVal = this.getOperatorId();
        env.put(safeEnvVarName(idEnvVarName), idEnvVarVal);

        scriptPid = pb.start();       // Runtime.getRuntime().exec(wrappedCmdArgs);

        DataOutputStream scriptOut = new DataOutputStream(new BufferedOutputStream(scriptPid.getOutputStream()));
        DataInputStream  scriptIn = new DataInputStream(new BufferedInputStream(scriptPid.getInputStream()));
        DataInputStream  scriptErr = new DataInputStream(new BufferedInputStream(scriptPid.getErrorStream()));

        scriptOutWriter = conf.getInRecordWriterClass().newInstance();
        scriptOutWriter.initialize(scriptOut, hconf);

        RecordReader scriptOutputReader = conf.getOutRecordReaderClass().newInstance();
        scriptOutputReader.initialize(scriptIn, hconf, conf.getScriptOutputInfo().getProperties());

        outThread = new StreamThread(scriptOutputReader, new OutputStreamProcessor(
                                                                                   scriptOutputDeserializer.getObjectInspector()), "OutputProcessor");

        RecordReader scriptErrReader = conf.getOutRecordReaderClass().newInstance();
        scriptErrReader.initialize(scriptErr, hconf, conf.getScriptOutputInfo().getProperties());

        errThread = new StreamThread(scriptErrReader,
                                     new ErrorStreamProcessor
                                     (HiveConf.getIntVar(hconf, HiveConf.ConfVars.SCRIPTERRORLIMIT)),
                                     "ErrorProcessor");

        if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVESCRIPTAUTOPROGRESS)) {
          autoProgressor = new AutoProgressor(this.getClass().getName(), reporter,
                                              Utilities.getDefaultNotificationInterval(hconf));
          autoProgressor.go();
        }

        outThread.start();
        errThread.start();
      } catch (Exception e) {
        throw new HiveException ("Cannot initialize ScriptOperator", e);
      }
    }

    if(scriptError != null) {
      throw new HiveException(scriptError);
    }

    try {
      Writable res = scriptInputSerializer.serialize(row, inputObjInspectors[tag]);
      scriptOutWriter.write(res);
    } catch (SerDeException e) {
      LOG.error("Error in serializing the row: " + e.getMessage());
      scriptError = e;
      serialize_error_count.set(serialize_error_count.get() + 1);
      throw new HiveException(e);
    } catch (IOException e) {
      if(isBrokenPipeException(e) && allowPartialConsumption()) {
        setDone(true);
        LOG.warn("Got broken pipe during write: ignoring exception and setting operator to done");
      } else {
        LOG.error("Error in writing to script: " + e.getMessage());
        if(isBrokenPipeException(e)) {
          displayBrokenPipeInfo();
        }
        scriptError = e;
        throw new HiveException(e);
      }
    }
  }

  public void close(boolean abort) throws HiveException {

    boolean new_abort = abort;
    if(!abort) {
      if(scriptError != null) {
        throw new HiveException(scriptError);
      }
      // everything ok. try normal shutdown
      try {
        try {
          if (scriptOutWriter != null)
            scriptOutWriter.close();
        } catch (IOException e) {
          if(isBrokenPipeException(e) && allowPartialConsumption()) {
            LOG.warn("Got broken pipe: ignoring exception");
          } else {
            if(isBrokenPipeException(e)) {
              displayBrokenPipeInfo();
            }
            throw e;
          }
        }
        int exitVal = 0;
        if (scriptPid != null)
          exitVal = scriptPid.waitFor();
        if (exitVal != 0) {
          LOG.error("Script failed with code " + exitVal);
          new_abort = true;
        };
      } catch (IOException e) {
        LOG.error("Got ioexception: " + e.getMessage());
        e.printStackTrace();
        new_abort = true;
      } catch (InterruptedException e) { }

    } else {

      // Error already occurred, but we still want to get the
      // error code of the child process if possible.
      try {
        // Interrupt the current thread after 1 second
        final Thread mythread = Thread.currentThread();
        Timer timer = new Timer(true);
        timer.schedule(new TimerTask(){
          @Override
          public void run() {
            mythread.interrupt();
          }},
          1000);
        // Wait for the child process to finish
        int exitVal = 0;
        if (scriptPid != null)
          scriptPid.waitFor();
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
      if (outThread != null)
        outThread.join(0);
    } catch (Exception e) {
      LOG.warn("Exception in closing outThread: " + StringUtils.stringifyException(e));
    }

    try {
      if (errThread != null)
        errThread.join(0);
    } catch (Exception e) {
      LOG.warn("Exception in closing errThread: " + StringUtils.stringifyException(e));
    }

    try {
      if (scriptPid != null)
        scriptPid.destroy();
    } catch (Exception e) {
      LOG.warn("Exception in destroying scriptPid: " + StringUtils.stringifyException(e));
    }

    super.close(new_abort);

    if(new_abort && !abort) {
      throw new HiveException ("Hit error while closing ..");
    }
  }


  interface StreamProcessor {
    public void processLine(Writable line) throws HiveException;
    public void close() throws HiveException;
  }


  class OutputStreamProcessor implements StreamProcessor {
    Object row;
    ObjectInspector rowInspector;
    public OutputStreamProcessor(ObjectInspector rowInspector) {
      this.rowInspector = rowInspector;
    }
    public void processLine(Writable line) throws HiveException {
      try {
        row = scriptOutputDeserializer.deserialize(line);
      } catch (SerDeException e) {
        deserialize_error_count.set(deserialize_error_count.get()+1);
        return;
      }
      forward(row, rowInspector);
    }
    public void close() {
    }
  }

  /**
   * The processor for stderr stream.
   *
   * TODO: In the future when we move to hadoop 0.18 and above, we should borrow the logic
   * from HadoopStreaming: PipeMapRed.java MRErrorThread to support counters and status
   * updates.
   */
  class ErrorStreamProcessor implements StreamProcessor {
    private long bytesCopied = 0;
    private long maxBytes;

    private long lastReportTime;

    public ErrorStreamProcessor (int maxBytes) {
      this.maxBytes = (long)maxBytes;
      lastReportTime = 0;
    }

    public void processLine(Writable line) throws HiveException {

      String stringLine = line.toString();
      int len = 0;

      if (line instanceof Text)
        len = ((Text)line).getLength();
      else if (line instanceof BytesWritable)
        len = ((BytesWritable)line).getSize();

      // Report progress for each stderr line, but no more frequently than once per minute.
      long now = System.currentTimeMillis();
      // reporter is a member variable of the Operator class.
      if (now - lastReportTime > 60 * 1000 && reporter != null) {
        LOG.info("ErrorStreamProcessor calling reporter.progress()");
        lastReportTime = now;
        reporter.progress();
      }

      if((maxBytes < 0) || (bytesCopied < maxBytes)) {
        System.err.println(stringLine);
      }
      if (bytesCopied < maxBytes && bytesCopied + len >= maxBytes) {
        System.err.println("Operator " + id + " " + getName()
            + ": exceeding stderr limit of " + maxBytes + " bytes, will truncate stderr messages.");
      }
      bytesCopied += len;
    }
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

    public void run() {
      try {
        Writable row = in.createRow();

        while(true) {
          long bytes = in.next(row);

          if(bytes <= 0) {
            break;
          }
          proc.processLine(row);
        }
        LOG.info("StreamThread "+name+" done");

      } catch (Throwable th) {
        scriptError = th;
        LOG.warn(StringUtils.stringifyException(th));
      } finally {
        try {
          if (in != null) {
            in.close();
          }
          proc.close();
        } catch (Exception e) {
          LOG.warn(name + ": error in closing ..");
          LOG.warn(StringUtils.stringifyException(e));
        }
      }
    }
  }

  /**
   *  Wrap the script in a wrapper that allows admins to control
   **/
  protected String [] addWrapper(String [] inArgs) {
    String wrapper = HiveConf.getVar(hconf, HiveConf.ConfVars.SCRIPTWRAPPER);
    if(wrapper == null) {
      return inArgs;
    }

    String [] wrapComponents = splitArgs(wrapper);
    int totallength = wrapComponents.length + inArgs.length;
    String [] finalArgv = new String [totallength];
    for(int i=0; i<wrapComponents.length; i++) {
      finalArgv[i] = wrapComponents[i];
    }
    for(int i=0; i < inArgs.length; i++) {
      finalArgv[wrapComponents.length+i] = inArgs[i];
    }
    return (finalArgv);
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
    return "SCR";
  }

  public int getType() {
    return OperatorType.SCRIPT;
  }
}
