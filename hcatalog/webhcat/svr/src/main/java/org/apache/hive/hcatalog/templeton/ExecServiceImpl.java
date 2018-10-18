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
package org.apache.hive.hcatalog.templeton;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.Shell;

class StreamOutputWriter extends Thread
{
  InputStream is;
  String type;
  PrintWriter out;

  StreamOutputWriter(InputStream is, String type, OutputStream outStream)
  {
    this.is = is;
    this.type = type;
    this.out = new PrintWriter(outStream, true);
  }

  @Override
  public void run()
  {
    try
    {
      BufferedReader br =
        new BufferedReader(new InputStreamReader(is));
      String line = null;
      while ( (line = br.readLine()) != null){
        out.println(line);
      }
    } catch (IOException ioe)
    {
      ioe.printStackTrace();  
    }
  }
}

/**
 * Execute a local program.  This is a singleton service that will
 * execute programs as non-privileged users on the local box.  See
 * ExecService.run and ExecService.runUnlimited for details.
 */
public class ExecServiceImpl implements ExecService {
  private static final Logger LOG = LoggerFactory.getLogger(ExecServiceImpl.class);
  private static AppConfig appConf = Main.getAppConfigInstance();

  private static volatile ExecServiceImpl theSingleton;

  /** Windows CreateProcess synchronization object */
  private static final Object WindowsProcessLaunchLock = new Object();

  /**
   * Retrieve the singleton.
   */
  public static synchronized ExecServiceImpl getInstance() {
    if (theSingleton == null) {
      theSingleton = new ExecServiceImpl();
    }
    return theSingleton;
  }

  private Semaphore avail;

  private ExecServiceImpl() {
    avail = new Semaphore(appConf.getInt(AppConfig.EXEC_MAX_PROCS_NAME, 16));
  }

  /**
   * Run the program synchronously as the given user. We rate limit
   * the number of processes that can simultaneously created for
   * this instance.
   *
   * @param program   The program to run
   * @param args      Arguments to pass to the program
   * @param env       Any extra environment variables to set
   * @return The result of the run.
   */
  public ExecBean run(String program, List<String> args,
            Map<String, String> env)
    throws NotAuthorizedException, BusyException, ExecuteException, IOException {
    boolean aquired = false;
    try {
      aquired = avail.tryAcquire();
      if (aquired) {
        return runUnlimited(program, args, env);
      } else {
        throw new BusyException();
      }
    } finally {
      if (aquired) {
        avail.release();
      }
    }
  }

  /**
   * Run the program synchronously as the given user.  Warning:
   * CommandLine will trim the argument strings.
   *
   * @param program   The program to run.
   * @param args      Arguments to pass to the program
   * @param env       Any extra environment variables to set
   * @return The result of the run.
   */
  public ExecBean runUnlimited(String program, List<String> args,
                 Map<String, String> env)
    throws NotAuthorizedException, ExecuteException, IOException {
    try {
      return auxRun(program, args, env);
    } catch (IOException e) {
      File cwd = new java.io.File(".");
      if (cwd.canRead() && cwd.canWrite())
        throw e;
      else
        throw new IOException("Invalid permissions on Templeton directory: "
          + cwd.getCanonicalPath());
    }
  }

  private ExecBean auxRun(String program, List<String> args, Map<String, String> env)
    throws NotAuthorizedException, ExecuteException, IOException {
    DefaultExecutor executor = new DefaultExecutor();
    executor.setExitValues(null);

    // Setup stdout and stderr
    int nbytes = appConf.getInt(AppConfig.EXEC_MAX_BYTES_NAME, -1);
    ByteArrayOutputStream outStream = new MaxByteArrayOutputStream(nbytes);
    ByteArrayOutputStream errStream = new MaxByteArrayOutputStream(nbytes);
    executor.setStreamHandler(new PumpStreamHandler(outStream, errStream));

    // Only run for N milliseconds
    int timeout = appConf.getInt(AppConfig.EXEC_TIMEOUT_NAME, 0);
    ExecuteWatchdog watchdog = new ExecuteWatchdog(timeout);
    executor.setWatchdog(watchdog);

    CommandLine cmd = makeCommandLine(program, args);

    LOG.info("Running: " + cmd);
    ExecBean res = new ExecBean();

    res.exitcode = executor.execute(cmd, execEnv(env));

    String enc = appConf.get(AppConfig.EXEC_ENCODING_NAME);
    res.stdout = outStream.toString(enc);
    res.stderr = errStream.toString(enc);
    try {
      watchdog.checkException();
    }
    catch (Exception ex) {
      LOG.error("Command: " + cmd + " failed. res=" + res, ex);
    }
    if(watchdog.killedProcess()) {
      String msg = " was terminated due to timeout(" + timeout + "ms).  See " + AppConfig
              .EXEC_TIMEOUT_NAME + " property"; 
      LOG.warn("Command: " + cmd + msg + " res=" + res);
      res.stderr += " Command " + msg; 
    }
    if(res.exitcode != 0) {
      LOG.info("Command: " + cmd + " failed. res=" + res);
    }
    return res;
  }

  private CommandLine makeCommandLine(String program,
                    List<String> args)
    throws NotAuthorizedException, IOException {
    String path = validateProgram(program);
    CommandLine cmd = new CommandLine(path);
    if (args != null)
      for (String arg : args)
        cmd.addArgument(arg, false);

    return cmd;
  }

  /**
   * Build the environment used for all exec calls.
   *
   * @return The environment variables.
   */
  public Map<String, String> execEnv(Map<String, String> env) {
    HashMap<String, String> res = new HashMap<String, String>();

    for (String key : appConf.getStrings(AppConfig.EXEC_ENVS_NAME)) {
      String val = System.getenv(key);
      if (val != null) {
        res.put(key, val);
      }
    }
    if (env != null)
      res.putAll(env);
    for (Map.Entry<String, String> envs : res.entrySet()) {
      LOG.info("Env " + envs.getKey() + "=" + envs.getValue());
    }
    return res;
  }

  /**
   * Given a program name, lookup the fully qualified path.  Throws
   * an exception if the program is missing or not authorized.
   *
   * @param path      The path of the program.
   * @return The path of the validated program.
   */
  public String validateProgram(String path)
    throws NotAuthorizedException, IOException {
    File f = new File(path);
    if (f.canExecute()) {
      return f.getCanonicalPath();
    } else {
      throw new NotAuthorizedException("Unable to access program: " + path);
    }
  }
}
