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

package org.apache.hadoop.hive.ql.session;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.*;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory;

import org.apache.commons.lang.StringUtils;

/**
 * SessionState encapsulates common data associated with a session
 * 
 * Also provides support for a thread static session object that can
 * be accessed from any point in the code to interact with the user
 * and to retrieve configuration information
 */
public class SessionState {

  /**
   * current configuration
   */ 
  protected HiveConf conf;

  /**
   * silent mode
   */
  protected boolean isSilent;

  /**
   * cached current connection to Hive MetaStore
   */
  protected Hive db;

  /*
   *  HiveHistory Object 
   */
  protected HiveHistory hiveHist;
  /**
   * Streams to read/write from
   */
  public PrintStream out;
  public InputStream in;
  public PrintStream err;


  public HiveConf getConf() { return conf; }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public boolean getIsSilent() {
    return isSilent;
  }

  public void setIsSilent(boolean isSilent) {
    this.isSilent = isSilent;
  }

  public SessionState() {
    this(null, null);
  }

  public SessionState (HiveConf conf) {
    this (conf, null);
  }
  
  public SessionState (HiveConf conf, Hive db) {
    this.conf = conf;
    this.db = db;

    for(HiveConf.ConfVars oneVar: HiveConf.metaVars) {
      dbOptions.put(oneVar, conf.getVar(oneVar));
    }
  }

  /**
   * cached values of such options
   */
  private final HashMap<HiveConf.ConfVars, String> dbOptions =
    new HashMap<HiveConf.ConfVars, String> ();

  public Hive getDb() throws HiveException {
    boolean needsRefresh = false;

    for(HiveConf.ConfVars oneVar: HiveConf.metaVars) {
      if(!StringUtils.isEmpty(StringUtils.difference(dbOptions.get(oneVar), conf.getVar(oneVar)))) {
        needsRefresh = true;
        break;
      }
    }
    
    if((db == null) || needsRefresh) {
      db = Hive.get(conf, needsRefresh);
    }
  
    return db;
  }

  public void setCmd(String cmdString) {
    conf.setVar(HiveConf.ConfVars.HIVEQUERYSTRING, cmdString);
  }

  public String getCmd() {
    return (conf.getVar(HiveConf.ConfVars.HIVEQUERYSTRING));
  }
  
  
  public String getQueryId() {
    return (conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
  }

  public String getSessionId() {
    return (conf.getVar(HiveConf.ConfVars.HIVESESSIONID));
  }

  /**
   * Singleton Session object per thread.
   *
   **/
  private static ThreadLocal<SessionState> tss = new ThreadLocal<SessionState> ();

  /**
   * start a new session and set it to current session
   */
  public static SessionState start(HiveConf conf) {
    SessionState ss = new SessionState (conf);
    ss.getConf().setVar(HiveConf.ConfVars.HIVESESSIONID, makeSessionId());
    ss.hiveHist = new HiveHistory(ss);
    tss.set(ss);
    return (ss);
  }

  /**
   * set current session to existing session object
   * if a thread is running multiple sessions - it must call this method with the new
   * session object when switching from one session to another
   */
  public static SessionState start(SessionState startSs) {
   
    tss.set(startSs);
    if(StringUtils.isEmpty(startSs.getConf().getVar(HiveConf.ConfVars.HIVESESSIONID))) {
      startSs.getConf().setVar(HiveConf.ConfVars.HIVESESSIONID, makeSessionId());
    }
    
    if (startSs.hiveHist == null){
      startSs.hiveHist = new HiveHistory(startSs);
    }
    return startSs;
  }

  /**
   * get the current session
   */
  public static SessionState get() {
    return tss.get();
  }

 
  /**
   * get hiveHitsory object which does structured logging
   * @return The hive history object
   */
  public HiveHistory getHiveHistory(){
    return hiveHist;
  }
  
  
  private static String makeSessionId() {
    GregorianCalendar gc = new GregorianCalendar();
    String userid = System.getProperty("user.name");

    return userid + "_" +
      String.format("%1$4d%2$02d%3$02d%4$02d%5$02d", gc.get(Calendar.YEAR),
                    gc.get(Calendar.MONTH) + 1,
                    gc.get(Calendar.DAY_OF_MONTH),
                    gc.get(Calendar.HOUR_OF_DAY),
                    gc.get(Calendar.MINUTE));
  }

  public static final String HIVE_L4J = "hive-log4j.properties";

  public static void initHiveLog4j () {
    // allow hive log4j to override any normal initialized one
    URL hive_l4j = SessionState.class.getClassLoader().getResource(HIVE_L4J);
    if(hive_l4j == null) {
      System.out.println(HIVE_L4J + " not found");
    } else {
      LogManager.resetConfiguration();
      PropertyConfigurator.configure(hive_l4j);
    }
  }

  /**
   * This class provides helper routines to emit informational and error messages to the user
   * and log4j files while obeying the current session's verbosity levels.
   * 
   * NEVER write directly to the SessionStates standard output other than to emit result data
   * DO use printInfo and printError provided by LogHelper to emit non result data strings
   * 
   * It is perfectly acceptable to have global static LogHelper objects (for example - once per module)
   * LogHelper always emits info/error to current session as required.
   */
  public static class LogHelper {

    protected Log LOG;
    protected boolean isSilent;
    
    public LogHelper(Log LOG) {
      this(LOG, false);
    }

    public LogHelper(Log LOG, boolean isSilent) {
      this.LOG = LOG;
      this.isSilent = isSilent;
    }

    public PrintStream getOutStream() {
      SessionState ss = SessionState.get();
      return ((ss != null) && (ss.out != null)) ? ss.out : System.out;   
    }

    public PrintStream getErrStream() {
      SessionState ss = SessionState.get();
      return ((ss != null) && (ss.err != null)) ? ss.err : System.err;
    }

    public boolean getIsSilent() {
      SessionState ss = SessionState.get();
      // use the session or the one supplied in constructor
      return (ss != null) ? ss.getIsSilent() : isSilent;
    }

    public void printInfo(String info) {
      printInfo(info, null);
    }

    public void printInfo(String info, String detail) {
      if(!getIsSilent()) {
        getErrStream().println(info);
      }
      LOG.info(info + StringUtils.defaultString(detail));
    }

    public void printError(String error) {
      printError(error, null);
    }

    public void printError(String error, String detail) {
      getErrStream().println(error);
      LOG.error(error + StringUtils.defaultString(detail));
    }
  }

  private static LogHelper _console;
  /**
   * initialize or retrieve console object for SessionState
   */
  public static LogHelper getConsole() {
    if(_console == null) {
      Log LOG = LogFactory.getLog("SessionState");
      _console = new LogHelper(LOG);
    }
    return _console;
  }

  public static String validateFile(Set<String> curFiles, String newFile) {
    SessionState ss = SessionState.get();
    LogHelper console = getConsole();
    Configuration conf = (ss == null) ? new Configuration() : ss.getConf();

    try {
      if(Utilities.realFile(newFile, conf) != null)
        return newFile;
      else {
        console.printError(newFile + " does not exist");
        return null;
      }
    } catch (IOException e) {
      console.printError("Unable to validate " + newFile + "\nException: " + e.getMessage(),
                         "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return null;
    }
  }

  public static boolean registerJar(String newJar) {
    LogHelper console = getConsole();
    try {
      Utilities.addToClassPath(StringUtils.split(newJar, ","));
      console.printInfo("Added " + newJar + " to class path");
      return true;
    } catch (Exception e) {
      console.printError("Unable to register " + newJar + "\nException: " + e.getMessage(),
                         "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return false;
    }
  }
  
  public static boolean unregisterJar(String jarsToUnregister) {
    LogHelper console = getConsole();
    try {
      Utilities.removeFromClassPath(StringUtils.split(jarsToUnregister, ","));
      console.printInfo("Deleted " + jarsToUnregister + " from class path");
      return true;
    } catch (Exception e) {
      console.printError("Unable to unregister " + jarsToUnregister + "\nException: " + e.getMessage(),
                         "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return false;
    }
  }

  public static interface ResourceHook {
    public String preHook(Set<String> cur, String s);
    public boolean postHook(Set<String> cur, String s);
  }

  public static enum ResourceType {
    FILE(new ResourceHook () {
        public String preHook(Set<String> cur, String s) { return validateFile(cur, s); }
        public boolean postHook(Set<String> cur, String s) { return true; }
      }),

    JAR(new ResourceHook () {
        public String preHook(Set<String> cur, String s) {
          String newJar = validateFile(cur, s);
          if(newJar != null) {
            return (registerJar(newJar) ? newJar : null);
          } else {
            return null;
          }
        }
        public boolean postHook(Set<String> cur, String s) { return unregisterJar(s); }
      });

    public ResourceHook hook;

    ResourceType(ResourceHook hook) {
      this.hook = hook;
    }
  };

  public static ResourceType find_resource_type(String s) {
    
    s = s.trim().toUpperCase();
    
    try {
      return ResourceType.valueOf(s);
    } catch (IllegalArgumentException e) {
    }
    
    // try singular
    if(s.endsWith("S")) {
      s = s.substring(0, s.length()-1);
    } else {
      return null;
    }

    try {
      return ResourceType.valueOf(s);
    } catch (IllegalArgumentException e) {
    }
    return null;
  }

  private HashMap<ResourceType, HashSet<String>> resource_map = new HashMap<ResourceType, HashSet<String>> ();

  public void add_resource(ResourceType t, String value) {
    if(resource_map.get(t) == null) {
      resource_map.put(t, new HashSet<String> ());
    }

    String fnlVal = value;
    if(t.hook != null) {
      fnlVal = t.hook.preHook(resource_map.get(t), value);
      if(fnlVal == null)
        return;
    }
    resource_map.get(t).add(fnlVal);
  }

  public boolean delete_resource(ResourceType t, String value) {
    if(resource_map.get(t) == null) {
      return false;
    }
    if(t.hook != null) {
      if(!t.hook.postHook(resource_map.get(t), value))
        return false;
    }
    return (resource_map.get(t).remove(value));
  }

  public Set<String> list_resource(ResourceType t, List<String> filter) {
    if(resource_map.get(t) == null) {
      return null;
    }
    Set<String> orig = resource_map.get(t);
    if(filter == null) {
      return orig;
    } else {
      Set<String> fnl = new HashSet<String> ();
      for(String one: orig) {
        if(filter.contains(one)) {
          fnl.add(one);
        }
      }
      return fnl;
    }
  }

  public void delete_resource(ResourceType t) {
    if(resource_map.get(t) != null) {
      for(String value : resource_map.get(t)) {
        delete_resource(t, value);
      }
      resource_map.remove (t);
    }
  }
}
