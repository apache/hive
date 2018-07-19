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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import jline.Terminal;
import jline.TerminalFactory;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import jline.console.history.MemoryHistory;
import org.apache.hadoop.hive.conf.HiveConf;

class BeeLineOpts implements Completer {
  public static final int DEFAULT_MAX_WIDTH = 80;
  public static final int DEFAULT_MAX_HEIGHT = 80;
  public static final int DEFAULT_HEADER_INTERVAL = 100;
  public static final String DEFAULT_ISOLATION_LEVEL =
      "TRANSACTION_REPEATABLE_READ";
  public static final String PROPERTY_PREFIX = "beeline.";
  public static final String PROPERTY_NAME_EXIT =
      PROPERTY_PREFIX + "system.exit";
  public static final String DEFAULT_NULL_STRING = "NULL";
  public static final char DEFAULT_DELIMITER_FOR_DSV = '|';
  public static final int DEFAULT_MAX_COLUMN_WIDTH = 50;
  public static final int DEFAULT_INCREMENTAL_BUFFER_ROWS = 1000;
  public static final String DEFAULT_DELIMITER = ";";

  public static final String URL_ENV_PREFIX = "BEELINE_URL_";

  private final BeeLine beeLine;
  private boolean autosave = false;
  private boolean silent = false;
  private boolean color = false;
  private boolean showHeader = true;
  private boolean escapeCRLF = false;
  private boolean showDbInPrompt = false;
  private int headerInterval = 100;
  private boolean fastConnect = true;
  private boolean autoCommit = true;
  private boolean verbose = false;
  private boolean force = false;
  private boolean incremental = true;
  private boolean convertBinaryArrayToString = true;
  private int incrementalBufferRows = DEFAULT_INCREMENTAL_BUFFER_ROWS;
  private boolean showWarnings = false;
  private boolean showNestedErrs = false;
  private boolean showElapsedTime = true;
  private boolean entireLineAsCommand = false;
  private String numberFormat = "default";
  private final Terminal terminal = TerminalFactory.get();
  private int maxWidth = DEFAULT_MAX_WIDTH;
  private int maxHeight = DEFAULT_MAX_HEIGHT;
  private int maxColumnWidth = DEFAULT_MAX_COLUMN_WIDTH;
  int timeout = -1;
  private String isolation = DEFAULT_ISOLATION_LEVEL;
  private String outputFormat = "table";
  // This configuration is used only for client side configuration.
  private HiveConf conf;
  private boolean trimScripts = true;
  private boolean allowMultiLineCommand = true;

  //This can be set for old behavior of nulls printed as empty strings
  private boolean nullEmptyString = false;

  private boolean truncateTable = false;

  private final File rcFile = new File(saveDir(), "beeline.properties");
  private String historyFile = new File(saveDir(), "history").getAbsolutePath();
  private int maxHistoryRows = MemoryHistory.DEFAULT_MAX_SIZE;

  private String scriptFile = null;
  private String[] initFiles = null;
  private String authType = null;
  private char delimiterForDSV = DEFAULT_DELIMITER_FOR_DSV;

  private Map<String, String> hiveVariables = new HashMap<String, String>();
  private Map<String, String> hiveConfVariables = new HashMap<String, String>();
  private boolean helpAsked;

  private String lastConnectedUrl = null;

  private TreeSet<String> cachedPropertyNameSet = null;

  private String delimiter = DEFAULT_DELIMITER;

  @Retention(RetentionPolicy.RUNTIME)
  public @interface Ignore {
    // marker annotations for functions that Reflector should ignore / pretend it does not exist

    // NOTE: BeeLineOpts uses Reflector in an extensive way to call getters and setters on itself
    // If you want to add any getters or setters to this class, but not have it interfere with
    // saved variables in beeline.properties, careful use of this marker is needed.
    // Also possible to get this by naming these functions obtainBlah instead of getBlah
    // and so on, but that is not explicit and will likely surprise people looking at the
    // code in the future. Better to be explicit in intent.
  }

  public interface Env {
    // Env interface to mock out dealing with Environment variables
    // This allows us to interface with Environment vars through
    // BeeLineOpts while allowing tests to mock out Env setting if needed.
    String get(String envVar);
  }

  public static Env env = new Env() {
    @Override
    public String get(String envVar) {
      return System.getenv(envVar); // base env impl simply defers to System.getenv.
    }
  };

  public BeeLineOpts(BeeLine beeLine, Properties props) {
    this.beeLine = beeLine;
    if (terminal.getWidth() > 0) {
      maxWidth = terminal.getWidth();
    }
    if (terminal.getHeight() > 0) {
      maxHeight = terminal.getHeight();
    }
    loadProperties(props);
  }

  public Completer[] optionCompleters() {
    return new Completer[] {this};
  }

  public String[] possibleSettingValues() {
    List<String> vals = new LinkedList<String>();
    vals.addAll(Arrays.asList(new String[] { "yes", "no" }));
    return vals.toArray(new String[vals.size()]);
  }


  /**
   * The save directory if HOME/.beeline/ on UNIX, and
   * HOME/beeline/ on Windows.
   */
  public File saveDir() {
    String dir = System.getProperty("beeline.rcfile");
    if (dir != null && dir.length() > 0) {
      return new File(dir);
    }

    File f = new File(System.getProperty("user.home"),
        (System.getProperty("os.name").toLowerCase()
            .indexOf("windows") != -1 ? "" : ".") + "beeline")
        .getAbsoluteFile();
    try {
      f.mkdirs();
    } catch (Exception e) {
    }
    return f;
  }


  @Override
  public int complete(String buf, int pos, List cand) {
    try {
      return new StringsCompleter(propertyNames()).complete(buf, pos, cand);
    } catch (Exception e) {
      beeLine.handleException(e);
      return -1;
    }
  }


  public void save() throws IOException {
    try (OutputStream out = new FileOutputStream(rcFile)) {
      save(out);
    }
  }

  public void save(OutputStream out) throws IOException {
    try {
      Properties props = toProperties();
      // don't save maxwidth: it is automatically set based on
      // the terminal configuration
      props.remove(PROPERTY_PREFIX + "maxwidth");
      props.store(out, beeLine.getApplicationTitle());
    } catch (Exception e) {
      beeLine.handleException(e);
    }
  }

  String[] propertyNames()
      throws IllegalAccessException, InvocationTargetException {
    Set<String> names = propertyNamesSet(); // make sure we initialize if necessary
    return names.toArray(new String[names.size()]);
  }

  Set<String> propertyNamesSet()
    throws IllegalAccessException, InvocationTargetException {
    if (cachedPropertyNameSet == null){
      TreeSet<String> names = new TreeSet<String>();

      // get all the values from getXXX methods
      Method[] m = getClass().getDeclaredMethods();
      for (int i = 0; m != null && i < m.length; i++) {
        if (!(m[i].getName().startsWith("get"))) {
          continue;
        }
        if (m[i].getAnnotation(Ignore.class) != null){
          continue; // not actually a getter
        }
        if (m[i].getParameterTypes().length != 0) {
          continue;
        }
        String propName = m[i].getName().substring(3).toLowerCase();
        names.add(propName);
      }
      cachedPropertyNameSet = names;
    }
    return cachedPropertyNameSet;
  }

  public Properties toProperties()
      throws IllegalAccessException, InvocationTargetException,
      ClassNotFoundException {
    Properties props = new Properties();

    String[] names = propertyNames();
    for (int i = 0; names != null && i < names.length; i++) {
      Object o = beeLine.getReflector().invoke(this, "get" + names[i], new Object[0]);
      props.setProperty(PROPERTY_PREFIX + names[i],
          o == null ? "" : o.toString());
    }
    beeLine.debug("properties: " + props.toString());
    return props;
  }


  public void load() throws IOException {
    try (InputStream in = new FileInputStream(rcFile)) {
      load(in);
    }
  }


  public void load(InputStream fin) throws IOException {
    Properties p = new Properties();
    p.load(fin);
    loadProperties(p);
  }

  /**
   * Update the options after connection is established in CLI mode.
   */
  public void updateBeeLineOptsFromConf() {
    if (!beeLine.isBeeLine()) {
      if (conf == null) {
        conf = beeLine.getCommands().getHiveConf(false);
      }
      setForce(HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIIGNOREERRORS));
    }
  }

  public void setHiveConf(HiveConf conf) {
    this.conf = conf;
  }

  public void loadProperties(Properties props) {
    for (Object element : props.keySet()) {
      String key = element.toString();
      if (key.equals(PROPERTY_NAME_EXIT)) {
        // fix for sf.net bug 879422
        continue;
      }
      if (key.startsWith(PROPERTY_PREFIX)) {
        set(key.substring(PROPERTY_PREFIX.length()),
            props.getProperty(key));
      }
    }
  }

  public void set(String key, String value) {
    set(key, value, false);
  }

  public boolean set(String key, String value, boolean quiet) {
    try {
      beeLine.getReflector().invoke(this, "set" + key, new Object[] {value});
      return true;
    } catch (Exception e) {
      if (!quiet) {
        beeLine.error(beeLine.loc("error-setting", new Object[] {key, e}));
      }
      return false;
    }
  }

  public void setFastConnect(boolean fastConnect) {
    this.fastConnect = fastConnect;
  }

  public String getAuthType() {
    return authType;
  }

  public void setAuthType(String authType) {
    this.authType = authType;
  }

  public boolean getFastConnect() {
    return fastConnect;
  }

  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit = autoCommit;
  }

  public boolean getAutoCommit() {
    return autoCommit;
  }

  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  public boolean getVerbose() {
    return verbose;
  }

  public void setShowWarnings(boolean showWarnings) {
    this.showWarnings = showWarnings;
  }

  public boolean getShowWarnings() {
    return showWarnings;
  }

  public void setShowNestedErrs(boolean showNestedErrs) {
    this.showNestedErrs = showNestedErrs;
  }

  public boolean getShowNestedErrs() {
    return showNestedErrs;
  }

  public void setShowElapsedTime(boolean showElapsedTime) {
    this.showElapsedTime = showElapsedTime;
  }

  public boolean getShowElapsedTime() {
    return showElapsedTime;
  }

  public void setNumberFormat(String numberFormat) {
    this.numberFormat = numberFormat;
  }

  public String getNumberFormat() {
    return numberFormat;
  }

  public void setConvertBinaryArrayToString(boolean convert) {
    this.convertBinaryArrayToString = convert;
  }

  public boolean getConvertBinaryArrayToString() {
    return this.convertBinaryArrayToString;
  }

  public void setMaxWidth(int maxWidth) {
    this.maxWidth = maxWidth;
  }

  public int getMaxWidth() {
    return maxWidth;
  }

  public void setMaxColumnWidth(int maxColumnWidth) {
    this.maxColumnWidth = maxColumnWidth;
  }

  public int getMaxColumnWidth() {
    return maxColumnWidth;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public int getTimeout() {
    return timeout;
  }

  public void setIsolation(String isolation) {
    this.isolation = isolation;
  }

  public String getIsolation() {
    return isolation;
  }

  public void setEntireLineAsCommand(boolean entireLineAsCommand) {
    this.entireLineAsCommand = entireLineAsCommand;
  }

  public boolean getEntireLineAsCommand() {
    return entireLineAsCommand;
  }

  public void setHistoryFile(String historyFile) {
    this.historyFile = historyFile;
  }

  public String getHistoryFile() {
    return historyFile;
  }

  /**
   * @param numRows - the number of rows to store in history file
   */
  public void setMaxHistoryRows(int numRows) {
    this.maxHistoryRows = numRows;
  }

  public int getMaxHistoryRows() {
    return maxHistoryRows;
  }

  public void setScriptFile(String scriptFile) {
    this.scriptFile = scriptFile;
  }

  public String getScriptFile() {
    return scriptFile;
  }

  public String[] getInitFiles() {
    return initFiles;
  }

  public void setInitFiles(String[] initFiles) {
    this.initFiles = initFiles;
  }

  public void setColor(boolean color) {
    this.color = color;
  }

  public boolean getColor() {
    return color;
  }

  public void setShowHeader(boolean showHeader) {
    this.showHeader = showHeader;
  }

  public boolean getShowHeader() {
    if (beeLine.isBeeLine()) {
      return showHeader;
    } else {
      boolean header;
      HiveConf conf = beeLine.getCommands().getHiveConf(true);
      header = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER);
      return header;
    }
  }

  public void setEscapeCRLF(boolean escapeCRLF) {
    this.escapeCRLF = escapeCRLF;
  }

  public boolean getEscapeCRLF() {
    if (beeLine.isBeeLine()) {
      return escapeCRLF;
    } else { //hive cli
      if(conf != null) {
        return HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_ESCAPE_CRLF);
      } else {
        return false;
      }
    }
  }

  public void setShowDbInPrompt(boolean showDbInPrompt) {
    this.showDbInPrompt = showDbInPrompt;
  }

  /**
   * In beeline mode returns the beeline option provided by command line argument or config file
   * In compatibility mode returns the value of the hive.cli.print.current.db config variable
   * @return Should the current db displayed in the prompt
   */
  public boolean getShowDbInPrompt() {
    if (beeLine.isBeeLine()) {
      return showDbInPrompt;
    } else {
      HiveConf conf = beeLine.getCommands().getHiveConf(true);
      return HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIPRINTCURRENTDB);
    }
  }

  public void setHeaderInterval(int headerInterval) {
    this.headerInterval = headerInterval;
  }

  public int getHeaderInterval() {
    return headerInterval;
  }

  public void setForce(boolean force) {
    this.force = force;
  }

  public boolean getForce() {
    return force;
  }

  public void setIncremental(boolean incremental) {
    this.incremental = incremental;
  }

  public boolean getIncremental() {
    return incremental;
  }

  public void setIncrementalBufferRows(int incrementalBufferRows) {
    this.incrementalBufferRows = incrementalBufferRows;
  }

  public int getIncrementalBufferRows() {
    return this.incrementalBufferRows;
  }

  public void setSilent(boolean silent) {
    this.silent = silent;
  }

  public boolean isSilent() {
    return silent;
  }

  public void setAutosave(boolean autosave) {
    this.autosave = autosave;
  }

  public boolean getAutosave() {
    return autosave;
  }

  public void setOutputFormat(String outputFormat) {
    if(outputFormat.equalsIgnoreCase("csv") || outputFormat.equalsIgnoreCase("tsv")) {
      beeLine.info("Format " + outputFormat + " is deprecated, please use " + outputFormat + "2");
    }
    this.outputFormat = outputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setTrimScripts(boolean trimScripts) {
    this.trimScripts = trimScripts;
  }

  public boolean getTrimScripts() {
    return trimScripts;
  }

  public void setMaxHeight(int maxHeight) {
    this.maxHeight = maxHeight;
  }

  public int getMaxHeight() {
    return maxHeight;
  }

  @Ignore
  public File getPropertiesFile() {
    return rcFile;
  }

  public Map<String, String> getHiveVariables() {
    return hiveVariables;
  }

  public void setHiveVariables(Map<String, String> hiveVariables) {
    this.hiveVariables = hiveVariables;
  }

  public boolean isAllowMultiLineCommand() {
    return allowMultiLineCommand;
  }

  public void setAllowMultiLineCommand(boolean allowMultiLineCommand) {
    this.allowMultiLineCommand = allowMultiLineCommand;
  }

  /**
   * Use getNullString() to get the null string to be used.
   * @return true if null representation should be an empty string
   */
  public boolean getNullEmptyString() {
    return nullEmptyString;
  }

  public void setNullEmptyString(boolean nullStringEmpty) {
    this.nullEmptyString = nullStringEmpty;
  }

  @Ignore
  public String getNullString(){
    return nullEmptyString ? "" : DEFAULT_NULL_STRING;
  }

  public Map<String, String> getHiveConfVariables() {
    return hiveConfVariables;
  }

  public void setHiveConfVariables(Map<String, String> hiveConfVariables) {
    this.hiveConfVariables = hiveConfVariables;
  }

  public boolean getTruncateTable() {
    return truncateTable;
  }

  public void setTruncateTable(boolean truncateTable) {
    this.truncateTable = truncateTable;
  }

  public char getDelimiterForDSV() {
    return delimiterForDSV;
  }

  public void setDelimiterForDSV(char delimiterForDSV) {
    this.delimiterForDSV = delimiterForDSV;
  }

  @Ignore
  public HiveConf getConf() {
    return conf;
  }

  public void setHelpAsked(boolean helpAsked) {
    this.helpAsked = helpAsked;
  }

  public boolean isHelpAsked() {
    return helpAsked;
  }

  public String getLastConnectedUrl(){
    return lastConnectedUrl;
  }

  public void setLastConnectedUrl(String lastConnectedUrl){
    this.lastConnectedUrl = lastConnectedUrl;
  }

  public String getDelimiter() {
    return this.delimiter;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  @Ignore
  public static Env getEnv(){
    return env;
  }

  @Ignore
  public static void setEnv(Env envToUse){
    env = envToUse;
  }
}

