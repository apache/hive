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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.ChoiceFormat;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import jline.ClassNameCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.FileNameCompletor;
import jline.SimpleCompletor;


/**
 * A console SQL shell with command completion.
 * <p>
 * TODO:
 * <ul>
 * <li>User-friendly connection prompts</li>
 * <li>Page results</li>
 * <li>Handle binary data (blob fields)</li>
 * <li>Implement command aliases</li>
 * <li>Stored procedure execution</li>
 * <li>Binding parameters to prepared statements</li>
 * <li>Scripting language</li>
 * <li>XA transactions</li>
 * </ul>
 *
 */
public class BeeLine {
  private static final ResourceBundle resourceBundle =
      ResourceBundle.getBundle(BeeLine.class.getSimpleName());
  private final BeeLineSignalHandler signalHandler = null;
  private static final String separator = System.getProperty("line.separator");
  private boolean exit = false;
  private final DatabaseConnections connections = new DatabaseConnections();
  public static final String COMMAND_PREFIX = "!";
  private final Completor beeLineCommandCompletor;
  private Collection<Driver> drivers = null;
  private final BeeLineOpts opts = new BeeLineOpts(this, System.getProperties());
  private String lastProgress = null;
  private final Map<SQLWarning, Date> seenWarnings = new HashMap<SQLWarning, Date>();
  private final Commands commands = new Commands(this);
  private OutputFile scriptOutputFile = null;
  private OutputFile recordOutputFile = null;
  private PrintStream outputStream = new PrintStream(System.out, true);
  private PrintStream errorStream = new PrintStream(System.err, true);
  private ConsoleReader consoleReader;
  private List<String> batch = null;
  private final Reflector reflector;

  public static final String BEELINE_DEFAULT_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
  public static final String BEELINE_DEFAULT_JDBC_URL = "jdbc:hive2://";

  private static final String SCRIPT_OUTPUT_PREFIX = ">>>";
  private static final int SCRIPT_OUTPUT_PAD_SIZE = 5;

  private static final int ERRNO_OK = 0;
  private static final int ERRNO_ARGS = 1;
  private static final int ERRNO_OTHER = 2;

  private static final String HIVE_VAR_PREFIX = "--hivevar";

  private final Map<Object, Object> formats = map(new Object[] {
      "vertical", new VerticalOutputFormat(this),
      "table", new TableOutputFormat(this),
      "csv", new SeparatedValuesOutputFormat(this, ','),
      "tsv", new SeparatedValuesOutputFormat(this, '\t'),
      "xmlattr", new XMLAttributeOutputFormat(this),
      "xmlelements", new XMLElementOutputFormat(this),
  });


  final CommandHandler[] commandHandlers = new CommandHandler[] {
      new ReflectiveCommandHandler(this, new String[] {"quit", "done", "exit"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"connect", "open"},
          new Completor[] {new SimpleCompletor(getConnectionURLExamples())}),
      new ReflectiveCommandHandler(this, new String[] {"describe"},
          new Completor[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"indexes"},
          new Completor[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"primarykeys"},
          new Completor[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"exportedkeys"},
          new Completor[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"manual"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"importedkeys"},
          new Completor[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"procedures"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"tables"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"typeinfo"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"columns"},
          new Completor[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"reconnect"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"dropall"},
          new Completor[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"history"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"metadata"},
          new Completor[] {
              new SimpleCompletor(getMetadataMethodNames())}),
      new ReflectiveCommandHandler(this, new String[] {"nativesql"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"dbinfo"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"rehash"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"verbose"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"run"},
          new Completor[] {new FileNameCompletor()}),
      new ReflectiveCommandHandler(this, new String[] {"batch"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"list"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"all"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"go", "#"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"script"},
          new Completor[] {new FileNameCompletor()}),
      new ReflectiveCommandHandler(this, new String[] {"record"},
          new Completor[] {new FileNameCompletor()}),
      new ReflectiveCommandHandler(this, new String[] {"brief"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"close"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"closeall"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"isolation"},
          new Completor[] {new SimpleCompletor(getIsolationLevels())}),
      new ReflectiveCommandHandler(this, new String[] {"outputformat"},
          new Completor[] {new SimpleCompletor(
              formats.keySet().toArray(new String[0]))}),
      new ReflectiveCommandHandler(this, new String[] {"autocommit"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"commit"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"properties"},
          new Completor[] {new FileNameCompletor()}),
      new ReflectiveCommandHandler(this, new String[] {"rollback"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"help", "?"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"set"},
          getOpts().optionCompletors()),
      new ReflectiveCommandHandler(this, new String[] {"save"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"scan"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"sql"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"call"},
          null),
  };


  static final SortedSet<String> KNOWN_DRIVERS = new TreeSet<String>(Arrays.asList(
      new String[] {
          "org.apache.hive.jdbc.HiveDriver",
          "org.apache.hadoop.hive.jdbc.HiveDriver",
      }));


  static {
    try {
      Class.forName("jline.ConsoleReader");
    } catch (Throwable t) {
      throw new ExceptionInInitializerError("jline-missing");
    }
  }


  static Manifest getManifest() throws IOException {
    URL base = BeeLine.class.getResource("/META-INF/MANIFEST.MF");
    URLConnection c = base.openConnection();
    if (c instanceof JarURLConnection) {
      return ((JarURLConnection) c).getManifest();
    }
    return null;
  }


  String getManifestAttribute(String name) {
    try {
      Manifest m = getManifest();
      if (m == null) {
        return "??";
      }

      Attributes attrs = m.getAttributes("beeline");
      if (attrs == null) {
        return "???";
      }

      String val = attrs.getValue(name);
      if (val == null || "".equals(val)) {
        return "????";
      }

      return val;
    } catch (Exception e) {
      e.printStackTrace(errorStream);
      return "?????";
    }
  }


  String getApplicationTitle() {
    Package pack = BeeLine.class.getPackage();

    return loc("app-introduction", new Object[] {
        "Beeline",
        pack.getImplementationVersion() == null ? "???"
            : pack.getImplementationVersion(),
        "Apache Hive",
        // getManifestAttribute ("Specification-Title"),
        // getManifestAttribute ("Implementation-Version"),
        // getManifestAttribute ("Implementation-ReleaseDate"),
        // getManifestAttribute ("Implementation-Vendor"),
        // getManifestAttribute ("Implementation-License"),
    });
  }

  String getApplicationContactInformation() {
    return getManifestAttribute("Implementation-Vendor");
  }

  String loc(String res) {
    return loc(res, new Object[0]);
  }

  String loc(String res, int param) {
    try {
      return MessageFormat.format(
          new ChoiceFormat(resourceBundle.getString(res)).format(param),
          new Object[] {new Integer(param)});
    } catch (Exception e) {
      return res + ": " + param;
    }
  }

  String loc(String res, Object param1) {
    return loc(res, new Object[] {param1});
  }

  String loc(String res, Object param1, Object param2) {
    return loc(res, new Object[] {param1, param2});
  }

  String loc(String res, Object[] params) {
    try {
      return MessageFormat.format(resourceBundle.getString(res), params);
    } catch (Exception e) {
      e.printStackTrace(getErrorStream());
      try {
        return res + ": " + Arrays.asList(params);
      } catch (Exception e2) {
        return res;
      }
    }
  }

  protected String locElapsedTime(long milliseconds) {
    if (getOpts().getShowElapsedTime()) {
      return loc("time-ms", new Object[] {new Double(milliseconds / 1000d)});
    }
    return "";
  }


  /**
   * Starts the program.
   */
  public static void main(String[] args) throws IOException {
    mainWithInputRedirection(args, null);
  }

  /**
   * Starts the program with redirected input. For redirected output,
   * setOutputStream() and setErrorStream can be used.
   * Exits with 0 on success, 1 on invalid arguments, and 2 on any other error
   *
   * @param args
   *          same as main()
   *
   * @param inputStream
   *          redirected input, or null to use standard input
   */
  public static void mainWithInputRedirection(String[] args, InputStream inputStream)
      throws IOException {
    BeeLine beeLine = new BeeLine();
    int status = beeLine.begin(args, inputStream);

    if (!Boolean.getBoolean(BeeLineOpts.PROPERTY_NAME_EXIT)) {
      System.exit(status);
    }
  }


  public BeeLine() {
    beeLineCommandCompletor = new BeeLineCommandCompletor(this);
    reflector = new Reflector(this);

    // attempt to dynamically load signal handler
    /* TODO disable signal handler
    try {
      Class<?> handlerClass =
          Class.forName("org.apache.hive.beeline.SunSignalHandler");
      signalHandler = (BeeLineSignalHandler)
          handlerClass.newInstance();
    } catch (Throwable t) {
      // ignore and leave cancel functionality disabled
    }
    */
  }


  DatabaseConnection getDatabaseConnection() {
    return getDatabaseConnections().current();
  }


  Connection getConnection() throws SQLException {
    if (getDatabaseConnections().current() == null) {
      throw new IllegalArgumentException(loc("no-current-connection"));
    }
    if (getDatabaseConnections().current().getConnection() == null) {
      throw new IllegalArgumentException(loc("no-current-connection"));
    }
    return getDatabaseConnections().current().getConnection();
  }


  DatabaseMetaData getDatabaseMetaData() {
    if (getDatabaseConnections().current() == null) {
      throw new IllegalArgumentException(loc("no-current-connection"));
    }
    if (getDatabaseConnections().current().getDatabaseMetaData() == null) {
      throw new IllegalArgumentException(loc("no-current-connection"));
    }
    return getDatabaseConnections().current().getDatabaseMetaData();
  }


  public String[] getIsolationLevels() {
    return new String[] {
        "TRANSACTION_NONE",
        "TRANSACTION_READ_COMMITTED",
        "TRANSACTION_READ_UNCOMMITTED",
        "TRANSACTION_REPEATABLE_READ",
        "TRANSACTION_SERIALIZABLE",
    };
  }


  public String[] getMetadataMethodNames() {
    try {
      TreeSet<String> mnames = new TreeSet<String>();
      Method[] m = DatabaseMetaData.class.getDeclaredMethods();
      for (int i = 0; m != null && i < m.length; i++) {
        mnames.add(m[i].getName());
      }
      return mnames.toArray(new String[0]);
    } catch (Throwable t) {
      return new String[0];
    }
  }


  public String[] getConnectionURLExamples() {
    return new String[] {
        "jdbc:JSQLConnect://<hostname>/database=<database>",
        "jdbc:cloudscape:<database>;create=true",
        "jdbc:twtds:sqlserver://<hostname>/<database>",
        "jdbc:daffodilDB_embedded:<database>;create=true",
        "jdbc:datadirect:db2://<hostname>:50000;databaseName=<database>",
        "jdbc:inetdae:<hostname>:1433",
        "jdbc:datadirect:oracle://<hostname>:1521;SID=<database>;MaxPooledStatements=0",
        "jdbc:datadirect:sqlserver://<hostname>:1433;SelectMethod=cursor;DatabaseName=<database>",
        "jdbc:datadirect:sybase://<hostname>:5000",
        "jdbc:db2://<hostname>/<database>",
        "jdbc:hive2://<hostname>",
        "jdbc:hsqldb:<database>",
        "jdbc:idb:<database>.properties",
        "jdbc:informix-sqli://<hostname>:1526/<database>:INFORMIXSERVER=<database>",
        "jdbc:interbase://<hostname>//<database>.gdb",
        "jdbc:microsoft:sqlserver://<hostname>:1433;DatabaseName=<database>;SelectMethod=cursor",
        "jdbc:mysql://<hostname>/<database>?autoReconnect=true",
        "jdbc:oracle:thin:@<hostname>:1521:<database>",
        "jdbc:pointbase:<database>,database.home=<database>,create=true",
        "jdbc:postgresql://<hostname>:5432/<database>",
        "jdbc:postgresql:net//<hostname>/<database>",
        "jdbc:sybase:Tds:<hostname>:4100/<database>?ServiceName=<database>",
        "jdbc:weblogic:mssqlserver4:<database>@<hostname>:1433",
        "jdbc:odbc:<database>",
        "jdbc:sequelink://<hostname>:4003/[Oracle]",
        "jdbc:sequelink://<hostname>:4004/[Informix];Database=<database>",
        "jdbc:sequelink://<hostname>:4005/[Sybase];Database=<database>",
        "jdbc:sequelink://<hostname>:4006/[SQLServer];Database=<database>",
        "jdbc:sequelink://<hostname>:4011/[ODBC MS Access];Database=<database>",
        "jdbc:openlink://<hostname>/DSN=SQLServerDB/UID=sa/PWD=",
        "jdbc:solid://<hostname>:<port>/<UID>/<PWD>",
        "jdbc:dbaw://<hostname>:8889/<database>",
    };
  }

  /**
   * Entry point to creating a {@link ColorBuffer} with color
   * enabled or disabled depending on the value of {@link BeeLineOpts#getColor}.
   */
  ColorBuffer getColorBuffer() {
    return new ColorBuffer(getOpts().getColor());
  }


  /**
   * Entry point to creating a {@link ColorBuffer} with color
   * enabled or disabled depending on the value of {@link BeeLineOpts#getColor}.
   */
  ColorBuffer getColorBuffer(String msg) {
    return new ColorBuffer(msg, getOpts().getColor());
  }


  boolean initArgs(String[] args) {
    List<String> commands = new LinkedList<String>();
    List<String> files = new LinkedList<String>();
    String driver = null, user = null, pass = null, url = null, cmd = null;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--help") || args[i].equals("-h")) {
        // Return false here, so usage will be printed.
        return false;
      }

      // Parse hive variables
      if (args[i].equals(HIVE_VAR_PREFIX)) {
        String[] parts = split(args[++i], "=");
        if (parts.length != 2) {
          return false;
        }
        getOpts().getHiveVariables().put(parts[0], parts[1]);
        continue;
      }

      // -- arguments are treated as properties
      if (args[i].startsWith("--")) {
        String[] parts = split(args[i].substring(2), "=");
        debug(loc("setting-prop", Arrays.asList(parts)));
        if (parts.length > 0) {
          boolean ret;

          if (parts.length >= 2) {
            ret = getOpts().set(parts[0], parts[1], true);
          } else {
            ret = getOpts().set(parts[0], "true", true);
          }

          if (!ret) {
            return false;
          }

        }
        continue;
      }

      if (args[i].equals("-d")) {
        driver = args[i++ + 1];
      } else if (args[i].equals("-n")) {
        user = args[i++ + 1];
      } else if (args[i].equals("-p")) {
        pass = args[i++ + 1];
      } else if (args[i].equals("-u")) {
        url = args[i++ + 1];
      } else if (args[i].equals("-e")) {
        commands.add(args[i++ + 1]);
      } else if (args[i].equals("-f")) {
        getOpts().setScriptFile(args[i++ + 1]);
      } else {
        files.add(args[i]);
      }
    }

    // TODO: temporary disable this for easier debugging
    /*
    if (url == null) {
      url = BEELINE_DEFAULT_JDBC_URL;
    }
    if (driver == null) {
      driver = BEELINE_DEFAULT_JDBC_DRIVER;
    }
    */

    if (url != null) {
      String com = "!connect "
          + url + " "
          + (user == null || user.length() == 0 ? "''" : user) + " "
          + (pass == null || pass.length() == 0 ? "''" : pass) + " "
          + (driver == null ? "" : driver);
      debug("issuing: " + com);
      dispatch(com);
    }

    // now load properties files
    for (Iterator<String> i = files.iterator(); i.hasNext();) {
      dispatch("!properties " + i.next());
    }


    if (commands.size() > 0) {
      // for single command execute, disable color
      getOpts().setColor(false);
      getOpts().setHeaderInterval(-1);

      for (Iterator<String> i = commands.iterator(); i.hasNext();) {
        String command = i.next().toString();
        debug(loc("executing-command", command));
        dispatch(command);
      }
      exit = true; // execute and exit
    }
    return true;
  }


  /**
   * Start accepting input from stdin, and dispatch it
   * to the appropriate {@link CommandHandler} until the
   * global variable <code>exit</code> is true.
   */
  public int begin(String[] args, InputStream inputStream) throws IOException {
    int status = ERRNO_OK;
    try {
      // load the options first, so we can override on the command line
      getOpts().load();
    } catch (Exception e) {
      // nothing
    }

    if (!(initArgs(args))) {
      usage();
      return ERRNO_ARGS;
    }

    ConsoleReader reader = null;
    boolean runningScript = (getOpts().getScriptFile() != null);
    if (runningScript) {
      try {
        FileInputStream scriptStream = new FileInputStream(getOpts().getScriptFile());
        reader = getConsoleReader(scriptStream);
      } catch (Throwable t) {
        handleException(t);
        commands.quit(null);
        status = ERRNO_OTHER;
      }
    } else {
      reader = getConsoleReader(inputStream);
    }

    try {
      info(getApplicationTitle());
    } catch (Exception e) {
      // ignore
    }

    while (!exit) {
      try {
        // Execute one instruction; terminate on executing a script if there is an error
        if (!dispatch(reader.readLine(getPrompt())) && runningScript) {
          commands.quit(null);
          status = ERRNO_OTHER;
        }
      } catch (EOFException eof) {
        // CTRL-D
        commands.quit(null);
      } catch (Throwable t) {
        handleException(t);
        status = ERRNO_OTHER;
      }
    }
    // ### NOTE jvs 10-Aug-2004: Clean up any outstanding
    // connections automatically.
    commands.closeall(null);
    return status;
  }

  public void close() {
    commands.quit(null);
    commands.closeall(null);
  }

  public ConsoleReader getConsoleReader(InputStream inputStream) throws IOException {
    if (inputStream != null) {
      // ### NOTE: fix for sf.net bug 879425.
      consoleReader = new ConsoleReader(inputStream, new PrintWriter(getOutputStream(), true));
    } else {
      consoleReader = new ConsoleReader();
    }

    // setup history
    ByteArrayInputStream historyBuffer = null;

    if (new File(getOpts().getHistoryFile()).isFile()) {
      try {
        // save the current contents of the history buffer. This gets
        // around a bug in JLine where setting the output before the
        // input will clobber the history input, but setting the
        // input before the output will cause the previous commands
        // to not be saved to the buffer.
        FileInputStream historyIn = new FileInputStream(getOpts().getHistoryFile());
        ByteArrayOutputStream hist = new ByteArrayOutputStream();
        int n;
        while ((n = historyIn.read()) != -1) {
          hist.write(n);
        }
        historyIn.close();
        historyBuffer = new ByteArrayInputStream(hist.toByteArray());
      } catch (Exception e) {
        handleException(e);
      }
    }

    try {
      // now set the output for the history
      PrintWriter historyOut = new PrintWriter(new FileWriter(getOpts().getHistoryFile()), true);
      consoleReader.getHistory().setOutput(historyOut);
    } catch (Exception e) {
      handleException(e);
    }

    try {
      // now load in the previous history
      if (historyBuffer != null) {
        consoleReader.getHistory().load(historyBuffer);
      }
    } catch (Exception e) {
      handleException(e);
    }
    consoleReader.addCompletor(new BeeLineCompletor(this));
    return consoleReader;
  }


  void usage() {
    output(loc("cmd-usage"));
  }


  /**
   * Dispatch the specified line to the appropriate {@link CommandHandler}.
   *
   * @param line
   *          the commmand-line to dispatch
   * @return true if the command was "successful"
   */
  boolean dispatch(String line) {
    if (line == null) {
      // exit
      exit = true;
      return true;
    }

    if (line.trim().length() == 0) {
      return true;
    }

    if (isComment(line)) {
      return true;
    }

    line = line.trim();

    // save it to the current script, if any
    if (scriptOutputFile != null) {
      scriptOutputFile.addLine(line);
    }

    if (isHelpRequest(line)) {
      line = "!help";
    }

    if (line.startsWith(COMMAND_PREFIX)) {
      Map<String, CommandHandler> cmdMap = new TreeMap<String, CommandHandler>();
      line = line.substring(1);
      for (int i = 0; i < commandHandlers.length; i++) {
        String match = commandHandlers[i].matches(line);
        if (match != null) {
          cmdMap.put(match, commandHandlers[i]);
        }
      }

      if (cmdMap.size() == 0) {
        return error(loc("unknown-command", line));
      } else if (cmdMap.size() > 1) {
        return error(loc("multiple-matches",
            cmdMap.keySet().toString()));
      } else {
        return cmdMap.values().iterator().next()
            .execute(line);
      }
    } else {
      return commands.sql(line);
    }
  }

  /**
   * Test whether a line requires a continuation.
   *
   * @param line
   *          the line to be tested
   *
   * @return true if continuation required
   */
  boolean needsContinuation(String line) {
    if (isHelpRequest(line)) {
      return false;
    }

    if (line.startsWith(COMMAND_PREFIX)) {
      return false;
    }

    if (isComment(line)) {
      return false;
    }

    String trimmed = line.trim();

    if (trimmed.length() == 0) {
      return false;
    }

    if (!getOpts().isAllowMultiLineCommand()) {
      return false;
    }

    return !trimmed.endsWith(";");
  }

  /**
   * Test whether a line is a help request other than !help.
   *
   * @param line
   *          the line to be tested
   *
   * @return true if a help request
   */
  boolean isHelpRequest(String line) {
    return line.equals("?") || line.equalsIgnoreCase("help");
  }

  /**
   * Test whether a line is a comment.
   *
   * @param line
   *          the line to be tested
   *
   * @return true if a comment
   */
  boolean isComment(String line) {
    // SQL92 comment prefix is "--"
    // beeline also supports shell-style "#" prefix
    return line.startsWith("#") || line.startsWith("--");
  }

  /**
   * Print the specified message to the console
   *
   * @param msg
   *          the message to print
   */
  void output(String msg) {
    output(msg, true);
  }


  void info(String msg) {
    if (!(getOpts().isSilent())) {
      output(msg, true, getErrorStream());
    }
  }


  void info(ColorBuffer msg) {
    if (!(getOpts().isSilent())) {
      output(msg, true, getErrorStream());
    }
  }


  /**
   * Issue the specified error message
   *
   * @param msg
   *          the message to issue
   * @return false always
   */
  boolean error(String msg) {
    output(getColorBuffer().red(msg), true, getErrorStream());
    return false;
  }


  boolean error(Throwable t) {
    handleException(t);
    return false;
  }


  void debug(String msg) {
    if (getOpts().getVerbose()) {
      output(getColorBuffer().blue(msg), true, getErrorStream());
    }
  }


  void output(ColorBuffer msg) {
    output(msg, true);
  }


  void output(String msg, boolean newline, PrintStream out) {
    output(getColorBuffer(msg), newline, out);
  }


  void output(ColorBuffer msg, boolean newline) {
    output(msg, newline, getOutputStream());
  }


  void output(ColorBuffer msg, boolean newline, PrintStream out) {
    if (newline) {
      out.println(msg.getColor());
    } else {
      out.print(msg.getColor());
    }

    if (recordOutputFile == null) {
      return;
    }

    // only write to the record file if we are writing a line ...
    // otherwise we might get garbage from backspaces and such.
    if (newline) {
      recordOutputFile.addLine(msg.getMono()); // always just write mono
    } else {
      recordOutputFile.print(msg.getMono());
    }
  }


  /**
   * Print the specified message to the console
   *
   * @param msg
   *          the message to print
   * @param newline
   *          if false, do not append a newline
   */
  void output(String msg, boolean newline) {
    output(getColorBuffer(msg), newline);
  }


  void autocommitStatus(Connection c) throws SQLException {
    info(loc("autocommit-status", c.getAutoCommit() + ""));
  }


  /**
   * Ensure that autocommit is on for the current connection
   *
   * @return true if autocommit is set
   */
  boolean assertAutoCommit() {
    if (!(assertConnection())) {
      return false;
    }
    try {
      if (getDatabaseConnection().getConnection().getAutoCommit()) {
        return error(loc("autocommit-needs-off"));
      }
    } catch (Exception e) {
      return error(e);
    }
    return true;
  }


  /**
   * Assert that we have an active, living connection. Print
   * an error message if we do not.
   *
   * @return true if there is a current, active connection
   */
  boolean assertConnection() {
    try {
      if (getDatabaseConnection() == null || getDatabaseConnection().getConnection() == null) {
        return error(loc("no-current-connection"));
      }
      if (getDatabaseConnection().getConnection().isClosed()) {
        return error(loc("connection-is-closed"));
      }
    } catch (SQLException sqle) {
      return error(loc("no-current-connection"));
    }
    return true;
  }


  /**
   * Print out any warnings that exist for the current connection.
   */
  void showWarnings() {
    try {
      if (getDatabaseConnection().getConnection() == null
          || !getOpts().getVerbose()) {
        return;
      }
      showWarnings(getDatabaseConnection().getConnection().getWarnings());
    } catch (Exception e) {
      handleException(e);
    }
  }


  /**
   * Print the specified warning on the console, as well as
   * any warnings that are returned from {@link SQLWarning#getNextWarning}.
   *
   * @param warn
   *          the {@link SQLWarning} to print
   */
  void showWarnings(SQLWarning warn) {
    if (warn == null) {
      return;
    }

    if (seenWarnings.get(warn) == null) {
      // don't re-display warnings we have already seen
      seenWarnings.put(warn, new java.util.Date());
      handleSQLException(warn);
    }

    SQLWarning next = warn.getNextWarning();
    if (next != warn) {
      showWarnings(next);
    }
  }


  String getPrompt() {
    if (getDatabaseConnection() == null || getDatabaseConnection().getUrl() == null) {
      return "beeline> ";
    } else {
      return getPrompt(getDatabaseConnections().getIndex()
          + ": " + getDatabaseConnection().getUrl()) + "> ";
    }
  }


  static String getPrompt(String url) {
    if (url == null || url.length() == 0) {
      url = "beeline";
    }
    if (url.indexOf(";") > -1) {
      url = url.substring(0, url.indexOf(";"));
    }
    if (url.indexOf("?") > -1) {
      url = url.substring(0, url.indexOf("?"));
    }
    if (url.length() > 45) {
      url = url.substring(0, 45);
    }
    return url;
  }


  /**
   * Try to obtain the current size of the specified {@link ResultSet} by jumping to the last row
   * and getting the row number.
   *
   * @param rs
   *          the {@link ResultSet} to get the size for
   * @return the size, or -1 if it could not be obtained
   */
  int getSize(ResultSet rs) {
    try {
      if (rs.getType() == rs.TYPE_FORWARD_ONLY) {
        return -1;
      }
      rs.last();
      int total = rs.getRow();
      rs.beforeFirst();
      return total;
    } catch (SQLException sqle) {
      return -1;
    }
    // JDBC 1 driver error
    catch (AbstractMethodError ame) {
      return -1;
    }
  }


  ResultSet getColumns(String table) throws SQLException {
    if (!(assertConnection())) {
      return null;
    }
    return getDatabaseConnection().getDatabaseMetaData().getColumns(
        getDatabaseConnection().getDatabaseMetaData().getConnection().getCatalog(), null, table, "%");
  }


  ResultSet getTables() throws SQLException {
    if (!(assertConnection())) {
      return null;
    }
    return getDatabaseConnection().getDatabaseMetaData().getTables(
        getDatabaseConnection().getDatabaseMetaData().getConnection().getCatalog(), null, "%",
        new String[] {"TABLE"});
  }


  String[] getColumnNames(DatabaseMetaData meta) throws SQLException {
    Set<String> names = new HashSet<String>();
    info(loc("building-tables"));
    try {
      ResultSet columns = getColumns("%");
      try {
        int total = getSize(columns);
        int index = 0;

        while (columns.next()) {
          // add the following strings:
          // 1. column name
          // 2. table name
          // 3. tablename.columnname

          progress(index++, total);
          String name = columns.getString("TABLE_NAME");
          names.add(name);
          names.add(columns.getString("COLUMN_NAME"));
          names.add(columns.getString("TABLE_NAME") + "."
              + columns.getString("COLUMN_NAME"));
        }
        progress(index, index);
      } finally {
        columns.close();
      }
      info(loc("done"));
      return names.toArray(new String[0]);
    } catch (Throwable t) {
      handleException(t);
      return new String[0];
    }
  }


  // //////////////////
  // String utilities
  // //////////////////


  /**
   * Split the line into an array by tokenizing on space characters
   *
   * @param line
   *          the line to break up
   * @return an array of individual words
   */
  String[] split(String line) {
    return split(line, " ");
  }


  String dequote(String str) {
    if (str == null) {
      return null;
    }
    while ((str.startsWith("'") && str.endsWith("'"))
        || (str.startsWith("\"") && str.endsWith("\""))) {
      str = str.substring(1, str.length() - 1);
    }
    return str;
  }


  String[] split(String line, String delim) {
    StringTokenizer tok = new StringTokenizer(line, delim);
    String[] ret = new String[tok.countTokens()];
    int index = 0;
    while (tok.hasMoreTokens()) {
      String t = tok.nextToken();
      t = dequote(t);
      ret[index++] = t;
    }
    return ret;
  }


  static Map<Object, Object> map(Object[] obs) {
    Map<Object, Object> m = new HashMap<Object, Object>();
    for (int i = 0; i < obs.length - 1; i += 2) {
      m.put(obs[i], obs[i + 1]);
    }
    return Collections.unmodifiableMap(m);
  }


  static boolean getMoreResults(Statement stmnt) {
    try {
      return stmnt.getMoreResults();
    } catch (Throwable t) {
      return false;
    }
  }


  static String xmlattrencode(String str) {
    str = replace(str, "\"", "&quot;");
    str = replace(str, "<", "&lt;");
    return str;
  }


  static String replace(String source, String from, String to) {
    if (source == null) {
      return null;
    }

    if (from.equals(to)) {
      return source;
    }

    StringBuilder replaced = new StringBuilder();

    int index = -1;
    while ((index = source.indexOf(from)) != -1) {
      replaced.append(source.substring(0, index));
      replaced.append(to);
      source = source.substring(index + from.length());
    }
    replaced.append(source);

    return replaced.toString();
  }


  /**
   * Split the line based on spaces, asserting that the
   * number of words is correct.
   *
   * @param line
   *          the line to split
   * @param assertLen
   *          the number of words to assure
   * @param usage
   *          the message to output if there are an incorrect
   *          number of words.
   * @return the split lines, or null if the assertion failed.
   */
  String[] split(String line, int assertLen, String usage) {
    String[] ret = split(line);

    if (ret.length != assertLen) {
      error(usage);
      return null;
    }

    return ret;
  }


  /**
   * Wrap the specified string by breaking on space characters.
   *
   * @param toWrap
   *          the string to wrap
   * @param len
   *          the maximum length of any line
   * @param start
   *          the number of spaces to pad at the
   *          beginning of a line
   * @return the wrapped string
   */
  String wrap(String toWrap, int len, int start) {
    StringBuilder buff = new StringBuilder();
    StringBuilder line = new StringBuilder();

    char[] head = new char[start];
    Arrays.fill(head, ' ');

    for (StringTokenizer tok = new StringTokenizer(toWrap, " "); tok.hasMoreTokens();) {
      String next = tok.nextToken();
      if (line.length() + next.length() > len) {
        buff.append(line).append(separator).append(head);
        line.setLength(0);
      }

      line.append(line.length() == 0 ? "" : " ").append(next);
    }

    buff.append(line);
    return buff.toString();
  }


  /**
   * Output a progress indicator to the console.
   *
   * @param cur
   *          the current progress
   * @param max
   *          the maximum progress, or -1 if unknown
   */
  void progress(int cur, int max) {
    StringBuilder out = new StringBuilder();

    if (lastProgress != null) {
      char[] back = new char[lastProgress.length()];
      Arrays.fill(back, '\b');
      out.append(back);
    }

    String progress = cur + "/"
    + (max == -1 ? "?" : "" + max) + " "
    + (max == -1 ? "(??%)"
        : ("(" + (cur * 100 / (max == 0 ? 1 : max)) + "%)"));

    if (cur >= max && max != -1) {
      progress += " " + loc("done") + separator;
      lastProgress = null;
    } else {
      lastProgress = progress;
    }

    out.append(progress);

    outputStream.print(out.toString());
    outputStream.flush();
  }

  // /////////////////////////////
  // Exception handling routines
  // /////////////////////////////

  void handleException(Throwable e) {
    while (e instanceof InvocationTargetException) {
      e = ((InvocationTargetException) e).getTargetException();
    }

    if (e instanceof SQLException) {
      handleSQLException((SQLException) e);
    } else if (!(getOpts().getVerbose())) {
      if (e.getMessage() == null) {
        error(e.getClass().getName());
      } else {
        error(e.getMessage());
      }
    } else {
      e.printStackTrace(getErrorStream());
    }
  }


  void handleSQLException(SQLException e) {
    if (e instanceof SQLWarning && !(getOpts().getShowWarnings())) {
      return;
    }

    error(loc(e instanceof SQLWarning ? "Warning" : "Error",
        new Object[] {
            e.getMessage() == null ? "" : e.getMessage().trim(),
            e.getSQLState() == null ? "" : e.getSQLState().trim(),
            new Integer(e.getErrorCode())}));

    if (getOpts().getVerbose()) {
      e.printStackTrace(getErrorStream());
    }

    if (!getOpts().getShowNestedErrs()) {
      return;
    }

    for (SQLException nested = e.getNextException(); nested != null && nested != e; nested = nested
        .getNextException()) {
      handleSQLException(nested);
    }
  }


  boolean scanForDriver(String url) {
    try {
      // already registered
      if (findRegisteredDriver(url) != null) {
        return true;
      }

      // first try known drivers...
      scanDrivers(true);

      if (findRegisteredDriver(url) != null) {
        return true;
      }

      // now really scan...
      scanDrivers(false);

      if (findRegisteredDriver(url) != null) {
        return true;
      }

      return false;
    } catch (Exception e) {
      debug(e.toString());
      return false;
    }
  }


  private Driver findRegisteredDriver(String url) {
    for (Enumeration drivers = DriverManager.getDrivers(); drivers != null
        && drivers.hasMoreElements();) {
      Driver driver = (Driver) drivers.nextElement();
      try {
        if (driver.acceptsURL(url)) {
          return driver;
        }
      } catch (Exception e) {
      }
    }
    return null;
  }


  Driver[] scanDrivers(String line) throws IOException {
    return scanDrivers(false);
  }


  Driver[] scanDrivers(boolean knownOnly) throws IOException {
    long start = System.currentTimeMillis();

    Set<String> classNames = new HashSet<String>();

    if (!knownOnly) {
      classNames.addAll(Arrays.asList(
          ClassNameCompletor.getClassNames()));
    }

    classNames.addAll(KNOWN_DRIVERS);

    Set driverClasses = new HashSet();

    for (Iterator<String> i = classNames.iterator(); i.hasNext();) {
      String className = i.next().toString();

      if (className.toLowerCase().indexOf("driver") == -1) {
        continue;
      }

      try {
        Class c = Class.forName(className, false,
            Thread.currentThread().getContextClassLoader());
        if (!Driver.class.isAssignableFrom(c)) {
          continue;
        }

        if (Modifier.isAbstract(c.getModifiers())) {
          continue;
        }

        // now instantiate and initialize it
        driverClasses.add(c.newInstance());
      } catch (Throwable t) {
      }
    }
    info("scan complete in "
        + (System.currentTimeMillis() - start) + "ms");
    return (Driver[]) driverClasses.toArray(new Driver[0]);
  }


  private Driver[] scanDriversOLD(String line) {
    long start = System.currentTimeMillis();

    Set<String> paths = new HashSet<String>();
    Set driverClasses = new HashSet();

    for (StringTokenizer tok = new StringTokenizer(
        System.getProperty("java.ext.dirs"),
        System.getProperty("path.separator")); tok.hasMoreTokens();) {
      File[] files = new File(tok.nextToken()).listFiles();
      for (int i = 0; files != null && i < files.length; i++) {
        paths.add(files[i].getAbsolutePath());
      }
    }

    for (StringTokenizer tok = new StringTokenizer(
        System.getProperty("java.class.path"),
        System.getProperty("path.separator")); tok.hasMoreTokens();) {
      paths.add(new File(tok.nextToken()).getAbsolutePath());
    }

    for (Iterator<String> i = paths.iterator(); i.hasNext();) {
      File f = new File(i.next());
      output(getColorBuffer().pad(loc("scanning", f.getAbsolutePath()), 60),
          false);

      try {
        ZipFile zf = new ZipFile(f);
        int total = zf.size();
        int index = 0;

        for (Enumeration zfEnum = zf.entries(); zfEnum.hasMoreElements();) {
          ZipEntry entry = (ZipEntry) zfEnum.nextElement();
          String name = entry.getName();
          progress(index++, total);

          if (name.endsWith(".class")) {
            name = name.replace('/', '.');
            name = name.substring(0, name.length() - 6);

            try {
              // check for the string "driver" in the class
              // to see if we should load it. Not perfect, but
              // it is far too slow otherwise.
              if (name.toLowerCase().indexOf("driver") != -1) {
                Class c = Class.forName(name, false,
                    getClass().getClassLoader());
                if (Driver.class.isAssignableFrom(c)
                    && !(Modifier.isAbstract(
                        c.getModifiers()))) {
                  try {
                    // load and initialize
                    Class.forName(name);
                  } catch (Exception e) {
                  }
                  driverClasses.add(c.newInstance());
                }
              }
            } catch (Throwable t) {
            }
          }
        }
        progress(total, total);
      } catch (Exception e) {
      }
    }

    info("scan complete in "
        + (System.currentTimeMillis() - start) + "ms");
    return (Driver[]) driverClasses.toArray(new Driver[0]);
  }


  // /////////////////////////////////////
  // ResultSet output formatting classes
  // /////////////////////////////////////



  int print(ResultSet rs) throws SQLException {
    String format = getOpts().getOutputFormat();
    OutputFormat f = (OutputFormat) formats.get(format);

    if (f == null) {
      error(loc("unknown-format", new Object[] {
          format, formats.keySet()}));
      f = new TableOutputFormat(this);
    }

    Rows rows;

    if (getOpts().getIncremental()) {
      rows = new IncrementalRows(this, rs);
    } else {
      rows = new BufferedRows(this, rs);
    }
    return f.print(rows);
  }


  Statement createStatement() throws SQLException {
    Statement stmnt = getDatabaseConnection().getConnection().createStatement();
    if (getOpts().timeout > -1) {
      stmnt.setQueryTimeout(getOpts().timeout);
    }
    if (signalHandler != null) {
      signalHandler.setStatement(stmnt);
    }
    return stmnt;
  }


  void runBatch(List<String> statements) {
    try {
      Statement stmnt = createStatement();
      try {
        for (Iterator<String> i = statements.iterator(); i.hasNext();) {
          stmnt.addBatch(i.next().toString());
        }
        int[] counts = stmnt.executeBatch();

        output(getColorBuffer().pad(getColorBuffer().bold("COUNT"), 8)
            .append(getColorBuffer().bold("STATEMENT")));

        for (int i = 0; counts != null && i < counts.length; i++) {
          output(getColorBuffer().pad(counts[i] + "", 8)
              .append(statements.get(i).toString()));
        }
      } finally {
        try {
          stmnt.close();
        } catch (Exception e) {
        }
      }
    } catch (Exception e) {
      handleException(e);
    }
  }

  public int runCommands(String[] cmds) {
    return runCommands(Arrays.asList(cmds));
  }

  public int runCommands(List<String> cmds) {
    int successCount = 0;
    try {
      // TODO: Make script output prefixing configurable. Had to disable this since
      // it results in lots of test diffs.
      for (String cmd : cmds) {
        info(getColorBuffer().pad(SCRIPT_OUTPUT_PREFIX, SCRIPT_OUTPUT_PAD_SIZE).append(cmd));
        // if we do not force script execution, abort
        // when a failure occurs.
        if (dispatch(cmd) || getOpts().getForce()) {
          ++successCount;
        } else {
          error(loc("abort-on-error", cmd));
          return successCount;
        }
      }
    } catch (Exception e) {
      handleException(e);
    }
    return successCount;
  }

  // ////////////////////////
  // Command methods follow
  // ////////////////////////

  void setCompletions() throws SQLException, IOException {
    if (getDatabaseConnection() != null) {
      getDatabaseConnection().setCompletions(getOpts().getFastConnect());
    }
  }

  public BeeLineOpts getOpts() {
    return opts;
  }

  DatabaseConnections getDatabaseConnections() {
    return connections;
  }

  Completor getCommandCompletor() {
    return beeLineCommandCompletor;
  }

  public boolean isExit() {
    return exit;
  }

  public void setExit(boolean exit) {
    this.exit = exit;
  }

  Collection<Driver> getDrivers() {
    return drivers;
  }

  void setDrivers(Collection<Driver> drivers) {
    this.drivers = drivers;
  }

  public static String getSeparator() {
    return separator;
  }

  Commands getCommands() {
    return commands;
  }

  OutputFile getScriptOutputFile() {
    return scriptOutputFile;
  }

  void setScriptOutputFile(OutputFile script) {
    this.scriptOutputFile = script;
  }

  OutputFile getRecordOutputFile() {
    return recordOutputFile;
  }

  void setRecordOutputFile(OutputFile record) {
    this.recordOutputFile = record;
  }

  public void setOutputStream(PrintStream outputStream) {
    this.outputStream = new PrintStream(outputStream, true);
  }

  PrintStream getOutputStream() {
    return outputStream;
  }

  public void setErrorStream(PrintStream errorStream) {
    this.errorStream = new PrintStream(errorStream, true);
  }

  PrintStream getErrorStream() {
    return errorStream;
  }

  ConsoleReader getConsoleReader() {
    return consoleReader;
  }

  void setConsoleReader(ConsoleReader reader) {
    this.consoleReader = reader;
  }

  List<String> getBatch() {
    return batch;
  }

  void setBatch(List<String> batch) {
    this.batch = batch;
  }

  protected Reflector getReflector() {
    return reflector;
  }
}
