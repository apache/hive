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
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.SequenceInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
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

import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import jline.console.completer.FileNameCompleter;
import jline.console.ConsoleReader;
import jline.console.history.FileHistory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.IOUtils;
import org.apache.hive.beeline.cli.CliOptionsProcessor;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.hive.beeline.hs2connection.BeelineHS2ConnectionFileParseException;
import org.apache.hive.beeline.hs2connection.HS2ConnectionFileUtils;
import org.apache.hive.beeline.hs2connection.UserHS2ConnectionFileParser;
import org.apache.hive.beeline.hs2connection.HS2ConnectionFileParser;
import org.apache.hive.beeline.hs2connection.HiveSiteHS2ConnectionFileParser;
import org.apache.thrift.transport.TTransportException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hive.jdbc.Utils;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;

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
@SuppressWarnings("static-access")
public class BeeLine implements Closeable {
  private static final ResourceBundle resourceBundle =
      ResourceBundle.getBundle(BeeLine.class.getSimpleName());
  private final BeeLineSignalHandler signalHandler;
  private final Runnable shutdownHook;
  private static final String separator = System.getProperty("line.separator");
  private boolean exit = false;
  private final DatabaseConnections connections = new DatabaseConnections();
  public static final String COMMAND_PREFIX = "!";
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
  private final Reflector reflector = new Reflector(this);
  private String dbName = null;
  private String currentDatabase = null;

  private FileHistory history;
  // Indicates if this instance of beeline is running in compatibility mode, or beeline mode
  private boolean isBeeLine = true;

  private static final Options options = new Options();

  public static final String BEELINE_DEFAULT_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
  public static final String DEFAULT_DATABASE_NAME = "default";

  private static final String SCRIPT_OUTPUT_PREFIX = ">>>";
  private static final int SCRIPT_OUTPUT_PAD_SIZE = 5;

  private static final int ERRNO_OK = 0;
  private static final int ERRNO_ARGS = 1;
  private static final int ERRNO_OTHER = 2;

  private static final String HIVE_VAR_PREFIX = "--hivevar";
  private static final String HIVE_CONF_PREFIX = "--hiveconf";
  private static final String PROP_FILE_PREFIX = "--property-file";
  static final String PASSWD_MASK = "[passwd stripped]";

  private final Map<Object, Object> formats = map(new Object[] {
      "vertical", new VerticalOutputFormat(this),
      "table", new TableOutputFormat(this),
      "csv2", new SeparatedValuesOutputFormat(this, ','),
      "tsv2", new SeparatedValuesOutputFormat(this, '\t'),
      "dsv", new SeparatedValuesOutputFormat(this, BeeLineOpts.DEFAULT_DELIMITER_FOR_DSV),
      "csv", new DeprecatedSeparatedValuesOutputFormat(this, ','),
      "tsv", new DeprecatedSeparatedValuesOutputFormat(this, '\t'),
      "xmlattr", new XMLAttributeOutputFormat(this),
      "xmlelements", new XMLElementOutputFormat(this),
  });

  private List<String> supportedLocalDriver =
    new ArrayList<String>(Arrays.asList("com.mysql.jdbc.Driver", "org.postgresql.Driver"));

  final CommandHandler[] commandHandlers = new CommandHandler[] {
      new ReflectiveCommandHandler(this, new String[] {"quit", "done", "exit"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"connect", "open"},
          new Completer[] {new StringsCompleter(getConnectionURLExamples())}),
      new ReflectiveCommandHandler(this, new String[] {"describe"},
          new Completer[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"indexes"},
          new Completer[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"primarykeys"},
          new Completer[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"exportedkeys"},
          new Completer[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"manual"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"importedkeys"},
          new Completer[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"procedures"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"tables"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"typeinfo"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"columns"},
          new Completer[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"reconnect"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"dropall"},
          new Completer[] {new TableNameCompletor(this)}),
      new ReflectiveCommandHandler(this, new String[] {"history"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"metadata"},
          new Completer[] {
              new StringsCompleter(getMetadataMethodNames())}),
      new ReflectiveCommandHandler(this, new String[] {"nativesql"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"dbinfo"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"rehash"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"verbose"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"run"},
          new Completer[] {new FileNameCompleter()}),
      new ReflectiveCommandHandler(this, new String[] {"batch"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"list"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"all"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"go", "#"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"script"},
          new Completer[] {new FileNameCompleter()}),
      new ReflectiveCommandHandler(this, new String[] {"record"},
          new Completer[] {new FileNameCompleter()}),
      new ReflectiveCommandHandler(this, new String[] {"brief"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"close"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"closeall"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"isolation"},
          new Completer[] {new StringsCompleter(getIsolationLevels())}),
      new ReflectiveCommandHandler(this, new String[] {"outputformat"},
          new Completer[] {new StringsCompleter(
              formats.keySet().toArray(new String[0]))}),
      new ReflectiveCommandHandler(this, new String[] {"autocommit"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"commit"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"properties"},
          new Completer[] {new FileNameCompleter()}),
      new ReflectiveCommandHandler(this, new String[] {"rollback"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"help", "?"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"set"},
          getOpts().optionCompleters()),
      new ReflectiveCommandHandler(this, new String[] {"save"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"scan"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"sql"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"sh"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"call"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"nullemptystring"},
          new Completer[] {new BooleanCompleter()}),
      new ReflectiveCommandHandler(this, new String[]{"addlocaldriverjar"},
          null),
      new ReflectiveCommandHandler(this, new String[]{"addlocaldrivername"},
          null)
  };

  private final Completer beeLineCommandCompleter = new BeeLineCommandCompleter(Arrays.asList(commandHandlers));

  static final SortedSet<String> KNOWN_DRIVERS = new TreeSet<String>(Arrays.asList(
      new String[] {
          "org.apache.hive.jdbc.HiveDriver",
          "org.apache.hadoop.hive.jdbc.HiveDriver",
      }));

  static {
    try {
      Class.forName("jline.console.ConsoleReader");
    } catch (Throwable t) {
      throw new ExceptionInInitializerError("jline-missing");
    }
  }

  static {
    // -d <driver class>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("driver class")
        .withDescription("the driver class to use")
        .create('d'));

    // -u <database url>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("database url")
        .withDescription("the JDBC URL to connect to")
        .create('u'));

    // -r
    options.addOption(OptionBuilder
        .withLongOpt("reconnect")
        .withDescription("Reconnect to last saved connect url (in conjunction with !save)")
        .create('r'));

    // -n <username>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("username")
        .withDescription("the username to connect as")
        .create('n'));

    // -p <password>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("password")
        .withDescription("the password to connect as")
        .hasOptionalArg()
        .create('p'));

    // -w (or) --password-file <file>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("password-file")
        .withDescription("the password file to read password from")
        .withLongOpt("password-file")
        .create('w'));

    // -a <authType>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("authType")
        .withDescription("the authentication type")
        .create('a'));

    // -i <init file>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("init")
        .withDescription("script file for initialization")
        .create('i'));

    // -e <query>
    options.addOption(OptionBuilder
        .hasArgs()
        .withArgName("query")
        .withDescription("query that should be executed")
        .create('e'));

    // -f <script file>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("file")
        .withDescription("script file that should be executed")
        .create('f'));

    // -help
    options.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("display this message")
        .create('h'));

    // Substitution option --hivevar
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("key=value")
        .withLongOpt("hivevar")
        .withDescription("hive variable name and value")
        .create());

    //hive conf option --hiveconf
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("property=value")
        .withLongOpt("hiveconf")
        .withDescription("Use value for given property")
        .create());

    // --property-file <file>
    options.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("property-file")
        .withDescription("the file to read configuration properties from")
        .create());
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

    return loc("app-introduction", new Object[] { "Beeline",
        pack.getImplementationVersion() == null ? "???" : pack.getImplementationVersion(),
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
    try {
      int status = beeLine.begin(args, inputStream);

      if (!Boolean.getBoolean(BeeLineOpts.PROPERTY_NAME_EXIT)) {
          System.exit(status);
      }
    } finally {
      beeLine.close();
    }
  }

  public BeeLine() {
    this(true);
  }

  public BeeLine(boolean isBeeLine) {
    this.isBeeLine = isBeeLine;
    this.signalHandler = new SunSignalHandler(this);
    this.shutdownHook = new Runnable() {
      @Override
      public void run() {
        try {
          if (history != null) {
            history.setMaxSize(getOpts().getMaxHistoryRows());
            history.flush();
          }
        } catch (IOException e) {
          error(e);
        } finally {
          close();
        }
      }
    };
  }

  DatabaseConnection getDatabaseConnection() {
    return getDatabaseConnections().current();
  }

  Connection getConnection() throws SQLException {
    if (getDatabaseConnections().current() == null
        || getDatabaseConnections().current().getConnection() == null) {
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


  public class BeelineParser extends GnuParser {
    private boolean isPasswordOptionSet = false;

    @Override
    protected void processOption(String arg, final ListIterator iter) throws  ParseException {
      if (isBeeLineOpt(arg)) {
        processBeeLineOpt(arg);
      } else {
        //-p with the next argument being for BeeLineOpts
        if ("-p".equals(arg)) {
          isPasswordOptionSet = true;
          if(iter.hasNext()) {
            String next = (String) iter.next();
            if(isBeeLineOpt(next)) {
              processBeeLineOpt(next);
              return;
            } else {
              iter.previous();
            }
          }
        }
        super.processOption(arg, iter);
      }
    }

    private void processBeeLineOpt(final String arg) {
      String stripped = arg.substring(2, arg.length());
      String[] parts = split(stripped, "=");
      debug(loc("setting-prop", Arrays.asList(parts)));
      if (parts.length >= 2) {
        getOpts().set(parts[0], parts[1], true);
      } else {
        getOpts().set(parts[0], "true", true);
      }
    }

    private boolean isBeeLineOpt(String arg) {
      return arg.startsWith("--") && !(HIVE_VAR_PREFIX.equals(arg) || (HIVE_CONF_PREFIX.equals(arg))
          || "--help".equals(arg) || PROP_FILE_PREFIX.equals(arg));
    }
  }

  int initArgsFromCliVars(String[] args) {
    List<String> commands = Collections.emptyList();

    CliOptionsProcessor optionsProcessor = new CliOptionsProcessor();
    if (!optionsProcessor.process(args)) {
      return 1;
    }
    CommandLine commandLine = optionsProcessor.getCommandLine();


    Properties confProps = commandLine.getOptionProperties("hiveconf");
    for (String propKey : confProps.stringPropertyNames()) {
      setHiveConfVar(propKey, confProps.getProperty(propKey));
    }

    Properties hiveVars = commandLine.getOptionProperties("define");
    for (String propKey : hiveVars.stringPropertyNames()) {
      getOpts().getHiveConfVariables().put(propKey, hiveVars.getProperty(propKey));
    }

    Properties hiveVars2 = commandLine.getOptionProperties("hivevar");
    for (String propKey : hiveVars2.stringPropertyNames()) {
      getOpts().getHiveConfVariables().put(propKey, hiveVars2.getProperty(propKey));
    }

    getOpts().setScriptFile(commandLine.getOptionValue("f"));

    if (commandLine.getOptionValues("i") != null) {
      getOpts().setInitFiles(commandLine.getOptionValues("i"));
    }

    dbName = commandLine.getOptionValue("database");
    getOpts().setVerbose(Boolean.parseBoolean(commandLine.getOptionValue("verbose")));
    getOpts().setSilent(Boolean.parseBoolean(commandLine.getOptionValue("slient")));

    int code = 0;
    if (commandLine.getOptionValues("e") != null) {
      commands = Arrays.asList(commandLine.getOptionValues("e"));
    }

    if (!commands.isEmpty() && getOpts().getScriptFile() != null) {
      System.err.println("The '-e' and '-f' options cannot be specified simultaneously");
      optionsProcessor.printCliUsage();
      return 1;
    }

    if (!commands.isEmpty()) {
      embeddedConnect();
      connectDBInEmbededMode();
      for (Iterator<String> i = commands.iterator(); i.hasNext(); ) {
        String command = i.next().toString();
        debug(loc("executing-command", command));
        if (!dispatch(command)) {
          code++;
        }
      }
      exit = true; // execute and exit
    }
    return code;
  }

  int initArgs(String[] args) {
    List<String> commands = Collections.emptyList();

    CommandLine cl;
    BeelineParser beelineParser;

    try {
      beelineParser = new BeelineParser();
      cl = beelineParser.parse(options, args);
    } catch (ParseException e1) {
      output(e1.getMessage());
      usage();
      return -1;
    }

    boolean connSuccessful = connectUsingArgs(beelineParser, cl);
    // checks if default hs2 connection configuration file is present
    // and uses it to connect if found
    // no-op if the file is not present
    if(!connSuccessful && !exit) {
      connSuccessful = defaultBeelineConnect();
    }

    int code = 0;
    if (cl.getOptionValues('e') != null) {
      commands = Arrays.asList(cl.getOptionValues('e'));
    }

    if (!commands.isEmpty() && getOpts().getScriptFile() != null) {
      error("The '-e' and '-f' options cannot be specified simultaneously");
      return 1;
    } else if(!commands.isEmpty() && !connSuccessful) {
      error("Cannot run commands specified using -e. No current connection");
      return 1;
    }
    if (!commands.isEmpty()) {
      for (Iterator<String> i = commands.iterator(); i.hasNext();) {
        String command = i.next().toString();
        debug(loc("executing-command", command));
        if (!dispatch(command)) {
          code++;
        }
      }
      exit = true; // execute and exit
    }
    return code;
  }


  /*
   * Connects using the command line arguments. There are two
   * possible ways to connect here 1. using the cmd line arguments like -u
   * or using !properties <property-file>
   */
  private boolean connectUsingArgs(BeelineParser beelineParser, CommandLine cl) {
    String driver = null, user = null, pass = "", url = null;
    String auth = null;


    if (cl.hasOption("help")) {
      usage();
      getOpts().setHelpAsked(true);
      return true;
    }

    Properties hiveVars = cl.getOptionProperties("hivevar");
    for (String key : hiveVars.stringPropertyNames()) {
      getOpts().getHiveVariables().put(key, hiveVars.getProperty(key));
    }

    Properties hiveConfs = cl.getOptionProperties("hiveconf");
    for (String key : hiveConfs.stringPropertyNames()) {
      setHiveConfVar(key, hiveConfs.getProperty(key));
    }

    driver = cl.getOptionValue("d");
    auth = cl.getOptionValue("a");
    user = cl.getOptionValue("n");
    getOpts().setAuthType(auth);
    if (cl.hasOption("w")) {
      pass = obtainPasswordFromFile(cl.getOptionValue("w"));
    } else {
      if (beelineParser.isPasswordOptionSet) {
        pass = cl.getOptionValue("p");
      }
    }
    url = cl.getOptionValue("u");
    if ((url == null) && cl.hasOption("reconnect")){
      // If url was not specified with -u, but -r was present, use that.
      url = getOpts().getLastConnectedUrl();
    }
    getOpts().setInitFiles(cl.getOptionValues("i"));
    getOpts().setScriptFile(cl.getOptionValue("f"));


    if (url != null) {
      String com;
      String comForDebug;
      if(pass != null) {
        com = constructCmd(url, user, pass, driver, false);
        comForDebug = constructCmd(url, user, pass, driver, true);
      } else {
        com = constructCmdUrl(url, user, driver, false);
        comForDebug = constructCmdUrl(url, user, driver, true);
      }
      debug(comForDebug);
      return dispatch(com);
    }
    // load property file
    String propertyFile = cl.getOptionValue("property-file");
    if (propertyFile != null) {
      try {
        this.consoleReader = new ConsoleReader();
      } catch (IOException e) {
        handleException(e);
      }
      if (!dispatch("!properties " + propertyFile)) {
        exit = true;
        return false;
      }
    }
    return false;
  }

  private void setHiveConfVar(String key, String val) {
    getOpts().getHiveConfVariables().put(key, val);
    if (HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname.equals(key) && "mr".equals(val)) {
      info(HiveConf.generateMrDeprecationWarning());
    }
  }

  private String constructCmd(String url, String user, String pass, String driver, boolean stripPasswd) {
    return new StringBuilder()
       .append("!connect ")
       .append(url)
       .append(" ")
       .append(user == null || user.length() == 0 ? "''" : user)
       .append(" ")
       .append(stripPasswd ? PASSWD_MASK : (pass.length() == 0 ? "''" : pass))
       .append(" ")
       .append((driver == null ? "" : driver))
       .toString();
  }

  /**
   * This is an internal method used to create !connect command when -p option is used without
   * providing the password on the command line. Connect command returned should be ; separated
   * key-value pairs along with the url. We cannot use space separated !connect url user [password]
   * [driver] here since both password and driver are optional and there would be no way to
   * distinguish if the last string is password or driver
   *
   * @param url connection url passed using -u argument on the command line
   * @param user username passed through command line
   * @param driver driver passed through command line -d option
   * @param stripPasswd when set to true generates a !connect command which strips the password for
   *          logging purposes
   * @return !connect command
   */
  private String constructCmdUrl(String url, String user, String driver,
      boolean stripPasswd)  {
    StringBuilder command = new StringBuilder("!connect ");
    command.append(url);
    //if the url does not have a database name add the trailing '/'
    if(isTrailingSlashNeeded(url)) {
      command.append('/');
    }
    command.append(';');
    // if the username is not already available in the URL add the one provided
    if (Utils.parsePropertyFromUrl(url, JdbcConnectionParams.AUTH_USER) == null) {
      command.append(JdbcConnectionParams.AUTH_USER);
      command.append('=');
      command.append((user == null || user.length() == 0 ? "''" : user));
    }
    if (stripPasswd) {
      // if password is available in url it needs to be striped
      int startIndex = command.indexOf(JdbcConnectionParams.AUTH_PASSWD + "=")
          + JdbcConnectionParams.AUTH_PASSWD.length() + 2;
      if(startIndex != -1) {
        int endIndex = command.toString().indexOf(";", startIndex);
        command.replace(startIndex, (endIndex == -1 ? command.length() : endIndex),
          BeeLine.PASSWD_MASK);
      }
    }
    // if the driver is not already available in the URL add the one provided
    if (Utils.parsePropertyFromUrl(url, JdbcConnectionParams.PROPERTY_DRIVER) == null
        && driver != null) {
      command.append(';');
      command.append(JdbcConnectionParams.PROPERTY_DRIVER);
      command.append("=");
      command.append(driver);
    }
    return command.toString();
  }

  /*
   * Returns true if trailing slash is needed to be appended to the url
   */
  private boolean isTrailingSlashNeeded(String url) {
    if (url.toLowerCase().startsWith("jdbc:hive2://")) {
      return url.indexOf('/', "jdbc:hive2://".length()) < 0;
    }
    return false;
  }


  /**
   * Obtains a password from the passed file path.
   */
  private String obtainPasswordFromFile(String passwordFilePath) {
    try {
      Path path = Paths.get(passwordFilePath);
      byte[] passwordFileContents = Files.readAllBytes(path);
      return new String(passwordFileContents, "UTF-8").trim();
    } catch (Exception e) {
      throw new RuntimeException("Unable to read user password from the password file: "
          + passwordFilePath, e);
    }
  }

  public void updateOptsForCli() {
    getOpts().updateBeeLineOptsFromConf();
    getOpts().setShowHeader(false);
    getOpts().setOutputFormat("dsv");
    getOpts().setDelimiterForDSV(' ');
    getOpts().setNullEmptyString(true);
  }

  /**
   * Start accepting input from stdin, and dispatch it
   * to the appropriate {@link CommandHandler} until the
   * global variable <code>exit</code> is true.
   */
  public int begin(String[] args, InputStream inputStream) throws IOException {
    try {
      // load the options first, so we can override on the command line
      getOpts().load();
    } catch (Exception e) {
      // nothing
    }

    setupHistory();

    //add shutdown hook to cleanup the beeline for smooth exit
    addBeelineShutdownHook();

    //this method also initializes the consoleReader which is
    //needed by initArgs for certain execution paths
    ConsoleReader reader = initializeConsoleReader(inputStream);
    if (isBeeLine) {
      int code = initArgs(args);
      if (code != 0) {
        return code;
      }
    } else {
      int code = initArgsFromCliVars(args);
      if (code != 0 || exit) {
        return code;
      }
      defaultConnect(false);
    }

    if (getOpts().isHelpAsked()) {
      return 0;
    }
    if (getOpts().getScriptFile() != null) {
      return executeFile(getOpts().getScriptFile());
    }
    try {
      info(getApplicationTitle());
    } catch (Exception e) {
      // ignore
    }
    return execute(reader, false);
  }

  /*
   * Attempts to make a connection using default HS2 connection config file if available
   * if there connection is not made return false
   *
   */
  private boolean defaultBeelineConnect() {
    String url;
    try {
      url = getDefaultConnectionUrl();
      if (url == null) {
        debug("Default hs2 connection config file not found");
        return false;
      }
    } catch (BeelineHS2ConnectionFileParseException e) {
      error(e);
      return false;
    }
    return dispatch("!connect " + url);
  }


  private String getDefaultConnectionUrl() throws BeelineHS2ConnectionFileParseException {
    HS2ConnectionFileParser userHS2ConnFileParser = getUserHS2ConnFileParser();
    if (!userHS2ConnFileParser.configExists()) {
      // nothing to do if there is no user HS2 connection configuration file
      return null;
    }
    // get the connection properties from user specific config file
    Properties userConnectionProperties = userHS2ConnFileParser.getConnectionProperties();
    // load the HS2 connection url properties from hive-site.xml if it is present in the classpath
    HS2ConnectionFileParser hiveSiteParser = getHiveSiteHS2ConnectionFileParser();
    Properties hiveSiteConnectionProperties = hiveSiteParser.getConnectionProperties();
    // add/override properties found from hive-site with user-specific properties
    for (String key : userConnectionProperties.stringPropertyNames()) {
      if (hiveSiteConnectionProperties.containsKey(key)) {
        debug("Overriding connection url property " + key
            + " from user connection configuration file");
      }
      hiveSiteConnectionProperties.setProperty(key, userConnectionProperties.getProperty(key));
    }
    // return the url based on the aggregated connection properties
    return HS2ConnectionFileUtils.getUrl(hiveSiteConnectionProperties);
  }

  /*
   * Increased visibility of this method is only for providing better test coverage
   */
  @VisibleForTesting
  public HS2ConnectionFileParser getUserHS2ConnFileParser() {
    return new UserHS2ConnectionFileParser();
  }

  /*
   * Increased visibility of this method is only for providing better test coverage
   */
  @VisibleForTesting
  public HS2ConnectionFileParser getHiveSiteHS2ConnectionFileParser() {
    return new HiveSiteHS2ConnectionFileParser();
  }

  int runInit() {
    String initFiles[] = getOpts().getInitFiles();
    if (initFiles != null && initFiles.length != 0) {
      for (String initFile : initFiles) {
        info("Running init script " + initFile);
        try {
          return executeFile(initFile);
        } finally {
          exit = false;
        }
      }
    }
    return ERRNO_OK;
  }

  private int embeddedConnect() {
    if (!execCommandWithPrefix("!connect " + Utils.URL_PREFIX + " '' ''")) {
      return ERRNO_OTHER;
    } else {
      return ERRNO_OK;
    }
  }

  private int connectDBInEmbededMode() {
    if (dbName != null && !dbName.isEmpty()) {
      if (!dispatch("use " + dbName + ";")) {
        return ERRNO_OTHER;
      }
    }
    return ERRNO_OK;
  }

  public int defaultConnect(boolean exitOnError) {
    if (embeddedConnect() != ERRNO_OK && exitOnError) {
      return ERRNO_OTHER;
    }
    if (connectDBInEmbededMode() != ERRNO_OK && exitOnError) {
      return ERRNO_OTHER;
    }
    return ERRNO_OK;
  }

  private int executeFile(String fileName) {
    InputStream fileStream = null;
    try {
      if (!isBeeLine) {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(fileName);
        FileSystem fs;
        HiveConf conf = getCommands().getHiveConf(true);
        if (!path.toUri().isAbsolute()) {
          fs = FileSystem.getLocal(conf);
          path = fs.makeQualified(path);
        } else {
          fs = FileSystem.get(path.toUri(), conf);
        }
        fileStream = fs.open(path);
      } else {
        fileStream = new FileInputStream(fileName);
      }
      return execute(initializeConsoleReader(fileStream), !getOpts().getForce());
    } catch (Throwable t) {
      handleException(t);
      return ERRNO_OTHER;
    } finally {
      IOUtils.closeStream(fileStream);
      output("");   // dummy new line
    }
  }

  private int execute(ConsoleReader reader, boolean exitOnError) {
    int lastExecutionResult = ERRNO_OK;
    while (!exit) {
      try {
        // Execute one instruction; terminate on executing a script if there is an error
        // in silent mode, prevent the query and prompt being echoed back to terminal
        String line = (getOpts().isSilent() && getOpts().getScriptFile() != null) ? reader
            .readLine(null, ConsoleReader.NULL_MASK) : reader.readLine(getPrompt());

        // trim line
        if (line != null) {
          line = line.trim();
        }

        if (!dispatch(line)) {
          lastExecutionResult = ERRNO_OTHER;
          if (exitOnError) break;
        } else if (line != null) {
          lastExecutionResult = ERRNO_OK;
        }

      } catch (Throwable t) {
        handleException(t);
        return ERRNO_OTHER;
      }
    }
    return lastExecutionResult;
  }

  @Override
  public void close() {
    commands.closeall(null);
  }

  private void setupHistory() throws IOException {
    if (this.history != null) {
       return;
    }

    this.history = new FileHistory(new File(getOpts().getHistoryFile()));
  }

  private void addBeelineShutdownHook() throws IOException {
    // add shutdown hook to flush the history to history file and it also close all open connections
    ShutdownHookManager.addShutdownHook(getShutdownHook());
  }

  public ConsoleReader initializeConsoleReader(InputStream inputStream) throws IOException {
    if (inputStream != null) {
      // ### NOTE: fix for sf.net bug 879425.
      // Working around an issue in jline-2.1.2, see https://github.com/jline/jline/issues/10
      // by appending a newline to the end of inputstream
      InputStream inputStreamAppendedNewline = new SequenceInputStream(inputStream,
          new ByteArrayInputStream((new String("\n")).getBytes()));
      consoleReader = new ConsoleReader(inputStreamAppendedNewline, getOutputStream());
      consoleReader.setCopyPasteDetection(true); // jline will detect if <tab> is regular character
    } else {
      consoleReader = new ConsoleReader();
    }

    //disable the expandEvents for the purpose of backward compatibility
    consoleReader.setExpandEvents(false);

    try {
      // now set the output for the history
      consoleReader.setHistory(this.history);
    } catch (Exception e) {
      handleException(e);
    }

    if (inputStream instanceof FileInputStream || inputStream instanceof FSDataInputStream) {
      // from script.. no need to load history and no need of completer, either
      return consoleReader;
    }

    consoleReader.addCompleter(new BeeLineCompleter(this));
    return consoleReader;
  }

  void usage() {
    output(loc("cmd-usage"));
  }

  /**
   * This method is used for executing commands beginning with !
   * @param line
   * @return
   */
  public boolean execCommandWithPrefix(String line) {
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
    }
    if (cmdMap.size() > 1) {
      // any exact match?
      CommandHandler handler = cmdMap.get(line);
      if (handler == null) {
        return error(loc("multiple-matches", cmdMap.keySet().toString()));
      }
      return handler.execute(line);
    }
    return cmdMap.values().iterator().next().execute(line);
  }

  /**
   * Dispatch the specified line to the appropriate {@link CommandHandler}.
   *
   * @param line
   *          the command-line to dispatch
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

    if (isBeeLine) {
      if (line.startsWith(COMMAND_PREFIX)) {
        // handle SQLLine command in beeline which starts with ! and does not end with ;
        return execCommandWithPrefix(line);
      } else {
        return commands.sql(line, getOpts().getEntireLineAsCommand());
      }
    } else {
      return commands.sql(line, getOpts().getEntireLineAsCommand());
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
    String lineTrimmed = line.trim();
    return lineTrimmed.startsWith("#") || lineTrimmed.startsWith("--");
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
    if (isBeeLine) {
      return getPromptForBeeline();
    } else {
      return getPromptForCli();
    }
  }

  String getPromptForCli() {
    String prompt;
    // read prompt configuration and substitute variables.
    HiveConf conf = getCommands().getHiveConf(true);
    prompt = conf.getVar(HiveConf.ConfVars.CLIPROMPT);
    prompt = getCommands().substituteVariables(conf, prompt);
    return prompt + getFormattedDb() + "> ";
  }

  /**
   * Retrieve the current database name string to display, based on the
   * configuration value.
   *
   * @return String to show user for current db value
   */
  String getFormattedDb() {
    if (!getOpts().getShowDbInPrompt()) {
      return "";
    }
    String currDb = getCurrentDatabase();

    if (currDb == null) {
      return "";
    }

    return " (" + currDb + ")";
  }

  String getPromptForBeeline() {
    if (getDatabaseConnection() == null || getDatabaseConnection().getUrl() == null) {
      return "beeline> ";
    } else {
      String printClosed = getDatabaseConnection().isClosed() ? " (closed)" : "";
      return getPromptForBeeline(getDatabaseConnections().getIndex()
          + ": " + getDatabaseConnection().getUrl()) + printClosed + getFormattedDb() + "> ";
    }
  }

  static String getPromptForBeeline(String url) {
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
    } else if (e instanceof EOFException) {
      setExit(true);  // CTRL-D
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

    if (e.getCause() instanceof TTransportException) {
      switch (((TTransportException)e.getCause()).getType()) {
        case TTransportException.ALREADY_OPEN:
          error(loc("hs2-connection-already-open"));
          break;
        case TTransportException.END_OF_FILE:
          error(loc("hs2-unexpected-end-of-file"));
          break;
        case TTransportException.NOT_OPEN:
          error(loc("hs2-could-not-open-connection"));
          break;
        case TTransportException.TIMED_OUT:
          error(loc("hs2-connection-timed-out"));
          break;
        case TTransportException.UNKNOWN:
          error(loc("hs2-unknown-connection-problem"));
          break;
        default:
          error(loc("hs2-unexpected-error"));
      }
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

      // find whether exists a local driver to accept the url
      if (findLocalDriver(url) != null) {
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

  public Driver findLocalDriver(String url) throws Exception {
    if(drivers == null){
      return null;
    }

    for (Driver d : drivers) {
      try {
        String clazzName = d.getClass().getName();
        Driver driver = (Driver) Class.forName(clazzName, true,
          Thread.currentThread().getContextClassLoader()).newInstance();
        if (driver.acceptsURL(url) && isSupportedLocalDriver(driver)) {
          return driver;
        }
      } catch (SQLException e) {
        error(e);
        throw new Exception(e);
      }
    }
    return null;
  }

  public boolean isSupportedLocalDriver(Driver driver) {
    String driverName = driver.getClass().getName();
    for (String name : supportedLocalDriver) {
      if (name.equals(driverName)) {
        return true;
      }
    }
    return false;
  }

  public void addLocalDriverClazz(String driverClazz) {
    supportedLocalDriver.add(driverClazz);
  }

  Driver[] scanDrivers(String line) throws IOException {
    return scanDrivers(false);
  }


  Driver[] scanDrivers(boolean knownOnly) throws IOException {
    long start = System.currentTimeMillis();

    Set<String> classNames = new HashSet<String>();

    if (!knownOnly) {
      classNames.addAll(Arrays.asList(
          ClassNameCompleter.getClassNames()));
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

    if (f instanceof TableOutputFormat) {
      if (getOpts().getIncremental()) {
        rows = new IncrementalRowsWithNormalization(this, rs);
      } else {
        rows = new BufferedRows(this, rs);
      }
    } else {
      rows = new IncrementalRows(this, rs);
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

  Runnable getShutdownHook() {
    return shutdownHook;
  }

  Completer getCommandCompletor() {
    return beeLineCommandCompleter;
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

  public boolean isBeeLine() {
    return isBeeLine;
  }

  public void setBeeLine(boolean isBeeLine) {
    this.isBeeLine = isBeeLine;
  }

  public String getCurrentDatabase() {
    if (currentDatabase == null) {
      currentDatabase = DEFAULT_DATABASE_NAME;
    }
    return currentDatabase;
  }

  public void setCurrentDatabase(String currentDatabase) {
    this.currentDatabase = currentDatabase;
  }
}
