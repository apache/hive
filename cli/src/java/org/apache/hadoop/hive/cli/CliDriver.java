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

package org.apache.hadoop.hive.cli;

import static org.apache.hadoop.util.StringUtils.stringifyException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jline.ArgumentCompletor;
import jline.ArgumentCompletor.AbstractArgumentDelimiter;
import jline.ArgumentCompletor.ArgumentDelimiter;
import jline.Completor;
import jline.ConsoleReader;
import jline.History;
import jline.SimpleCompletor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.HiveInterruptUtils;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.StreamPrinter;
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.IOUtils;
import org.apache.thrift.TException;

import sun.misc.Signal;
import sun.misc.SignalHandler;


/**
 * CliDriver.
 *
 */
public class CliDriver {

  public static String prompt = null;
  public static String prompt2 = null; // when ';' is not yet seen
  public static final int LINES_TO_FETCH = 40; // number of lines to fetch in batch from remote hive server

  public static final String HIVERCFILE = ".hiverc";

  private final LogHelper console;
  private Configuration conf;

  public CliDriver() {
    SessionState ss = SessionState.get();
    conf = (ss != null) ? ss.getConf() : new Configuration();
    Log LOG = LogFactory.getLog("CliDriver");
    console = new LogHelper(LOG);
  }

  public int processCmd(String cmd) {
    CliSessionState ss = (CliSessionState) SessionState.get();
    // Flush the print stream, so it doesn't include output from the last command
    ss.err.flush();
    String cmd_trimmed = cmd.trim();
    String[] tokens = tokenizeCmd(cmd_trimmed);
    int ret = 0;

    if (cmd_trimmed.toLowerCase().equals("quit") || cmd_trimmed.toLowerCase().equals("exit")) {

      // if we have come this far - either the previous commands
      // are all successful or this is command line. in either case
      // this counts as a successful run
      ss.close();
      System.exit(0);

    } else if (tokens[0].equalsIgnoreCase("source")) {
      String cmd_1 = getFirstCmd(cmd_trimmed, tokens[0].length());

      File sourceFile = new File(cmd_1);
      if (! sourceFile.isFile()){
        console.printError("File: "+ cmd_1 + " is not a file.");
        ret = 1;
      } else {
        try {
          this.processFile(cmd_1);
        } catch (IOException e) {
          console.printError("Failed processing file "+ cmd_1 +" "+ e.getLocalizedMessage(),
            stringifyException(e));
          ret = 1;
        }
      }
    } else if (cmd_trimmed.startsWith("!")) {

      String shell_cmd = cmd_trimmed.substring(1);
      shell_cmd = new VariableSubstitution().substitute(ss.getConf(), shell_cmd);

      // shell_cmd = "/bin/bash -c \'" + shell_cmd + "\'";
      try {
        Process executor = Runtime.getRuntime().exec(shell_cmd);
        StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, ss.out);
        StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, ss.err);

        outPrinter.start();
        errPrinter.start();

        ret = executor.waitFor();
        if (ret != 0) {
          console.printError("Command failed with exit code = " + ret);
        }
      } catch (Exception e) {
        console.printError("Exception raised from Shell command " + e.getLocalizedMessage(),
            stringifyException(e));
        ret = 1;
      }

    } else if (tokens[0].toLowerCase().equals("list")) {

      SessionState.ResourceType t;
      if (tokens.length < 2 || (t = SessionState.find_resource_type(tokens[1])) == null) {
        console.printError("Usage: list ["
            + StringUtils.join(SessionState.ResourceType.values(), "|") + "] [<value> [<value>]*]");
        ret = 1;
      } else {
        List<String> filter = null;
        if (tokens.length >= 3) {
          System.arraycopy(tokens, 2, tokens, 0, tokens.length - 2);
          filter = Arrays.asList(tokens);
        }
        Set<String> s = ss.list_resource(t, filter);
        if (s != null && !s.isEmpty()) {
          ss.out.println(StringUtils.join(s, "\n"));
        }
      }
    } else if (ss.isRemoteMode()) { // remote mode -- connecting to remote hive server
      HiveClient client = ss.getClient();
      PrintStream out = ss.out;
      PrintStream err = ss.err;

      try {
        client.execute(cmd_trimmed);
        List<String> results;
        do {
          results = client.fetchN(LINES_TO_FETCH);
          for (String line : results) {
            out.println(line);
          }
        } while (results.size() == LINES_TO_FETCH);
      } catch (HiveServerException e) {
        ret = e.getErrorCode();
        if (ret != 0) { // OK if ret == 0 -- reached the EOF
          String errMsg = e.getMessage();
          if (errMsg == null) {
            errMsg = e.toString();
          }
          ret = e.getErrorCode();
          err.println("[Hive Error]: " + errMsg);
        }
      } catch (TException e) {
        String errMsg = e.getMessage();
        if (errMsg == null) {
          errMsg = e.toString();
        }
        ret = -10002;
        err.println("[Thrift Error]: " + errMsg);
      } finally {
        try {
          client.clean();
        } catch (TException e) {
          String errMsg = e.getMessage();
          if (errMsg == null) {
            errMsg = e.toString();
          }
          err.println("[Thrift Error]: Hive server is not cleaned due to thrift exception: "
              + errMsg);
        }
      }
    } else { // local mode
      try {
        CommandProcessor proc = CommandProcessorFactory.get(tokens[0], (HiveConf) conf);
        ret = processLocalCmd(cmd, proc, ss);
      } catch (SQLException e) {
        console.printError("Failed processing command " + tokens[0] + " " + e.getLocalizedMessage(),
          org.apache.hadoop.util.StringUtils.stringifyException(e));
        ret = 1;
      }
    }

    return ret;
  }

  /**
   * For testing purposes to inject Configuration dependency
   * @param conf to replace default
   */
  void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Extract and clean up the first command in the input.
   */
  private String getFirstCmd(String cmd, int length) {
    return cmd.substring(length).trim();
  }

  private String[] tokenizeCmd(String cmd) {
    return cmd.split("\\s+");
  }

  int processLocalCmd(String cmd, CommandProcessor proc, CliSessionState ss) {
    int tryCount = 0;
    boolean needRetry;
    int ret = 0;

    do {
      try {
        needRetry = false;
        if (proc != null) {
          if (proc instanceof Driver) {
            Driver qp = (Driver) proc;
            PrintStream out = ss.out;
            long start = System.currentTimeMillis();
            if (ss.getIsVerbose()) {
              out.println(cmd);
            }

            qp.setTryCount(tryCount);
            ret = qp.run(cmd).getResponseCode();
            if (ret != 0) {
              qp.close();
              return ret;
            }

            // query has run capture the time
            long end = System.currentTimeMillis();
            double timeTaken = (end - start) / 1000.0;

            ArrayList<String> res = new ArrayList<String>();

            printHeader(qp, out);

            // print the results
            int counter = 0;
            try {
              while (qp.getResults(res)) {
                for (String r : res) {
                  out.println(r);
                }
                counter += res.size();
                res.clear();
                if (out.checkError()) {
                  break;
                }
              }
            } catch (IOException e) {
              console.printError("Failed with exception " + e.getClass().getName() + ":"
                  + e.getMessage(), "\n"
                  + org.apache.hadoop.util.StringUtils.stringifyException(e));
              ret = 1;
            }

            int cret = qp.close();
            if (ret == 0) {
              ret = cret;
            }

            console.printInfo("Time taken: " + timeTaken + " seconds" +
                (counter == 0 ? "" : ", Fetched: " + counter + " row(s)"));
          } else {
            String firstToken = tokenizeCmd(cmd.trim())[0];
            String cmd_1 = getFirstCmd(cmd.trim(), firstToken.length());

            if (ss.getIsVerbose()) {
              ss.out.println(firstToken + " " + cmd_1);
            }
            CommandProcessorResponse res = proc.run(cmd_1);
            if (res.getResponseCode() != 0) {
              ss.out.println("Query returned non-zero code: " + res.getResponseCode() +
                  ", cause: " + res.getErrorMessage());
            }
            ret = res.getResponseCode();
          }
        }
      } catch (CommandNeedRetryException e) {
        console.printInfo("Retry query with a different approach...");
        tryCount++;
        needRetry = true;
      }
    } while (needRetry);

    return ret;
  }

  /**
   * If enabled and applicable to this command, print the field headers
   * for the output.
   *
   * @param qp Driver that executed the command
   * @param out Printstream which to send output to
   */
  private void printHeader(Driver qp, PrintStream out) {
    List<FieldSchema> fieldSchemas = qp.getSchema().getFieldSchemas();
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER)
          && fieldSchemas != null) {
      // Print the column names
      boolean first_col = true;
      for (FieldSchema fs : fieldSchemas) {
        if (!first_col) {
          out.print('\t');
        }
        out.print(fs.getName());
        first_col = false;
      }
      out.println();
    }
  }

  public int processLine(String line) {
    return processLine(line, false);
  }

  /**
   * Processes a line of semicolon separated commands
   *
   * @param line
   *          The commands to process
   * @param allowInterupting
   *          When true the function will handle SIG_INT (Ctrl+C) by interrupting the processing and
   *          returning -1
   * @return 0 if ok
   */
  public int processLine(String line, boolean allowInterupting) {
    SignalHandler oldSignal = null;
    Signal interupSignal = null;

    if (allowInterupting) {
      // Remember all threads that were running at the time we started line processing.
      // Hook up the custom Ctrl+C handler while processing this line
      interupSignal = new Signal("INT");
      oldSignal = Signal.handle(interupSignal, new SignalHandler() {
        private final Thread cliThread = Thread.currentThread();
        private boolean interruptRequested;

        @Override
        public void handle(Signal signal) {
          boolean initialRequest = !interruptRequested;
          interruptRequested = true;

          // Kill the VM on second ctrl+c
          if (!initialRequest) {
            console.printInfo("Exiting the JVM");
            System.exit(127);
          }

          // Interrupt the CLI thread to stop the current statement and return
          // to prompt
          console.printInfo("Interrupting... Be patient, this might take some time.");
          console.printInfo("Press Ctrl+C again to kill JVM");

          // First, kill any running MR jobs
          HadoopJobExecHelper.killRunningJobs();
          HiveInterruptUtils.interrupt();
          this.cliThread.interrupt();
        }
      });
    }

    try {
      int lastRet = 0, ret = 0;

      String command = "";
      for (String oneCmd : line.split(";")) {

        if (StringUtils.endsWith(oneCmd, "\\")) {
          command += StringUtils.chop(oneCmd) + ";";
          continue;
        } else {
          command += oneCmd;
        }
        if (StringUtils.isBlank(command)) {
          continue;
        }

        ret = processCmd(command);
        //wipe cli query state
        SessionState ss = SessionState.get();
        ss.setCommandType(null);
        command = "";
        lastRet = ret;
        boolean ignoreErrors = HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIIGNOREERRORS);
        if (ret != 0 && !ignoreErrors) {
          CommandProcessorFactory.clean((HiveConf) conf);
          return ret;
        }
      }
      CommandProcessorFactory.clean((HiveConf) conf);
      return lastRet;
    } finally {
      // Once we are done processing the line, restore the old handler
      if (oldSignal != null && interupSignal != null) {
        Signal.handle(interupSignal, oldSignal);
      }
    }
  }

  public int processReader(BufferedReader r) throws IOException {
    String line;
    StringBuilder qsb = new StringBuilder();

    while ((line = r.readLine()) != null) {
      // Skipping through comments
      if (! line.startsWith("--")) {
        qsb.append(line + "\n");
      }
    }

    return (processLine(qsb.toString()));
  }

  public int processFile(String fileName) throws IOException {
    FileReader fileReader = null;
    BufferedReader bufferReader = null;
    int rc = 0;
    try {
      fileReader = new FileReader(fileName);
      bufferReader = new BufferedReader(fileReader);
      rc = processReader(bufferReader);
      bufferReader.close();
      bufferReader = null;
    } finally {
      IOUtils.closeStream(bufferReader);
    }
    return rc;
  }

  public void processInitFiles(CliSessionState ss) throws IOException {
    boolean saveSilent = ss.getIsSilent();
    ss.setIsSilent(true);
    for (String initFile : ss.initFiles) {
      int rc = processFile(initFile);
      if (rc != 0) {
        System.exit(rc);
      }
    }
    if (ss.initFiles.size() == 0) {
      if (System.getenv("HIVE_HOME") != null) {
        String hivercDefault = System.getenv("HIVE_HOME") + File.separator +
          "bin" + File.separator + HIVERCFILE;
        if (new File(hivercDefault).exists()) {
          int rc = processFile(hivercDefault);
          if (rc != 0) {
            System.exit(rc);
          }
          console.printError("Putting the global hiverc in " +
                             "$HIVE_HOME/bin/.hiverc is deprecated. Please "+
                             "use $HIVE_CONF_DIR/.hiverc instead.");
        }
      }
      if (System.getenv("HIVE_CONF_DIR") != null) {
        String hivercDefault = System.getenv("HIVE_CONF_DIR") + File.separator
          + HIVERCFILE;
        if (new File(hivercDefault).exists()) {
          int rc = processFile(hivercDefault);
          if (rc != 0) {
            System.exit(rc);
          }
        }
      }
      if (System.getProperty("user.home") != null) {
        String hivercUser = System.getProperty("user.home") + File.separator +
          HIVERCFILE;
        if (new File(hivercUser).exists()) {
          int rc = processFile(hivercUser);
          if (rc != 0) {
            System.exit(rc);
          }
        }
      }
    }
    ss.setIsSilent(saveSilent);
  }

  public void processSelectDatabase(CliSessionState ss) throws IOException {
    String database = ss.database;
    if (database != null) {
      int rc = processLine("use " + database + ";");
      if (rc != 0) {
        System.exit(rc);
      }
    }
  }

  public static Completor[] getCommandCompletor () {
    // SimpleCompletor matches against a pre-defined wordlist
    // We start with an empty wordlist and build it up
    SimpleCompletor sc = new SimpleCompletor(new String[0]);

    // We add Hive function names
    // For functions that aren't infix operators, we add an open
    // parenthesis at the end.
    for (String s : FunctionRegistry.getFunctionNames()) {
      if (s.matches("[a-z_]+")) {
        sc.addCandidateString(s + "(");
      } else {
        sc.addCandidateString(s);
      }
    }

    // We add Hive keywords, including lower-cased versions
    for (String s : HiveParser.getKeywords()) {
      sc.addCandidateString(s);
      sc.addCandidateString(s.toLowerCase());
    }

    // Because we use parentheses in addition to whitespace
    // as a keyword delimiter, we need to define a new ArgumentDelimiter
    // that recognizes parenthesis as a delimiter.
    ArgumentDelimiter delim = new AbstractArgumentDelimiter () {
      @Override
      public boolean isDelimiterChar (String buffer, int pos) {
        char c = buffer.charAt(pos);
        return (Character.isWhitespace(c) || c == '(' || c == ')' ||
          c == '[' || c == ']');
      }
    };

    // The ArgumentCompletor allows us to match multiple tokens
    // in the same line.
    final ArgumentCompletor ac = new ArgumentCompletor(sc, delim);
    // By default ArgumentCompletor is in "strict" mode meaning
    // a token is only auto-completed if all prior tokens
    // match. We don't want that since there are valid tokens
    // that are not in our wordlist (eg. table and column names)
    ac.setStrict(false);

    // ArgumentCompletor always adds a space after a matched token.
    // This is undesirable for function names because a space after
    // the opening parenthesis is unnecessary (and uncommon) in Hive.
    // We stack a custom Completor on top of our ArgumentCompletor
    // to reverse this.
    Completor completor = new Completor () {
      public int complete (String buffer, int offset, List completions) {
        List<String> comp = (List<String>) completions;
        int ret = ac.complete(buffer, offset, completions);
        // ConsoleReader will do the substitution if and only if there
        // is exactly one valid completion, so we ignore other cases.
        if (completions.size() == 1) {
          if (comp.get(0).endsWith("( ")) {
            comp.set(0, comp.get(0).trim());
          }
        }
        return ret;
      }
    };

    HiveConf.ConfVars[] confs = HiveConf.ConfVars.values();
    String[] vars = new String[confs.length];
    for (int i = 0; i < vars.length; i++) {
      vars[i] = confs[i].varname;
    }
    SimpleCompletor conf = new SimpleCompletor(vars);
    conf.setDelimiter(".");

    SimpleCompletor set = new SimpleCompletor("set") {
      @Override
      public int complete(String buffer, int cursor, List clist) {
        return buffer != null && buffer.equals("set") ? super.complete(buffer, cursor, clist) : -1;
      }
    };
    ArgumentCompletor propCompletor = new ArgumentCompletor(new Completor[]{set, conf}) {
      @Override
      @SuppressWarnings("unchecked")
      public int complete(String buffer, int offset, List completions) {
        int ret = super.complete(buffer, offset, completions);
        if (completions.size() == 1) {
          completions.set(0, ((String)completions.get(0)).trim());
        }
        return ret;
      }
    };
    return new Completor[] {propCompletor, completor};
  }

  public static void main(String[] args) throws Exception {
    int ret = new CliDriver().run(args);
    System.exit(ret);
  }

  public  int run(String[] args) throws Exception {

    OptionsProcessor oproc = new OptionsProcessor();
    if (!oproc.process_stage1(args)) {
      return 1;
    }

    // NOTE: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    boolean logInitFailed = false;
    String logInitDetailMessage;
    try {
      logInitDetailMessage = LogUtils.initHiveLog4j();
    } catch (LogInitializationException e) {
      logInitFailed = true;
      logInitDetailMessage = e.getMessage();
    }

    CliSessionState ss = new CliSessionState(new HiveConf(SessionState.class));
    ss.in = System.in;
    try {
      ss.out = new PrintStream(System.out, true, "UTF-8");
      ss.info = new PrintStream(System.err, true, "UTF-8");
      ss.err = new CachingPrintStream(System.err, true, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return 3;
    }

    if (!oproc.process_stage2(ss)) {
      return 2;
    }

    if (!ss.getIsSilent()) {
      if (logInitFailed) {
        System.err.println(logInitDetailMessage);
      } else {
        SessionState.getConsole().printInfo(logInitDetailMessage);
      }
    }

    // set all properties specified via command line
    HiveConf conf = ss.getConf();
    for (Map.Entry<Object, Object> item : ss.cmdProperties.entrySet()) {
      conf.set((String) item.getKey(), (String) item.getValue());
      ss.getOverriddenConfigurations().put((String) item.getKey(), (String) item.getValue());
    }

    // read prompt configuration and substitute variables.
    prompt = conf.getVar(HiveConf.ConfVars.CLIPROMPT);
    prompt = new VariableSubstitution().substitute(conf, prompt);
    prompt2 = spacesForString(prompt);

    SessionState.start(ss);

    // execute cli driver work
    int ret = 0;
    try {
      ret = executeDriver(ss, conf, oproc);
    } catch (Exception e) {
      ss.close();
      throw e;
    }

    ss.close();
    return ret;
  }

  /**
   * Execute the cli work
   * @param ss CliSessionState of the CLI driver
   * @param conf HiveConf for the driver sionssion
   * @param oproc Opetion processor of the CLI invocation
   * @return status of the CLI comman execution
   * @throws Exception
   */
  private  int executeDriver(CliSessionState ss, HiveConf conf, OptionsProcessor oproc)
      throws Exception {

    // connect to Hive Server
    if (ss.getHost() != null) {
      ss.connect();
      if (ss.isRemoteMode()) {
        prompt = "[" + ss.host + ':' + ss.port + "] " + prompt;
        char[] spaces = new char[prompt.length()];
        Arrays.fill(spaces, ' ');
        prompt2 = new String(spaces);
      }
    }

    // CLI remote mode is a thin client: only load auxJars in local mode
    if (!ss.isRemoteMode()) {
      // hadoop-20 and above - we need to augment classpath using hiveconf
      // components
      // see also: code in ExecDriver.java
      ClassLoader loader = conf.getClassLoader();
      String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
      if (StringUtils.isNotBlank(auxJars)) {
        loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","));
      }
      conf.setClassLoader(loader);
      Thread.currentThread().setContextClassLoader(loader);
    }

    CliDriver cli = new CliDriver();
    cli.setHiveVariables(oproc.getHiveVariables());

    // use the specified database if specified
    cli.processSelectDatabase(ss);

    // Execute -i init files (always in silent mode)
    cli.processInitFiles(ss);

    if (ss.execString != null) {
      int cmdProcessStatus = cli.processLine(ss.execString);
      return cmdProcessStatus;
    }

    try {
      if (ss.fileName != null) {
        return cli.processFile(ss.fileName);
      }
    } catch (FileNotFoundException e) {
      System.err.println("Could not open input file for reading. (" + e.getMessage() + ")");
      return 3;
    }

    ConsoleReader reader =  getConsoleReader();
    reader.setBellEnabled(false);
    // reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));
    for (Completor completor : getCommandCompletor()) {
      reader.addCompletor(completor);
    }

    String line;
    final String HISTORYFILE = ".hivehistory";
    String historyDirectory = System.getProperty("user.home");
    try {
      if ((new File(historyDirectory)).exists()) {
        String historyFile = historyDirectory + File.separator + HISTORYFILE;
        reader.setHistory(new History(new File(historyFile)));
      } else {
        System.err.println("WARNING: Directory for Hive history file: " + historyDirectory +
                           " does not exist.   History will not be available during this session.");
      }
    } catch (Exception e) {
      System.err.println("WARNING: Encountered an error while trying to initialize Hive's " +
                         "history file.  History will not be available during this session.");
      System.err.println(e.getMessage());
    }

    int ret = 0;

    String prefix = "";
    String curDB = getFormattedDb(conf, ss);
    String curPrompt = prompt + curDB;
    String dbSpaces = spacesForString(curDB);

    while ((line = reader.readLine(curPrompt + "> ")) != null) {
      if (!prefix.equals("")) {
        prefix += '\n';
      }
      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line;
        ret = cli.processLine(line, true);
        prefix = "";
        curDB = getFormattedDb(conf, ss);
        curPrompt = prompt + curDB;
        dbSpaces = dbSpaces.length() == curDB.length() ? dbSpaces : spacesForString(curDB);
      } else {
        prefix = prefix + line;
        curPrompt = prompt2 + dbSpaces;
        continue;
      }
    }
    return ret;
  }

  protected ConsoleReader getConsoleReader() throws IOException{
    return new ConsoleReader();
  }
  /**
   * Retrieve the current database name string to display, based on the
   * configuration value.
   * @param conf storing whether or not to show current db
   * @param ss CliSessionState to query for db name
   * @return String to show user for current db value
   */
  private static String getFormattedDb(HiveConf conf, CliSessionState ss) {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIPRINTCURRENTDB)) {
      return "";
    }
    //BUG: This will not work in remote mode - HIVE-5153
    String currDb = SessionState.get().getCurrentDatabase();

    if (currDb == null) {
      return "";
    }

    return " (" + currDb + ")";
  }

  /**
   * Generate a string of whitespace the same length as the parameter
   *
   * @param s String for which to generate equivalent whitespace
   * @return  Whitespace
   */
  private static String spacesForString(String s) {
    if (s == null || s.length() == 0) {
      return "";
    }
    return String.format("%1$-" + s.length() +"s", "");
  }

  public void setHiveVariables(Map<String, String> hiveVariables) {
    SessionState.get().setHiveVariables(hiveVariables);
  }

}
