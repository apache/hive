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
package org.apache.hadoop.hive.cli.control;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.QOutProcessor;
import org.apache.hadoop.hive.ql.dataset.Dataset;
import org.apache.hadoop.hive.ql.dataset.DatasetCollection;
import org.apache.hadoop.hive.ql.dataset.QTestDatasetHandler;
import org.apache.hadoop.hive.ql.hooks.PreExecutePrinter;
import org.apache.hadoop.hive.ql.qoption.QTestOptionDispatcher;
import org.apache.hadoop.hive.ql.qoption.QTestReplaceHandler;
import org.apache.hive.beeline.ConvertedOutputFile.Converter;
import org.apache.hive.beeline.QFile;
import org.apache.hive.beeline.QFile.QFileBuilder;
import org.apache.hive.beeline.QFileBeeLineClient;
import org.apache.hive.beeline.QFileBeeLineClient.QFileClientBuilder;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.base.Strings;
import com.google.common.collect.ObjectArrays;

public class CoreBeeLineDriver extends CliAdapter {

  private final File hiveRootDirectory = new File(AbstractCliConfig.HIVE_ROOT);
  private final File queryDirectory;
  private final File logDirectory;
  private final File resultsDirectory;
  private final File initScript;
  private final File cleanupScript;
  private final File testDataDirectory;
  private final File testScriptDirectory;
  private final File datasetDirectory;
  private boolean overwrite = false;
  private boolean useSharedDatabase = false;
  private MiniHS2 miniHS2;
  private QFileClientBuilder clientBuilder;
  private QFileBuilder fileBuilder;
  private final Map<String, Set<String>> datasets = new HashMap<String, Set<String>>();
  protected QTestReplaceHandler replaceHandler;
  private final QOutProcessor qOutProcessor;

  public CoreBeeLineDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
    queryDirectory = new File(testCliConfig.getQueryDirectory());
    logDirectory = new File(testCliConfig.getLogDir());
    String testResultsDirectoryName = System.getProperty("test.results.dir");
    if (testResultsDirectoryName != null) {
      resultsDirectory = new File(hiveRootDirectory, testResultsDirectoryName);
    } else {
      resultsDirectory = new File(testCliConfig.getResultsDir());
    }
    String testDataDirectoryName = System.getProperty("test.data.dir");
    if (testDataDirectoryName == null) {
      testDataDirectory = new File(hiveRootDirectory, "data" + File.separator + "files");
    } else {
      testDataDirectory = new File(testDataDirectoryName);
    }
    testScriptDirectory = new File(hiveRootDirectory, "data" + File.separator + "scripts");
    datasetDirectory = new File(testDataDirectory, "datasets");
    String initScriptFileName = System.getProperty("test.init.script");
    if (initScriptFileName != null) {
      initScript = new File(testScriptDirectory, initScriptFileName);
    } else {
      initScript = new File(testScriptDirectory, testCliConfig.getInitScript());
    }
    cleanupScript = new File(testScriptDirectory, testCliConfig.getCleanupScript());
    this.replaceHandler = new QTestReplaceHandler();
    this.qOutProcessor = new QOutProcessor(null, replaceHandler);
  }

  private static MiniHS2 createMiniServer() throws Exception {
    HiveConf hiveConf = new HiveConf();
    // TODO: HIVE-28031: Adapt some cli driver tests to Tez where it's applicable
    hiveConf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
    // We do not need Zookeeper at the moment
    hiveConf.set(HiveConf.ConfVars.HIVE_LOCK_MANAGER.varname,
        "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");

    MiniHS2 miniHS2 = new MiniHS2.Builder()
        .withConf(hiveConf)
        .cleanupLocalDirOnStartup(true)
        .build();

    miniHS2.start(new HashMap<String, String>());

    System.err.println(HiveConfUtil.dumpConfig(miniHS2.getHiveConf()));

    return miniHS2;
  }

  boolean getBooleanPropertyValue(String name, boolean defaultValue) {
    String value = System.getProperty(name);
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value);
  }
  @Override
  public void beforeClass() throws Exception {
    // We use the beforeClassSpec, because we want to HMS to be ready before the miniHS2 start
    // See CliAdapter.buildClassRule
  }

  @Override
  @BeforeClass
  public void beforeClassSpec() throws Exception {
    overwrite = getBooleanPropertyValue("test.output.overwrite", Boolean.FALSE);

    useSharedDatabase = getBooleanPropertyValue("test.beeline.shared.database", Boolean.FALSE);

    String beeLineUrl = System.getProperty("test.beeline.url");
    if (StringUtils.isEmpty(beeLineUrl)) {
      miniHS2 = createMiniServer();
      beeLineUrl = miniHS2.getJdbcURL();
    }

    clientBuilder = new QFileClientBuilder()
        .setJdbcDriver("org.apache.hive.jdbc.HiveDriver")
        .setJdbcUrl(beeLineUrl)
        .setUsername(System.getProperty("test.beeline.user", "user"))
        .setPassword(System.getProperty("test.beeline.password", "password"));

    boolean comparePortable =
        getBooleanPropertyValue("test.beeline.compare.portable", Boolean.FALSE);

    fileBuilder = new QFileBuilder()
        .setLogDirectory(logDirectory)
        .setQueryDirectory(queryDirectory)
        .setResultsDirectory(resultsDirectory)
        .setUseSharedDatabase(useSharedDatabase)
        .setComparePortable(comparePortable);

    runInfraScript(initScript, new File(logDirectory, "init.beeline"),
        new File(logDirectory, "init.raw"));
  }

  protected void runInfraScript(File script, File beeLineOutput, File log)
      throws IOException, SQLException {
    try (QFileBeeLineClient beeLineClient = clientBuilder.getClient(beeLineOutput)) {
      beeLineClient.execute(
          new String[]{
            "set hive.exec.pre.hooks=" + PreExecutePrinter.class.getName() + ";",
            "set test.data.dir=" + testDataDirectory + ";",
            "set test.script.dir=" + testScriptDirectory + ";",
            "!run " + script,
          },
          log,
          Converter.NONE);
    } catch (Exception e) {
      throw new SQLException("Error running infra script: " + script
          + "\nCheck the following logs for details:\n - " + beeLineOutput + "\n - " + log, e);
    }
  }
  
  protected void runInfraScript(String[] commands, File beeLineOutput, File log)
      throws IOException, SQLException {
    try (QFileBeeLineClient beeLineClient = clientBuilder.getClient(beeLineOutput)) {
      String[] preCommands =
          new String[] { "set hive.exec.pre.hooks=" + PreExecutePrinter.class.getName() + ";",
              "set test.data.dir=" + testDataDirectory + ";",
              "set test.script.dir=" + testScriptDirectory + ";" };

      String[] allCommands =
          Stream.concat(Arrays.stream(preCommands), Arrays.stream(commands)).toArray(String[]::new);
      beeLineClient.execute(allCommands, log, Converter.NONE);
    } catch (Exception e) {
      throw new SQLException("Error running infra commands, "
          + "\nCheck the following logs for details:\n - " + beeLineOutput + "\n - " + log, e);
    }
  }

  @Override
  @AfterClass
  public void shutdown() throws Exception {
    runInfraScript(cleanupScript, new File(logDirectory, "cleanup.beeline"),
        new File(logDirectory, "cleanup.raw"));
    if (miniHS2 != null) {
      miniHS2.stop();
    }
  }

  @Override
  protected QTestUtil getQt() {
    return null;
  }

  private void runTest(QFile qFile, List<Callable<Void>> preCommands) throws Exception {
    try (QFileBeeLineClient beeLineClient = clientBuilder.getClient(qFile.getLogFile())) {
      long startTime = System.currentTimeMillis();
      System.err.println(">>> STARTED " + qFile.getName());

      beeLineClient.execute(qFile, preCommands);

      long queryEndTime = System.currentTimeMillis();
      System.err.println(">>> EXECUTED " + qFile.getName() + ": " + (queryEndTime - startTime)
          + "ms");

      qFile.filterOutput();
      long filterEndTime = System.currentTimeMillis();
      System.err.println(">>> FILTERED " + qFile.getName() + ": " + (filterEndTime - queryEndTime)
          + "ms");

      if (!overwrite) {
        qOutProcessor.maskPatterns(qFile.getOutputFile().getPath());
        QTestProcessExecResult result = qFile.compareResults();

        long compareEndTime = System.currentTimeMillis();
        System.err.println(">>> COMPARED " + qFile.getName() + ": "
            + (compareEndTime - filterEndTime) + "ms");
        if (result.getReturnCode() == 0) {
          System.err.println(">>> PASSED " + qFile.getName());
        } else {
          System.err.println(">>> FAILED " + qFile.getName());
          String messageText = "Client result comparison failed with error code = "
              + result.getReturnCode() + " while executing fname=" + qFile.getName() + "\n";
          String messageBody = Strings.isNullOrEmpty(result.getCapturedOutput()) ?
              qFile.getDebugHint() : result.getCapturedOutput();
          fail(messageText + messageBody);
        }
      } else {
        qFile.overwriteResults();
        System.err.println(">>> PASSED " + qFile.getName());
      }
    } catch (Exception e) {
      throw new Exception("Exception running or analyzing the results of the query file: " + qFile
          + "\n" + qFile.getDebugHint(), e);
    }

  }

  public void runTest(QFile qFile) throws Exception {
    runTest(qFile, null);
  }

  @Override
  public void setUp() {
  }

  @Override
  public void tearDown() {
  }

  @Override
  public void runTest(String name, String name2, String absolutePath) throws Exception {
    QFile qFile = fileBuilder.getQFile(name);
    List<Callable<Void>> commands = initDataSetForTest(qFile);
    runTest(qFile, commands);
  }

  private List<Callable<Void>> initDataSetForTest(QFile qFile) throws Exception {

    QTestOptionDispatcher dispatcher = new QTestOptionDispatcher();
    QTestDatasetHandler datasetHandler = new QTestDatasetHandler(miniHS2.getHiveConf());
    dispatcher.register("dataset", datasetHandler);
    dispatcher.register("replace", replaceHandler);
    dispatcher.process(qFile.getInputFile());

    List<Callable<Void>> commands = new ArrayList<>();

    DatasetCollection datasets = datasetHandler.getDatasets();
    for (String table : datasets.getTables()) {
      Callable<Void> command = initDataset(table, qFile);
      if (command != null) {
        commands.add(command);
      }
    }

    return commands;
  }

  protected Callable<Void> initDataset(String table, QFile qFile) throws Exception {
    if (datasetInitialized(table, qFile)) {
      return null;
    }

    Callable<Void> command = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        File tableFile = new File(new File(datasetDirectory, table), Dataset.INIT_FILE_NAME);
        List<String> datasetLines = FileUtils.readLines(tableFile);
        String[] datasetCommands = datasetLines.toArray(new String[datasetLines.size()]);

        runInfraScript(
            ObjectArrays.concat(String.format("use %s;", qFile.getDatabaseName()), datasetCommands),
            new File(logDirectory, "dataset.beeline"), new File(logDirectory, "dataset.raw"));

        return null;
      }
    };

    datasets.get(qFile.getDatabaseName()).add(table);

    return command;
  }

  private boolean datasetInitialized(String table, QFile qFile) {
    if (datasets.get(qFile.getDatabaseName()) == null) {
      datasets.put(qFile.getDatabaseName(), new HashSet<String>());
      return false;
    }

    return datasets.get(qFile.getDatabaseName()).contains(table);
  }
}
