/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hive.ptest.execution.conf.ExecutionContextConfiguration;
import org.apache.hive.ptest.execution.conf.Host;
import org.apache.hive.ptest.execution.conf.TestConfiguration;
import org.apache.hive.ptest.execution.conf.TestParser;
import org.apache.hive.ptest.execution.context.ExecutionContext;
import org.apache.hive.ptest.execution.context.ExecutionContextProvider;
import org.apache.hive.ptest.execution.ssh.NonZeroExitCodeException;
import org.apache.hive.ptest.execution.ssh.RSyncCommandExecutor;
import org.apache.hive.ptest.execution.ssh.SSHCommandExecutor;
import org.apache.velocity.app.Velocity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class PTest {

  static {
    Velocity.init();
  }
  private static final Logger LOG = LoggerFactory
      .getLogger(PTest.class);


  private final TestConfiguration mConfiguration;
  private final ListeningExecutorService mExecutor;
  private final Set<String> mExecutedTests;
  private final Set<String> mFailedTests;
  private final List<Phase> mPhases;
  private final ExecutionContext mExecutionContext;
  private final Logger mLogger;
  private final List<HostExecutor> mHostExecutors;
  private final String mBuildTag;
  private final SSHCommandExecutor mSshCommandExecutor;
  private final RSyncCommandExecutor mRsyncCommandExecutor;

  public PTest(final TestConfiguration configuration, final ExecutionContext executionContext,
      final String buildTag, final File logDir, final LocalCommandFactory localCommandFactory,
      final SSHCommandExecutor sshCommandExecutor, final  RSyncCommandExecutor rsyncCommandExecutor,
      final Logger logger) throws Exception {
    mConfiguration = configuration;
    mLogger = logger;
    mBuildTag = buildTag;
    mExecutedTests = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    mFailedTests = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    mExecutionContext = executionContext;
    mSshCommandExecutor = sshCommandExecutor;
    mRsyncCommandExecutor = rsyncCommandExecutor;
    mExecutor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    final File failedLogDir = Dirs.create(new File(logDir, "failed"));
    final File succeededLogDir = Dirs.create(new File(logDir, "succeeded"));
    final File scratchDir = Dirs.createEmpty(new File(mExecutionContext.getLocalWorkingDirectory(), "scratch"));
    File patchDir = Dirs.createEmpty(new File(logDir, "patches"));
    File patchFile = null;
    if(!configuration.getPatch().isEmpty()) {
      patchFile = new File(patchDir, buildTag + ".patch");
      Files.write(Resources.toByteArray(new URL(configuration.getPatch())), patchFile);
    }
    ImmutableMap.Builder<String, String> templateDefaultsBuilder = ImmutableMap.builder();
    templateDefaultsBuilder.
    put("repository", configuration.getRepository()).
    put("repositoryName", configuration.getRepositoryName()).
    put("repositoryType", configuration.getRepositoryType()).
    put("buildTool", configuration.getBuildTool()).
    put("branch", configuration.getBranch()).
    put("clearLibraryCache", String.valueOf(configuration.isClearLibraryCache())).
    put("workingDir", mExecutionContext.getLocalWorkingDirectory()).
    put("buildTag", buildTag).
    put("logDir", logDir.getAbsolutePath()).
    put("javaHome", configuration.getJavaHome()).
    put("javaHomeForTests", configuration.getJavaHomeForTests()).
    put("antEnvOpts", configuration.getAntEnvOpts()).
    put("antArgs", configuration.getAntArgs()).
    put("antTestArgs", configuration.getAntTestArgs()).
    put("antTestTarget", configuration.getAntTestTarget()).
    put("mavenEnvOpts", configuration.getMavenEnvOpts()).
    put("mavenArgs", configuration.getMavenArgs()).
    put("mavenBuildArgs", configuration.getMavenBuildArgs()).
    put("mavenTestArgs", configuration.getMavenTestArgs());
    final ImmutableMap<String, String> templateDefaults = templateDefaultsBuilder.build();
    TestParser testParser = new TestParser(configuration.getContext(), configuration.getTestCasePropertyName(),
        new File(mExecutionContext.getLocalWorkingDirectory(), configuration.getRepositoryName() + "-source"),
        logger);

    HostExecutorBuilder hostExecutorBuilder = new HostExecutorBuilder() {
      @Override
      public HostExecutor build(Host host) {
        return new HostExecutor(host, executionContext.getPrivateKey(), mExecutor, sshCommandExecutor,
            rsyncCommandExecutor, templateDefaults, scratchDir, succeededLogDir, failedLogDir, 10, logger);
      }

    };
    List<HostExecutor> hostExecutors = new ArrayList<HostExecutor>();
    for(Host host : mExecutionContext.getHosts()) {
      hostExecutors.add(hostExecutorBuilder.build(host));
    }
    mHostExecutors = new CopyOnWriteArrayList<HostExecutor>(hostExecutors);
    mPhases = Lists.newArrayList();
    mPhases.add(new PrepPhase(mHostExecutors, localCommandFactory, templateDefaults, scratchDir, patchFile, logger));
    mPhases.add(new ExecutionPhase(mHostExecutors, mExecutionContext, hostExecutorBuilder, localCommandFactory, templateDefaults,
        succeededLogDir, failedLogDir, testParser.parse(), mExecutedTests, mFailedTests, logger));
    mPhases.add(new ReportingPhase(mHostExecutors, localCommandFactory, templateDefaults, logger));
  }
  public int run() {
    int result = 0;
    boolean error = false;
    List<String> messages = Lists.newArrayList();
    Map<String, Long> elapsedTimes = Maps.newTreeMap();
    try {
      mLogger.info("Running tests with " + mConfiguration);
      for(Phase phase : mPhases) {
        String msg = "Executing " + phase.getClass().getName();
        mLogger.info(msg);
        messages.add(msg);
        long start = System.currentTimeMillis();
        try {
          phase.execute();
        } finally {
          long elapsedTime = TimeUnit.MINUTES.convert((System.currentTimeMillis() - start),
              TimeUnit.MILLISECONDS);
          elapsedTimes.put(phase.getClass().getSimpleName(), elapsedTime);
        }
      }
      if(!mFailedTests.isEmpty()) {
        throw new TestsFailedException(mFailedTests.size() + " tests failed");
      }
    } catch(Throwable throwable) {
      mLogger.error("Test run exited with an unexpected error", throwable);
      // NonZeroExitCodeExceptions can have long messages and should be
      // trimmable when published to the JIRA via the JiraService
      if(throwable instanceof NonZeroExitCodeException) {
        messages.add("Tests exited with: " + throwable.getClass().getSimpleName());
        for(String line : Strings.nullToEmpty(throwable.getMessage()).split("\n")) {
          messages.add(line);
        }
      } else {
        messages.add("Tests exited with: " + throwable.getClass().getSimpleName() +
            ": " + throwable.getMessage());
      }
      error = true;
    } finally {
      for(HostExecutor hostExecutor : mHostExecutors) {
        hostExecutor.shutdownNow();
        if(hostExecutor.isBad()) {
          mExecutionContext.addBadHost(hostExecutor.getHost());
        }
      }
      mSshCommandExecutor.shutdownNow();
      mRsyncCommandExecutor.shutdownNow();
      mExecutor.shutdownNow();
      SortedSet<String> failedTests = new TreeSet<String>(mFailedTests);
      if(failedTests.isEmpty()) {
        mLogger.info(String.format("%d failed tests", failedTests.size()));
      } else {
        mLogger.warn(String.format("%d failed tests", failedTests.size()));
      }
      for(String failingTestName : failedTests) {
        mLogger.warn(failingTestName);
      }
      mLogger.info("Executed " + mExecutedTests.size() + " tests");
      for(Map.Entry<String, Long> entry : elapsedTimes.entrySet()) {
        mLogger.info(String.format("PERF: Phase %s took %d minutes", entry.getKey(), entry.getValue()));
      }
      publishJiraComment(error, messages, failedTests);
      if(error || !mFailedTests.isEmpty()) {
        result = 1;
      }
    }
    return result;
  }

  private void publishJiraComment(boolean error, List<String> messages, SortedSet<String> failedTests) {
    if(mConfiguration.getJiraName().isEmpty()) {
      mLogger.info("Skipping JIRA comment as name is empty.");
      return;
    }
    if(mConfiguration.getJiraUrl().isEmpty()) {
      mLogger.info("Skipping JIRA comment as URL is empty.");
      return;
    }
    if(mConfiguration.getJiraUser().isEmpty()) {
      mLogger.info("Skipping JIRA comment as user is empty.");
      return;
    }
    if(mConfiguration.getJiraPassword().isEmpty()) {
      mLogger.info("Skipping JIRA comment as password is empty.");
      return;
    }
    JIRAService jira = new JIRAService(mLogger, mConfiguration, mBuildTag);
    jira.postComment(error, mExecutedTests.size(), failedTests, messages);
  }

  public static class Builder {
    public PTest build(TestConfiguration configuration, ExecutionContext executionContext,
        String buildTag, File logDir, LocalCommandFactory localCommandFactory, SSHCommandExecutor sshCommandExecutor,
        RSyncCommandExecutor rsyncCommandExecutor, Logger logger) throws Exception {
      return new PTest(configuration, executionContext, buildTag, logDir, localCommandFactory, sshCommandExecutor,
          rsyncCommandExecutor, logger);
    }
  }

  private static final String PROPERTIES = "properties";
  private static final String REPOSITORY = TestConfiguration.REPOSITORY;
  private static final String REPOSITORY_NAME = TestConfiguration.REPOSITORY_NAME;
  private static final String BRANCH = TestConfiguration.BRANCH;
  private static final String PATCH = "patch";
  private static final String JAVA_HOME = TestConfiguration.JAVA_HOME;
  private static final String JAVA_HOME_TEST = TestConfiguration.JAVA_HOME_TEST;
  private static final String ANT_TEST_ARGS = TestConfiguration.ANT_TEST_ARGS;
  private static final String ANT_ENV_OPTS = TestConfiguration.ANT_ENV_OPTS;
  private static final String ANT_TEST_TARGET = TestConfiguration.ANT_TEST_TARGET;
  /**
   * All args override properties file settings except
   * for this one which is additive.
   */
  private static final String ANT_ARG = "D";

  public static void main(String[] args) throws Exception {
    LOG.info("Args " + Arrays.toString(args));
    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption(null, PROPERTIES, true, "properties file");
    options.addOption(null, REPOSITORY, true, "Overrides git repository in properties file");
    options.addOption(null, REPOSITORY_NAME, true, "Overrides git repository *name* in properties file");
    options.addOption(null, BRANCH, true, "Overrides git branch in properties file");
    options.addOption(null, PATCH, true, "URI to patch, either file:/// or http(s)://");
    options.addOption(ANT_ARG, null, true, "Supplemntal ant arguments");
    options.addOption(null, JAVA_HOME, true, "Java Home for compiling and running tests (unless " + JAVA_HOME_TEST + " is specified)");
    options.addOption(null, JAVA_HOME_TEST, true, "Java Home for running tests (optional)");
    options.addOption(null, ANT_TEST_ARGS, true, "Arguments to ant test on slave nodes only");
    options.addOption(null, ANT_ENV_OPTS, true, "ANT_OPTS environment variable setting");
    CommandLine commandLine = parser.parse(options, args);
    if(!commandLine.hasOption(PROPERTIES)) {
      throw new IllegalArgumentException(Joiner.on(" ").
          join(PTest.class.getName(), "--" + PROPERTIES,"config.properties"));
    }
    String testConfigurationFile = commandLine.getOptionValue(PROPERTIES);
    ExecutionContextConfiguration executionContextConfiguration = ExecutionContextConfiguration.
        fromFile(testConfigurationFile);
    String buildTag = System.getenv("BUILD_TAG") == null ? "undefined-"
        + System.currentTimeMillis() : System.getenv("BUILD_TAG");
        File logDir = Dirs.create(new File(executionContextConfiguration.getGlobalLogDirectory(), buildTag));
    LogDirectoryCleaner cleaner = new LogDirectoryCleaner(new File(executionContextConfiguration.
        getGlobalLogDirectory()), 5);
    cleaner.setName("LogCleaner-" + executionContextConfiguration.getGlobalLogDirectory());
    cleaner.setDaemon(true);
    cleaner.start();
    TestConfiguration conf = TestConfiguration.fromFile(testConfigurationFile, LOG);
    String repository = Strings.nullToEmpty(commandLine.getOptionValue(REPOSITORY)).trim();
    if(!repository.isEmpty()) {
      conf.setRepository(repository);
    }
    String repositoryName = Strings.nullToEmpty(commandLine.getOptionValue(REPOSITORY_NAME)).trim();
    if(!repositoryName.isEmpty()) {
      conf.setRepositoryName(repositoryName);
    }
    String branch = Strings.nullToEmpty(commandLine.getOptionValue(BRANCH)).trim();
    if(!branch.isEmpty()) {
      conf.setBranch(branch);
    }
    String patch = Strings.nullToEmpty(commandLine.getOptionValue(PATCH)).trim();
    if(!patch.isEmpty()) {
      conf.setPatch(patch);
    }
    String javaHome = Strings.nullToEmpty(commandLine.getOptionValue(JAVA_HOME)).trim();
    if(!javaHome.isEmpty()) {
      conf.setJavaHome(javaHome);
    }
    String javaHomeForTests = Strings.nullToEmpty(commandLine.getOptionValue(JAVA_HOME_TEST)).trim();
    if(!javaHomeForTests.isEmpty()) {
      conf.setJavaHomeForTests(javaHomeForTests);
    }
    String antTestArgs = Strings.nullToEmpty(commandLine.getOptionValue(ANT_TEST_ARGS)).trim();
    if(!antTestArgs.isEmpty()) {
      conf.setAntTestArgs(antTestArgs);
    }
    String antEnvOpts = Strings.nullToEmpty(commandLine.getOptionValue(ANT_ENV_OPTS)).trim();
    if(!antEnvOpts.isEmpty()) {
      conf.setAntEnvOpts(antEnvOpts);
    }
    String antTestTarget = Strings.nullToEmpty(commandLine.getOptionValue(ANT_TEST_TARGET)).trim();
    if(!antTestTarget.isEmpty()) {
      conf.setAntTestTarget(antTestTarget);
    }
    String[] supplementalAntArgs = commandLine.getOptionValues(ANT_ARG);
    if(supplementalAntArgs != null && supplementalAntArgs.length > 0) {
      String antArgs = Strings.nullToEmpty(conf.getAntArgs());
      if(!(antArgs.isEmpty() || antArgs.endsWith(" "))) {
        antArgs += " ";
      }
      antArgs += "-" + ANT_ARG + Joiner.on(" -" + ANT_ARG).join(supplementalAntArgs);
      conf.setAntArgs(antArgs);
    }
    ExecutionContextProvider executionContextProvider = null;
    ExecutionContext executionContext = null;
    int exitCode = 0;
    try {
      executionContextProvider = executionContextConfiguration
          .getExecutionContextProvider();
      executionContext = executionContextProvider.createExecutionContext();
      LocalCommandFactory localCommandFactory = new LocalCommandFactory(LOG);
      PTest ptest = new PTest(conf, executionContext, buildTag, logDir,
          localCommandFactory, new SSHCommandExecutor(LOG, localCommandFactory, conf.getSshOpts()),
          new RSyncCommandExecutor(LOG, 10, localCommandFactory), LOG);
      exitCode = ptest.run();
    } finally {
      if(executionContext != null) {
        executionContext.terminate();
      }
      if(executionContextProvider != null) {
        executionContextProvider.close();
      }
    }
    System.exit(exitCode);
  }
}
