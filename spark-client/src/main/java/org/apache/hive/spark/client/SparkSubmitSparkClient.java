/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.hive.common.log.LogRedirector;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.spark.client.rpc.RpcServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hive.spark.client.SparkClientUtilities.containsErrorKeyword;

/**
 * Extends the {@link AbstractSparkClient} and launches a child process to run Spark's {@code
 * bin/spark-submit} script. Logs are re-directed from the child process logs.
 */
class SparkSubmitSparkClient extends AbstractSparkClient {

  private static final Logger LOG = LoggerFactory.getLogger(SparkSubmitSparkClient.class);

  private static final Pattern YARN_APPLICATION_ID_REGEX = Pattern.compile("\\s(application_[0-9]+_[0-9]+)(\\s|$)");
  private static final String SPARK_HOME_ENV = "SPARK_HOME";
  private static final String SPARK_HOME_KEY = "spark.home";

  private static final long serialVersionUID = -4272763023516238171L;

  private List<String> argv;

  SparkSubmitSparkClient(RpcServer rpcServer, Map<String, String> conf, HiveConf hiveConf,
                         String sessionid) throws IOException {
    super(rpcServer, conf, hiveConf, sessionid);
  }

  @Override
  protected String getSparkHome() {
    String sparkHome = Strings.emptyToNull(conf.get(SPARK_HOME_KEY));
    if (sparkHome == null) {
      sparkHome = Strings.emptyToNull(System.getenv(SPARK_HOME_ENV));
    }
    if (sparkHome == null) {
      sparkHome = Strings.emptyToNull(System.getProperty(SPARK_HOME_KEY));
    }

    Preconditions.checkNotNull(sparkHome, "Cannot use " + HiveConf.HIVE_SPARK_SUBMIT_CLIENT +
            " without setting Spark Home");
    String master = conf.get("spark.master");
    Preconditions.checkArgument(master != null, "spark.master is not defined.");

    argv = Lists.newLinkedList();
    argv.add(new File(sparkHome, "bin/spark-submit").getAbsolutePath());

    return sparkHome;
  }

  @Override
  protected void addAppArg(String arg) {
    argv.add(arg);
  }

  @Override
  protected void addExecutableJar(String jar) {
    argv.add(jar);
  }

  @Override
  protected void addPropertiesFile(String absolutePath) {
    argv.add("--properties-file");
    argv.add(absolutePath);
  }

  @Override
  protected void addClass(String name) {
    argv.add("--class");
    argv.add(RemoteDriver.class.getName());
  }

  @Override
  protected void addJars(String jars) {
    argv.add("--jars");
    argv.add(jars);
  }

  @Override
  protected void addProxyUser(String proxyUser) {
    argv.add("--proxy-user");
    argv.add(proxyUser);
  }

  @Override
  protected void addKeytabAndPrincipal(boolean isDoAsEnabled, String keyTabFile, String principal) {
    if (isDoAsEnabled) {
      List<String> kinitArgv = Lists.newLinkedList();
      kinitArgv.add("kinit");
      kinitArgv.add(principal);
      kinitArgv.add("-k");
      kinitArgv.add("-t");
      kinitArgv.add(keyTabFile + ";");
      kinitArgv.addAll(argv);
      argv = kinitArgv;
    } else {
      // if doAs is not enabled, we pass the principal/keypad to spark-submit in order to
      // support the possible delegation token renewal in Spark
      argv.add("--principal");
      argv.add(principal);
      argv.add("--keytab");
      argv.add(keyTabFile);
    }
  }

  @Override
  protected void addNumExecutors(String numOfExecutors) {
    argv.add("--num-executors");
    argv.add(numOfExecutors);
  }

  @Override
  protected void addExecutorMemory(String executorMemory) {
    argv.add("--executor-memory");
    argv.add(executorMemory);
  }

  @Override
  protected void addExecutorCores(String executorCores) {
    argv.add("--executor-cores");
    argv.add(executorCores);
  }

  private String getSparkJobCredentialProviderPassword() {
    if (conf.containsKey("spark.yarn.appMasterEnv.HADOOP_CREDSTORE_PASSWORD")) {
      return conf.get("spark.yarn.appMasterEnv.HADOOP_CREDSTORE_PASSWORD");
    } else if (conf.containsKey("spark.executorEnv.HADOOP_CREDSTORE_PASSWORD")) {
      return conf.get("spark.executorEnv.HADOOP_CREDSTORE_PASSWORD");
    }
    return null;
  }

  @Override
  protected Future<Void> launchDriver(String isTesting, RpcServer rpcServer, String clientId) throws
          IOException {
    Callable<Void> runnable;

    String cmd = Joiner.on(" ").join(argv);
    LOG.info("Running client driver with argv: {}", cmd);
    ProcessBuilder pb = new ProcessBuilder("sh", "-c", cmd);

    // Prevent hive configurations from being visible in Spark.
    pb.environment().remove("HIVE_HOME");
    pb.environment().remove("HIVE_CONF_DIR");
    // Add credential provider password to the child process's environment
    // In case of Spark the credential provider location is provided in the jobConf when the job is submitted
    String password = getSparkJobCredentialProviderPassword();
    if(password != null) {
      pb.environment().put(Constants.HADOOP_CREDENTIAL_PASSWORD_ENVVAR, password);
    }
    if (isTesting != null) {
      pb.environment().put("SPARK_TESTING", isTesting);
    }

    final Process child = pb.start();
    String threadName = Thread.currentThread().getName();
    final List<String> childErrorLog = Collections.synchronizedList(new ArrayList<String>());
    final List<String> childOutLog = Collections.synchronizedList(new ArrayList<String>());
    final LogRedirector.LogSourceCallback callback = () -> isAlive;

    LogRedirector.redirect("spark-submit-stdout-redir-" + threadName,
        new LogRedirector(child.getInputStream(), LOG, childOutLog, callback));
    LogRedirector.redirect("spark-submit-stderr-redir-" + threadName,
        new LogRedirector(child.getErrorStream(), LOG, childErrorLog, callback));

    runnable = () -> {
      try {
        int exitCode = child.waitFor();
        if (exitCode == 0) {
          synchronized (childOutLog) {
            for (String line : childOutLog) {
              Matcher m = YARN_APPLICATION_ID_REGEX.matcher(line);
              if (m.find()) {
                LOG.info("Found application id " + m.group(1));
                rpcServer.setApplicationId(m.group(1));
              }
            }
          }
          synchronized (childErrorLog) {
            for (String line : childErrorLog) {
              Matcher m = YARN_APPLICATION_ID_REGEX.matcher(line);
              if (m.find()) {
                LOG.info("Found application id " + m.group(1));
                rpcServer.setApplicationId(m.group(1));
              }
            }
          }
        } else {
          List<String> errorMessages = new ArrayList<>();
          synchronized (childErrorLog) {
            for (String line : childErrorLog) {
              if (containsErrorKeyword(line)) {
                errorMessages.add("\"" + line + "\"");
              }
            }
          }

          String errStr = errorMessages.isEmpty() ? "?" : Joiner.on(',').join(errorMessages);

          rpcServer.cancelClient(clientId, new RuntimeException("spark-submit process failed " +
                  "with exit code " + exitCode + " and error " + errStr));
        }
      } catch (InterruptedException ie) {
        LOG.warn("Thread waiting on the child process (spark-submit) is interrupted, killing the child process.");
        rpcServer.cancelClient(clientId, "Thread waiting on the child process (spark-submit) is interrupted");
        Thread.interrupted();
        child.destroy();
      } catch (Exception e) {
        String errMsg = "Exception while waiting for child process (spark-submit)";
        LOG.warn(errMsg, e);
        rpcServer.cancelClient(clientId, errMsg);
      }
      return null;
    };

    FutureTask<Void> futureTask = new FutureTask<>(runnable);

    Thread driverThread = new Thread(futureTask);
    driverThread.setDaemon(true);
    driverThread.setName("SparkSubmitMonitor");
    driverThread.start();

    return futureTask;
  }
}
