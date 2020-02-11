/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.druid;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ForkingDruidNode extends DruidNode {
  private final static String DEFAULT_JAVA_CMD = "java";

  private final static Logger log = LoggerFactory.getLogger(ForkingDruidNode.class);

  private final String classpath;

  private final Map<String, String> properties;

  private final List<String> jvmArgs;

  private final File logLocation;

  private final File logFile;

  private final String javaCmd;

  private final ProcessBuilder processBuilder = new ProcessBuilder();

  private Process druidProcess = null;

  private Boolean started = false;

  private final List<String> allowedPrefixes = Lists.newArrayList(
          "com.metamx",
          "druid",
          "org.apache.druid",
          "java.io.tmpdir",
          "hadoop"
  );

  public ForkingDruidNode(String nodeType,
          String extraClasspath,
          Map<String, String> properties,
          List<String> jvmArgs,
          File logLocation,
          String javaCmd
  ) {
    super(nodeType);

    final List<String> command = Lists.newArrayList();
    this.classpath = Strings.isNullOrEmpty(extraClasspath)
            ? System.getProperty("java.class.path")
            : extraClasspath;
    this.properties = properties == null ? new HashMap<>() : properties;
    this.jvmArgs = Preconditions.checkNotNull(jvmArgs);
    this.logLocation = logLocation == null ? new File("/tmp/druid") : logLocation;
    if (!this.logLocation.exists()) {
      this.logLocation.mkdirs();
    }

    this.javaCmd = javaCmd == null ? DEFAULT_JAVA_CMD : javaCmd;

    logFile = new File(this.logLocation, getNodeType() + ".log");
    // set the log stream
    processBuilder.redirectErrorStream(true);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
    command.add(this.javaCmd);
    command.addAll(this.jvmArgs);
    command.add("-server");
    command.add("-cp");
    command.add(classpath);

    // inject properties from the main App that matches allowedPrefix
    for (String propName : System.getProperties().stringPropertyNames()) {
      for (String allowedPrefix : allowedPrefixes) {
        if (propName.startsWith(allowedPrefix)) {
          command.add(
                  String.format(
                          "-D%s=%s",
                          propName,
                          System.getProperty(propName)
                  )
          );
        }
      }
    }
    this.properties
            .forEach((key, value) -> command.add(String.format("-D%s=%s", key, value)));
    command.addAll(Lists.newArrayList("org.apache.druid.cli.Main", "server", getNodeType()));
    processBuilder.command(command);
    log.info("Creating forking druid node with " + String.join(" ", processBuilder.command()));
  }

  @Override
  public void start() throws IOException {
    synchronized (started) {
      if (started == false) {
        druidProcess = processBuilder.start();
        started = true;
      }
      log.info("Started " + getNodeType());
    }
  }

  @Override
  public boolean isAlive() {
    synchronized (started) {
      return started && druidProcess != null && druidProcess.isAlive();
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (started) {
      if (druidProcess != null && druidProcess.isAlive()) {
        druidProcess.destroy();
      }
      try {
        log.info("Waiting for " + getNodeType());
        if (druidProcess.waitFor(5000, TimeUnit.MILLISECONDS)) {
          log.info(String.format("Shutdown completed for node [%s]", getNodeType()));
        } else {
          log.info(String.format("Waiting to shutdown node [%s] exhausted shutting down forcibly", getNodeType()));
          druidProcess.destroyForcibly();
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
        Throwables.propagate(e);
      }
    }
  }
}
