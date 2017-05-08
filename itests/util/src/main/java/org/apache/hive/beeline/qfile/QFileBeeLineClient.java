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

package org.apache.hive.beeline.qfile;

import org.apache.hive.beeline.BeeLine;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * QFile test client using BeeLine. It can be used to submit a list of command strings, or a QFile.
 */
public class QFileBeeLineClient implements AutoCloseable {
  private BeeLine beeLine;
  private PrintStream beelineOutputStream;
  private File logFile;

  protected QFileBeeLineClient(String jdbcUrl, String jdbcDriver, String username, String password,
      File log) throws IOException {
    logFile = log;
    beeLine = new BeeLine();
    beelineOutputStream = new PrintStream(logFile, "UTF-8");
    beeLine.setOutputStream(beelineOutputStream);
    beeLine.setErrorStream(beelineOutputStream);
    beeLine.runCommands(
        new String[] {
          "!set verbose true",
          "!set shownestederrs true",
          "!set showwarnings true",
          "!set showelapsedtime false",
          "!set maxwidth -1",
          "!connect " + jdbcUrl + " " + username + " " + password + " " + jdbcDriver
        });
  }

  public boolean execute(String[] commands, File resultFile) {
    boolean hasErrors = false;
    beeLine.runCommands(
        new String[] {
          "!set outputformat csv",
          "!record " + resultFile.getAbsolutePath()
        });

    if (commands.length != beeLine.runCommands(commands)) {
      hasErrors = true;
    }

    beeLine.runCommands(new String[] {"!record"});
    return !hasErrors;
  }

  private void beforeExecute(QFile qFile) {
    assert(execute(
        new String[] {
          "USE default;",
          "SHOW TABLES;",
          "DROP DATABASE IF EXISTS `" + qFile.getName() + "` CASCADE;",
          "CREATE DATABASE `" + qFile.getName() + "`;",
          "USE `" + qFile.getName() + "`;"
        },
        qFile.getInfraLogFile()));
  }

  private void afterExecute(QFile qFile) {
    assert(execute(
        new String[] {
          "USE default;",
          "DROP DATABASE IF EXISTS `" + qFile.getName() + "` CASCADE;",
        },
        qFile.getInfraLogFile()));
  }

  public boolean execute(QFile qFile) {
    beforeExecute(qFile);
    boolean result = execute(
        new String[] {
          "!run " + qFile.getInputFile().getAbsolutePath()
        },
        qFile.getRawOutputFile());
    afterExecute(qFile);
    return result;
  }

  public void close() {
    if (beeLine != null) {
      beeLine.runCommands(new String[] {
        "!quit"
      });
    }
    if (beelineOutputStream != null) {
      beelineOutputStream.close();
    }
  }

  /**
   * Builder to generated QFileBeeLineClient objects. The after initializing the builder, it can be
   * used to create new clients without any parameters.
   */
  public static class QFileClientBuilder {
    private String username;
    private String password;
    private String jdbcUrl;
    private String jdbcDriver;

    public QFileClientBuilder() {
    }

    public QFileClientBuilder setUsername(String username) {
      this.username = username;
      return this;
    }

    public QFileClientBuilder setPassword(String password) {
      this.password = password;
      return this;
    }

    public QFileClientBuilder setJdbcUrl(String jdbcUrl) {
      this.jdbcUrl = jdbcUrl;
      return this;
    }

    public QFileClientBuilder setJdbcDriver(String jdbcDriver) {
      this.jdbcDriver = jdbcDriver;
      return this;
    }

    public QFileBeeLineClient getClient(File logFile) throws IOException {
      return new QFileBeeLineClient(jdbcUrl, jdbcDriver, username, password, logFile);
    }
  }
}
