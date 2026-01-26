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
package org.apache.hadoop.hive.ql.externalDB;

import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Derby extends AbstractExternalDB {

  public Derby() {
  }

  private Path getDbPath() {
    return Paths.get(System.getProperty("test.tmp.dir"), dbName);
  }

  @Override
  protected String getJdbcUrl() {
    // First connection will also create the database
    return JdbcUrl.CREATE.of(getDbPath().toString());
  }

  @Override
  protected String getJdbcDriver() {
    return "org.apache.derby.iapi.jdbc.AutoloadedDriver";
  }

  @Override
  protected int getPort() {
    return -1;
  }

  /**
   * Stops the database and deletes the database files. Check the Derby documentation for details on the
   * <a href="https://db.apache.org/derby/docs/10.17/ref/rrefattrib16471.html">shutdown command</a>.
   * @throws IOException if there is a problem with the deletion of the database files.
   * @throws InterruptedException never thrown but declared for compatibility with the parent class.
   */
  @Override
  @SuppressWarnings("checkstyle:EmptyBlock")
  public void stop() throws IOException, InterruptedException {
    super.stop();
    try (Connection c = DriverManager.getConnection(JdbcUrl.SHUTDOWN.of(getDbPath().toString()))) {
    } catch (SQLException e) {
      if (!isNormalShutdown(e)) {
        throw new IllegalStateException(e);
      }
    } finally {
      FileUtils.deleteDirectory(getDbPath().toFile());
    }
  }

  /**
   * Whether the exception corresponds to a normal shutdown of Derby.
   * @param e the exception to check.
   * @return Whether the exception corresponds to a normal shutdown of Derby.
   */
  private boolean isNormalShutdown(SQLException e) {
    return "08006".equals(e.getSQLState()) && e.getErrorCode() == 45000;
  }

  private enum JdbcUrl {
    CREATE {
      @Override
      String of(String dbPath) {
        return "jdbc:derby:memory:" + dbPath + ";create=true";
      }
    }, SHUTDOWN {
      @Override
      String of(String dbPath) {
        return "jdbc:derby:memory:" + dbPath + ";drop=true";
      }
    };

    abstract String of(String dbPath);
  }
}
