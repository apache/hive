/*
 *  Copyright (c) 2002,2003,2004,2005 Marc Prud'hommeaux
 *  All rights reserved.
 *
 *
 *  Redistribution and use in source and binary forms,
 *  with or without modification, are permitted provided
 *  that the following conditions are met:
 *
 *  Redistributions of source code must retain the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer.
 *  Redistributions in binary form must reproduce the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer in the documentation and/or other materials
 *  provided with the distribution.
 *  Neither the name of the <ORGANIZATION> nor the names
 *  of its contributors may be used to endorse or promote
 *  products derived from this software without specific
 *  prior written permission.
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS
 *  AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 *  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 *  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 *  OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 *  IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 *  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *  This software is hosted by SourceForge.
 *  SourceForge is a trademark of VA Linux Systems, Inc.
 */

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * The license above originally appeared in src/sqlline/SqlLine.java
 * http://sqlline.sourceforge.net/
 */
package org.apache.hive.beeline;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import jline.ArgumentCompletor;
import jline.Completor;

class DatabaseConnection {
  private final BeeLine beeLine;
  private Connection connection;
  private DatabaseMetaData meta;
  private final String driver;
  private final String url;
  private final String username;
  private final String password;
  private Schema schema = null;
  private Completor sqlCompletor = null;


  public DatabaseConnection(BeeLine beeLine, String driver, String url,
      String username, String password) throws SQLException {
    this.beeLine = beeLine;
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
  }


  @Override
  public String toString() {
    return getUrl() + "";
  }


  void setCompletions(boolean skipmeta) throws SQLException, IOException {
    final String extraNameCharacters =
        getDatabaseMetaData() == null || getDatabaseMetaData().getExtraNameCharacters() == null ? ""
            : getDatabaseMetaData().getExtraNameCharacters();

    // setup the completor for the database
    sqlCompletor = new ArgumentCompletor(
        new SQLCompletor(beeLine, skipmeta),
        new ArgumentCompletor.AbstractArgumentDelimiter() {
          // delimiters for SQL statements are any
          // non-letter-or-number characters, except
          // underscore and characters that are specified
          // by the database to be valid name identifiers.
          @Override
          public boolean isDelimiterChar(String buf, int pos) {
            char c = buf.charAt(pos);
            if (Character.isWhitespace(c)) {
              return true;
            }
            return !(Character.isLetterOrDigit(c))
                && c != '_'
                && extraNameCharacters.indexOf(c) == -1;
          }
        });

    // not all argument elements need to hold true
    ((ArgumentCompletor) sqlCompletor).setStrict(false);
  }


  /**
   * Connection to the specified data source.
   *
   * @param driver
   *          the driver class
   * @param url
   *          the connection URL
   * @param username
   *          the username
   * @param password
   *          the password
   */
  boolean connect() throws SQLException {
    try {
      if (driver != null && driver.length() != 0) {
        Class.forName(driver);
      }
    } catch (ClassNotFoundException cnfe) {
      return beeLine.error(cnfe);
    }

    boolean foundDriver = false;
    try {
      foundDriver = DriverManager.getDriver(getUrl()) != null;
    } catch (Exception e) {
    }

    try {
      close();
    } catch (Exception e) {
      return beeLine.error(e);
    }

    setConnection(DriverManager.getConnection(getUrl(), username, password));
    setDatabaseMetaData(getConnection().getMetaData());

    try {
      beeLine.info(beeLine.loc("connected", new Object[] {
          getDatabaseMetaData().getDatabaseProductName(),
          getDatabaseMetaData().getDatabaseProductVersion()}));
    } catch (Exception e) {
      beeLine.handleException(e);
    }

    try {
      beeLine.info(beeLine.loc("driver", new Object[] {
          getDatabaseMetaData().getDriverName(),
          getDatabaseMetaData().getDriverVersion()}));
    } catch (Exception e) {
      beeLine.handleException(e);
    }

    try {
      getConnection().setAutoCommit(beeLine.getOpts().getAutoCommit());
      // TODO: Setting autocommit should not generate an exception as long as it is set to false
      // beeLine.autocommitStatus(getConnection());
    } catch (Exception e) {
      beeLine.handleException(e);
    }

    try {
      beeLine.getCommands().isolation("isolation: " + beeLine.getOpts().getIsolation());
    } catch (Exception e) {
      beeLine.handleException(e);
    }

    return true;
  }


  public Connection getConnection() throws SQLException {
    if (connection != null) {
      return connection;
    }
    connect();
    return connection;
  }


  public void reconnect() throws Exception {
    close();
    getConnection();
  }


  public void close() {
    try {
      try {
        if (connection != null && !connection.isClosed()) {
          beeLine.output(beeLine.loc("closing", connection));
          connection.close();
        }
      } catch (Exception e) {
        beeLine.handleException(e);
      }
    } finally {
      setConnection(null);
      setDatabaseMetaData(null);
    }
  }


  public String[] getTableNames(boolean force) {
    Schema.Table[] t = getSchema().getTables();
    Set<String> names = new TreeSet<String>();
    for (int i = 0; t != null && i < t.length; i++) {
      names.add(t[i].getName());
    }
    return names.toArray(new String[names.size()]);
  }

  Schema getSchema() {
    if (schema == null) {
      schema = new Schema();
    }
    return schema;
  }

  void setConnection(Connection connection) {
    this.connection = connection;
  }

  DatabaseMetaData getDatabaseMetaData() {
    return meta;
  }

  void setDatabaseMetaData(DatabaseMetaData meta) {
    this.meta = meta;
  }

  String getUrl() {
    return url;
  }

  Completor getSQLCompletor() {
    return sqlCompletor;
  }

  class Schema {
    private Table[] tables = null;

    Table[] getTables() {
      if (tables != null) {
        return tables;
      }

      List<Table> tnames = new LinkedList<Table>();

      try {
        ResultSet rs = getDatabaseMetaData().getTables(getConnection().getCatalog(),
            null, "%", new String[] {"TABLE"});
        try {
          while (rs.next()) {
            tnames.add(new Table(rs.getString("TABLE_NAME")));
          }
        } finally {
          try {
            rs.close();
          } catch (Exception e) {
          }
        }
      } catch (Throwable t) {
      }
      return tables = tnames.toArray(new Table[0]);
    }

    Table getTable(String name) {
      Table[] t = getTables();
      for (int i = 0; t != null && i < t.length; i++) {
        if (name.equalsIgnoreCase(t[i].getName())) {
          return t[i];
        }
      }
      return null;
    }

    class Table {
      final String name;
      Column[] columns;

      public Table(String name) {
        this.name = name;
      }


      public String getName() {
        return name;
      }

      class Column {
        final String name;
        boolean isPrimaryKey;

        public Column(String name) {
          this.name = name;
        }
      }
    }
  }
}