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

package org.apache.hadoop.hive.metastore;

import javax.jdo.PersistenceManager;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestPackageJdo {
  private static final String TMPDIR = System.getProperty("java.io.tmpdir", "target/tmp");
  private final Configuration conf = MetastoreConf.newMetastoreConf();

  private final String url1 = "jdbc:derby:;databaseName=" + TMPDIR + "/test_meta_mapping_db1;create=true";
  private final String url2 = "jdbc:derby:;databaseName=" + TMPDIR + "/test_meta_mapping_db2;create=true";
  private final String password = "mine";

  @Before
  public void setup() throws Exception {
    // Init the schema from script
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY, url1);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.PWD, password);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_USER_NAME, "APP");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_DRIVER,
        "org.apache.derby.iapi.jdbc.AutoloadedDriver");
    TestTxnDbUtil.prepDb(conf);

    // Init the schema from package.jdo
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY, url2);
    PersistenceManagerProvider.updatePmfProperties(conf);
    PersistenceManager pm = PersistenceManagerProvider.getPersistenceManager();
    Set<Class> classes = new HashSet<>();
    String packageName = "org/apache/hadoop/hive/metastore/model/";
    for (File mfile :
        new File(".", "src/main/java/" + packageName).listFiles()) {
      String fileName = mfile.getName();
      if (fileName.endsWith(".java") && !fileName.contains("FetchGroups")) {
        fileName = packageName.replace("/", ".") +
            fileName.substring(0, fileName.length() - 5);
        classes.add(getClass().getClassLoader().loadClass(fileName));
      }
    }
    for (Class clazz : classes) {
      try {
        pm.makePersistent(JavaUtils.newInstance(clazz, new Class[0], new Object[0]));
      } catch (Exception ignore) {
      }
    }
  }

  private static class ColumnInfo {
    final String columnName;
    final Object dataType;
    final Object columnSize;
    final Object nullable;

    public ColumnInfo(String columnName, Object dataType, Object columnSize, Object nullable) {
      this.columnName = columnName;
      this.dataType = dataType;
      this.columnSize = columnSize;
      this.nullable = nullable;
    }

    // Oops, we check only the column names here...
    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      ColumnInfo that = (ColumnInfo) o;
      return Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(columnName);
    }

    @Override
    public String toString() {
      return "columnName=" + columnName + ", dataType=" + dataType + ", columnSize=" + columnSize + ", nullable: " + nullable;
    }
  }

  @Test
  public void verifySchema() throws Exception {
    Connection connection2 = TestTxnDbUtil.getConnection(conf);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY, url1);
    Connection connection1 = TestTxnDbUtil.getConnection(conf);
    try (Connection conn1 = connection1; Connection conn2 = connection2) {
      Set<String> tables1 = getTableNames(conn1);
      Set<String> tables2 = getTableNames(conn2);
      Assert.assertFalse(tables1.isEmpty() || tables2.isEmpty());
      // We don't define the transaction table in package.jdo, iterator over table2,
      // check to see if there is any diff on the table schema
      for (String table : tables2) {
        ResultSet colRS1 = connection1.getMetaData().getColumns(null, "APP", table, null);
        ResultSet colRS2 = connection2.getMetaData().getColumns(null, "APP", table, null);
        Set<ColumnInfo> columnInfos1 = new HashSet<>();
        Set<ColumnInfo> columnInfos2 = new HashSet<>();
        while (colRS1.next() && colRS2.next()) {
          columnInfos1.add(new ColumnInfo(colRS1.getString("COLUMN_NAME"), colRS1.getObject("DATA_TYPE"),
              colRS1.getObject("COLUMN_SIZE"), colRS1.getString("IS_NULLABLE")));
          columnInfos2.add(new ColumnInfo(colRS2.getString("COLUMN_NAME"), colRS2.getObject("DATA_TYPE"),
              colRS2.getObject("COLUMN_SIZE"), colRS2.getString("IS_NULLABLE")));
        }
        assertEquals("Different columns found in the table: " + table, columnInfos1, columnInfos2);
        assertFalse(colRS1.next() || colRS2.next());
      }
    }
  }

  private Set<String> getTableNames(Connection connection) throws Exception {
    Set<String> tables = new HashSet<>();
    try (ResultSet rs2 = connection.getMetaData()
        .getTables(null, "APP", null, new String[] { "TABLE" })) {
      while (rs2.next()) {
        tables.add(rs2.getString("TABLE_NAME"));
      }
    }
    return tables;
  }
}
