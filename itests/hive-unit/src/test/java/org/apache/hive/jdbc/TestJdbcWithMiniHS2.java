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

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

  public class TestJdbcWithMiniHS2 {
    private static MiniHS2 miniHS2 = null;
    private static Path dataFilePath;

    private Connection hs2Conn = null;

    @BeforeClass
    public static void beforeTest() throws Exception {
      Class.forName(MiniHS2.getJdbcDriverName());
      HiveConf conf = new HiveConf();
      miniHS2 = new MiniHS2(conf);
      String dataFileDir = conf.get("test.data.files").replace('\\', '/')
          .replace("c:", "");
      dataFilePath = new Path(dataFileDir, "kv1.txt");
    }

    @Before
    public void setUp() throws Exception {
      miniHS2.start();
      hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
      hs2Conn.createStatement().execute("set hive.support.concurrency = false");
    }

    @After
    public void tearDown() throws Exception {
      hs2Conn.close();
      miniHS2.stop();
    }

    @Test
    public void testConnection() throws Exception {
      String tableName = "testTab1";
      Statement stmt = hs2Conn.createStatement();

      // create table
      stmt.execute("DROP TABLE IF EXISTS " + tableName);
      stmt.execute("CREATE TABLE " + tableName
          + " (under_col INT COMMENT 'the under column', value STRING) COMMENT ' test table'");

      // load data
      stmt.execute("load data local inpath '"
          + dataFilePath.toString() + "' into table " + tableName);

      ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
      assertTrue(res.next());
      assertEquals("val_238", res.getString(2));
      res.close();
      stmt.close();
    }
}
