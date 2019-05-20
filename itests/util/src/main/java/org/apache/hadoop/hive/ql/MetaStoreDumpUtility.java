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

package org.apache.hadoop.hive.ql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class which can load an existing metastore dump.
 *
 * This can be used to check planning on a large scale database.
 */
public class MetaStoreDumpUtility {

  static final Logger LOG = LoggerFactory.getLogger(MetaStoreDumpUtility.class);

  public static void setupMetaStoreTableColumnStatsFor30TBTPCDSWorkload(HiveConf conf, String tmpBaseDir) {
    Connection conn = null;

    try {
      Properties props = new Properties(); // connection properties
      props.put("user", conf.get("javax.jdo.option.ConnectionUserName"));
      props.put("password", conf.get("javax.jdo.option.ConnectionPassword"));
      String url = conf.get("javax.jdo.option.ConnectionURL");
      conn = DriverManager.getConnection(url, props);
      ResultSet rs = null;
      Statement s = conn.createStatement();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Connected to metastore database ");
      }

      String mdbPath = HiveTestEnvSetup.HIVE_ROOT + "/data/files/tpcds-perf/metastore_export/";

      // Setup the table column stats
      BufferedReader br = new BufferedReader(
          new FileReader(
              new File(HiveTestEnvSetup.HIVE_ROOT + "/metastore/scripts/upgrade/derby/022-HIVE-11107.derby.sql")));
      String command;

      s.execute("DROP TABLE APP.TABLE_PARAMS");
      s.execute("DROP TABLE APP.TAB_COL_STATS");
      // Create the column stats table
      while ((command = br.readLine()) != null) {
        if (!command.endsWith(";")) {
          continue;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to run command : " + command);
        }
        PreparedStatement psCommand = conn.prepareStatement(command.substring(0, command.length() - 1));
        psCommand.execute();
        psCommand.close();
        if (LOG.isDebugEnabled()) {
          LOG.debug("successfully completed " + command);
        }
      }
      br.close();

      java.nio.file.Path tabColStatsCsv = FileSystems.getDefault().getPath(mdbPath, "csv" ,"TAB_COL_STATS.txt.bz2");
      java.nio.file.Path tabParamsCsv = FileSystems.getDefault().getPath(mdbPath, "csv", "TABLE_PARAMS.txt.bz2");

      // Set up the foreign key constraints properly in the TAB_COL_STATS data
      java.nio.file.Path tmpFileLoc1 = FileSystems.getDefault().getPath(tmpBaseDir, "TAB_COL_STATS.txt");
      java.nio.file.Path tmpFileLoc2 = FileSystems.getDefault().getPath(tmpBaseDir, "TABLE_PARAMS.txt");

      class MyComp implements Comparator<String> {
        @Override
        public int compare(String str1, String str2) {
          if (str2.length() != str1.length()) {
            return str2.length() - str1.length();
          }
          return str1.compareTo(str2);
        }
      }

      final SortedMap<String, Integer> tableNameToID = new TreeMap<String, Integer>(new MyComp());

     rs = s.executeQuery("SELECT * FROM APP.TBLS");
      while(rs.next()) {
        String tblName = rs.getString("TBL_NAME");
        Integer tblId = rs.getInt("TBL_ID");
        tableNameToID.put(tblName, tblId);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Resultset : " + tblName + " | " + tblId);
        }
      }

      final Map<String, Map<String, String>> data = new HashMap<>();
      rs = s.executeQuery("select TBLS.TBL_NAME, a.COLUMN_NAME, a.TYPE_NAME from  "
          + "(select COLUMN_NAME, TYPE_NAME, SDS.SD_ID from APP.COLUMNS_V2 join APP.SDS on SDS.CD_ID = COLUMNS_V2.CD_ID) a"
          + " join APP.TBLS on  TBLS.SD_ID = a.SD_ID");
      while (rs.next()) {
        String tblName = rs.getString(1);
        String colName = rs.getString(2);
        String typeName = rs.getString(3);
        Map<String, String> cols = data.get(tblName);
        if (null == cols) {
          cols = new HashMap<>();
        }
        cols.put(colName, typeName);
        data.put(tblName, cols);
      }

      BufferedReader reader = new BufferedReader(new InputStreamReader(
        new BZip2CompressorInputStream(Files.newInputStream(tabColStatsCsv, StandardOpenOption.READ))));

      Stream<String> replaced = reader.lines().parallel().map(str-> {
        String[] splits = str.split(",");
        String tblName = splits[0];
        String colName = splits[1];
        Integer tblID = tableNameToID.get(tblName);
        StringBuilder sb = new StringBuilder("default@"+tblName + "@" + colName + "@" + data.get(tblName).get(colName)+"@");
        for (int i = 2; i < splits.length; i++) {
          sb.append(splits[i]+"@");
        }
        // Add tbl_id and empty bitvector
        return sb.append(tblID).append("@").toString();
        });

      Files.write(tmpFileLoc1, (Iterable<String>)replaced::iterator);
      replaced.close();
      reader.close();

      BufferedReader reader2 = new BufferedReader(new InputStreamReader(
          new BZip2CompressorInputStream(Files.newInputStream(tabParamsCsv, StandardOpenOption.READ))));
      final Map<String,String> colStats = new ConcurrentHashMap<>();
      Stream<String> replacedStream = reader2.lines().parallel().map(str-> {
        String[] splits = str.split("_@");
        String tblName = splits[0];
        Integer tblId = tableNameToID.get(tblName);
        Map<String,String> cols = data.get(tblName);
        StringBuilder sb = new StringBuilder();
        sb.append("{\"COLUMN_STATS\":{");
        for (String colName : cols.keySet()) {
          sb.append("\""+colName+"\":\"true\",");
        }
        sb.append("},\"BASIC_STATS\":\"true\"}");
        colStats.put(tblId.toString(), sb.toString());

        return  tblId.toString() + "@" + splits[1];
      });

      Files.write(tmpFileLoc2, (Iterable<String>)replacedStream::iterator);
      Files.write(tmpFileLoc2, (Iterable<String>)colStats.entrySet().stream()
        .map(map->map.getKey()+"@COLUMN_STATS_ACCURATE@"+map.getValue())::iterator, StandardOpenOption.APPEND);

      replacedStream.close();
      reader2.close();
      // Load the column stats and table params with 30 TB scale
      String importStatement1 =  "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE(null, '" + "TAB_COL_STATS" +
        "', '" + tmpFileLoc1.toAbsolutePath().toString() +
        "', '@', null, 'UTF-8', 1)";
      String importStatement2 =  "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE(null, '" + "TABLE_PARAMS" +
        "', '" + tmpFileLoc2.toAbsolutePath().toString() +
        "', '@', null, 'UTF-8', 1)";

      PreparedStatement psImport1 = conn.prepareStatement(importStatement1);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute : " + importStatement1);
      }
      psImport1.execute();
      psImport1.close();
      if (LOG.isDebugEnabled()) {
        LOG.debug("successfully completed " + importStatement1);
      }
      PreparedStatement psImport2 = conn.prepareStatement(importStatement2);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute : " + importStatement2);
      }
      psImport2.execute();
      psImport2.close();
      if (LOG.isDebugEnabled()) {
        LOG.debug("successfully completed " + importStatement2);
      }

      s.execute("ALTER TABLE APP.TAB_COL_STATS ADD COLUMN CAT_NAME VARCHAR(256)");
      s.execute("update APP.TAB_COL_STATS set CAT_NAME = '" + Warehouse.DEFAULT_CATALOG_NAME + "'");

      s.close();

      conn.close();

    } catch (Exception e) {
      throw new RuntimeException("error while loading tpcds metastore dump", e);
    }
  }

}
