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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.data;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.MiniCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for Other Data Testers
 */
public class HCatDataCheckUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HCatDataCheckUtil.class);

  public static IDriver instantiateDriver(MiniCluster cluster) {
    HiveConf hiveConf = new HiveConf(HCatDataCheckUtil.class);
    for (Entry e : cluster.getProperties().entrySet()) {
      hiveConf.set(e.getKey().toString(), e.getValue().toString());
    }
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    LOG.debug("Hive conf : {}", hiveConf.getAllProperties());
    IDriver driver = DriverFactory.newDriver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));
    return driver;
  }

  public static void generateDataFile(MiniCluster cluster, String fileName) throws IOException {
    MiniCluster.deleteFile(cluster, fileName);
    String[] input = new String[50];
    for (int i = 0; i < 50; i++) {
      input[i] = (i % 5) + "\t" + i + "\t" + "_S" + i + "S_";
    }
    MiniCluster.createInputFile(cluster, fileName, input);
  }

  public static void createTable(IDriver driver, String tableName, String createTableArgs) throws IOException {
    String createTable = "create table " + tableName + createTableArgs;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table. [" + createTable + "], return code from hive driver : [" + retCode + "]");
    }
  }

  public static void dropTable(IDriver driver, String tablename) throws IOException {
    driver.run("drop table if exists " + tablename);
  }

  public static ArrayList<String> formattedRun(IDriver driver, String name, String selectCmd) throws IOException {
    driver.run(selectCmd);
    ArrayList<String> src_values = new ArrayList<String>();
    driver.getResults(src_values);
    LOG.info("{} : {}", name, src_values);
    return src_values;
  }


  public static boolean recordsEqual(HCatRecord first, HCatRecord second) {
    return recordsEqual(first, second, null);
  }
  public static boolean recordsEqual(HCatRecord first, HCatRecord second,
                                     StringBuilder debugDetail) {
    return (compareRecords(first, second, debugDetail) == 0);
  }

  public static int compareRecords(HCatRecord first, HCatRecord second) {
    return compareRecords(first, second, null);
  }
  public static int compareRecords(HCatRecord first, HCatRecord second,
                                   StringBuilder debugDetail) {
    return compareRecordContents(first.getAll(), second.getAll(), debugDetail);
  }

  public static int compareRecordContents(List<Object> first, List<Object> second,
                                          StringBuilder debugDetail) {
    int mySz = first.size();
    int urSz = second.size();
    if (mySz != urSz) {
      return mySz - urSz;
    } else {
      for (int i = 0; i < first.size(); i++) {
        int c = DataType.compare(first.get(i), second.get(i));
        if (c != 0) {
          if(debugDetail != null) {
            String msg = "first.get(" + i + "}='" + first.get(i) + "' second.get(" +
                    i + ")='" + second.get(i) + "' compared as " + c + "\n" +
            "Types 1st/2nd=" + DataType.findType(first.get(i)) + "/" +DataType.findType(
                    second.get(i)) + '\n' +
                    "first='" + first.get(i) + "' second='" + second.get(i) + "'";
            if(first.get(i) instanceof Date) {
              msg += "\n((Date)first.get(i)).getTime()=" + ((Date)first.get(i)).getTime();
            }
            if(second.get(i) instanceof Date) {
              msg += "\n((Date)second.get(i)).getTime()=" + ((Date)second.get(i)).getTime();
            }

            debugDetail.append(msg);
            throw new RuntimeException(debugDetail.toString());
          }
          return c;
        }
      }
      return 0;
    }
  }


}
