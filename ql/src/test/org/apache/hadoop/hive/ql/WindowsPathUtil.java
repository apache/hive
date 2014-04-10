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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.Shell;

public class WindowsPathUtil {

  public static void convertPathsFromWindowsToHdfs(HiveConf conf){
    String orgWarehouseDir = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
    conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, getHdfsUriString(orgWarehouseDir));

    String orgTestTempDir = System.getProperty("test.tmp.dir");
    System.setProperty("test.tmp.dir", getHdfsUriString(orgTestTempDir));

    String orgScratchDir = conf.getVar(HiveConf.ConfVars.SCRATCHDIR);
    conf.setVar(HiveConf.ConfVars.SCRATCHDIR, getHdfsUriString(orgScratchDir));
  }

  private static String getHdfsUriString(String uriStr) {
    assert uriStr != null;
    if(Shell.WINDOWS) {
      // If the URI conversion is from Windows to HDFS then replace the '\' with '/'
      // and remove the windows single drive letter & colon from absolute path.
      return uriStr.replace('\\', '/')
        .replaceFirst("/[c-zC-Z]:", "/")
        .replaceFirst("^[c-zC-Z]:", "");
    }
    return uriStr;
  }
}
