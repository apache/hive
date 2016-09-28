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

package org.apache.hadoop.hive.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Hive Configuration utils
 */
@Private
public class HiveConfUtil {
  /**
   * Check if metastore is being used in embedded mode.
   * This utility function exists so that the logic for determining the mode is same
   * in HiveConf and HiveMetaStoreClient
   * @param msUri - metastore server uri
   * @return
   */
  public static boolean isEmbeddedMetaStore(String msUri) {
    return (msUri == null) ? true : msUri.trim().isEmpty();
  }

  /**
   * Dumps all HiveConf for debugging.  Convenient to dump state at process start up and log it
   * so that in later analysis the values of all variables is known
   */
  public static StringBuilder dumpConfig(HiveConf conf) {
    StringBuilder sb = new StringBuilder("START========\"HiveConf()\"========\n");
    sb.append("hiveDefaultUrl=").append(conf.getHiveDefaultLocation()).append('\n');
    sb.append("hiveSiteURL=").append(HiveConf.getHiveSiteLocation()).append('\n');
    sb.append("hiveServer2SiteUrl=").append(HiveConf.getHiveServer2SiteLocation()).append('\n');
    sb.append("hivemetastoreSiteUrl=").append(HiveConf.getMetastoreSiteLocation()).append('\n');
    dumpConfig(conf, sb);
    return sb.append("END========\"new HiveConf()\"========\n");
  }
  public static void dumpConfig(Configuration conf, StringBuilder sb) {
    Iterator<Map.Entry<String, String>> configIter = conf.iterator();
    List<Map.Entry<String, String>> configVals = new ArrayList<>();
    while(configIter.hasNext()) {
      configVals.add(configIter.next());
    }
    Collections.sort(configVals, new Comparator<Map.Entry<String, String>>() {
      @Override
      public int compare(Map.Entry<String, String> ent, Map.Entry<String, String> ent2) {
        return ent.getKey().compareTo(ent2.getKey());
      }
    });
    for(Map.Entry<String, String> entry : configVals) {
      //use get() to make sure variable substitution works
      if(entry.getKey().toLowerCase().contains("path")) {
        StringTokenizer st = new StringTokenizer(conf.get(entry.getKey()), File.pathSeparator);
        sb.append(entry.getKey()).append("=\n");
        while(st.hasMoreTokens()) {
          sb.append("    ").append(st.nextToken()).append(File.pathSeparator).append('\n');
        }
      }
      else {
        sb.append(entry.getKey()).append('=').append(conf.get(entry.getKey())).append('\n');
      }
    }
  }
}
