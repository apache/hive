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

package org.apache.hadoop.hive.serde2;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

/**
 * ColumnProjectionUtils.
 *
 */
public final class ColumnProjectionUtils {

  public static final String READ_COLUMN_IDS_CONF_STR = "hive.io.file.readcolumn.ids";

  /**
   * Sets read columns' ids(start from zero) for RCFile's Reader. Once a column
   * is included in the list, RCFile's reader will not skip its value.
   * 
   */
  public static void setReadColumnIDs(Configuration conf, ArrayList<Integer> ids) {
    String id = toReadColumnIDString(ids);
    setReadColumnIDConf(conf, id);
  }

  /**
   * Sets read columns' ids(start from zero) for RCFile's Reader. Once a column
   * is included in the list, RCFile's reader will not skip its value.
   * 
   */
  public static void appendReadColumnIDs(Configuration conf,
      ArrayList<Integer> ids) {
    String id = toReadColumnIDString(ids);
    if (id != null) {
      String old = conf.get(READ_COLUMN_IDS_CONF_STR, null);
      String newConfStr = id;
      if (old != null) {
        newConfStr = newConfStr + StringUtils.COMMA_STR + old;
      }

      setReadColumnIDConf(conf, newConfStr);
    }
  }

  private static void setReadColumnIDConf(Configuration conf, String id) {
    if (id == null || id.length() <= 0) {
      conf.set(READ_COLUMN_IDS_CONF_STR, "");
      return;
    }

    conf.set(READ_COLUMN_IDS_CONF_STR, id);
  }

  private static String toReadColumnIDString(ArrayList<Integer> ids) {
    String id = null;
    if (ids != null) {
      for (int i = 0; i < ids.size(); i++) {
        if (i == 0) {
          id = "" + ids.get(i);
        } else {
          id = id + StringUtils.COMMA_STR + ids.get(i);
        }
      }
    }
    return id;
  }

  /**
   * Returns an array of column ids(start from zero) which is set in the given
   * parameter <tt>conf</tt>.
   */
  public static ArrayList<Integer> getReadColumnIDs(Configuration conf) {
    if (conf == null) {
      return new ArrayList<Integer>(0);
    }
    String skips = conf.get(READ_COLUMN_IDS_CONF_STR, "");
    String[] list = StringUtils.split(skips);
    ArrayList<Integer> result = new ArrayList<Integer>(list.length);
    for (String element : list) {
      // it may contain duplicates, remove duplicates
      Integer toAdd = Integer.parseInt(element);
      if (!result.contains(toAdd)) {
        result.add(toAdd);
      }
    }
    return result;
  }

  /**
   * Clears the read column ids set in the conf, and will read all columns.
   */
  public static void setFullyReadColumns(Configuration conf) {
    conf.set(READ_COLUMN_IDS_CONF_STR, "");
  }

  private ColumnProjectionUtils() {
    // prevent instantiation
  }

}
