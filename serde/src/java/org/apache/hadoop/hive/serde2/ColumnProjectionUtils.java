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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * ColumnProjectionUtils.
 *
 */
public final class ColumnProjectionUtils {

  public static final String READ_COLUMN_IDS_CONF_STR = "hive.io.file.readcolumn.ids";
  public static final String READ_ALL_COLUMNS = "hive.io.file.read.all.columns";
  public static final String READ_COLUMN_NAMES_CONF_STR = "hive.io.file.readcolumn.names";
  private static final String READ_COLUMN_IDS_CONF_STR_DEFAULT = "";
  private static final boolean READ_ALL_COLUMNS_DEFAULT = true;
  private static final Joiner CSV_JOINER = Joiner.on(",").skipNulls();

  /**
   * @deprecated for backwards compatibility with <= 0.12, use setReadAllColumns
   */
  @Deprecated
  public static void setFullyReadColumns(Configuration conf) {
    setReadAllColumns(conf);
  }

  /**
   * @deprecated for backwards compatibility with <= 0.12, use setReadAllColumns
   * and appendReadColumns
   */
  @Deprecated
  public static void setReadColumnIDs(Configuration conf, List<Integer> ids) {
    setReadColumnIDConf(conf, READ_COLUMN_IDS_CONF_STR_DEFAULT);
    appendReadColumns(conf, ids);
  }

  /**
   * @deprecated for backwards compatibility with <= 0.12, use appendReadColumns
   */
  @Deprecated
  public static void appendReadColumnIDs(Configuration conf, List<Integer> ids) {
    appendReadColumns(conf, ids);
  }



  /**
   * Sets the <em>READ_ALL_COLUMNS</em> flag and removes any previously
   * set column ids.
   */
  public static void setReadAllColumns(Configuration conf) {
    conf.setBoolean(READ_ALL_COLUMNS, true);
    setReadColumnIDConf(conf, READ_COLUMN_IDS_CONF_STR_DEFAULT);
  }

  /**
   * Returns the <em>READ_ALL_COLUMNS</em> columns flag.
   */
  public static boolean isReadAllColumns(Configuration conf) {
    return conf.getBoolean(READ_ALL_COLUMNS, READ_ALL_COLUMNS_DEFAULT);
  }

  /**
   * Appends read columns' ids (start from zero). Once a column
   * is included in the list, a underlying record reader of a columnar file format
   * (e.g. RCFile and ORC) can know what columns are needed.
   */
  public static void appendReadColumns(Configuration conf, List<Integer> ids) {
    String id = toReadColumnIDString(ids);
    String old = conf.get(READ_COLUMN_IDS_CONF_STR, null);
    String newConfStr = id;
    if (old != null) {
      newConfStr = newConfStr + StringUtils.COMMA_STR + old;
    }
    setReadColumnIDConf(conf, newConfStr);
    // Set READ_ALL_COLUMNS to false
    conf.setBoolean(READ_ALL_COLUMNS, false);
  }

  public static void appendReadColumns(
      Configuration conf, List<Integer> ids, List<String> names) {
    appendReadColumns(conf, ids);
    appendReadColumnNames(conf, names);
  }

  public static void appendReadColumns(
      StringBuilder readColumnsBuffer, StringBuilder readColumnNamesBuffer, List<Integer> ids,
      List<String> names) {
    CSV_JOINER.appendTo(readColumnsBuffer, ids);
    CSV_JOINER.appendTo(readColumnNamesBuffer, names);
  }

  /**
   * Returns an array of column ids(start from zero) which is set in the given
   * parameter <tt>conf</tt>.
   */
  public static List<Integer> getReadColumnIDs(Configuration conf) {
    String skips = conf.get(READ_COLUMN_IDS_CONF_STR, READ_COLUMN_IDS_CONF_STR_DEFAULT);
    String[] list = StringUtils.split(skips);
    List<Integer> result = new ArrayList<Integer>(list.length);
    for (String element : list) {
      // it may contain duplicates, remove duplicates
      Integer toAdd = Integer.parseInt(element);
      if (!result.contains(toAdd)) {
        result.add(toAdd);
      }
    }
    return result;
  }

  public static List<String> getReadColumnNames(Configuration conf) {
    String colNames = conf.get(READ_COLUMN_NAMES_CONF_STR, READ_COLUMN_IDS_CONF_STR_DEFAULT);
    if (colNames != null && !colNames.isEmpty()) {
      return Arrays.asList(colNames.split(","));
    }
    return Lists.newArrayList();
  }

  private static void setReadColumnIDConf(Configuration conf, String id) {
    if (id.trim().isEmpty()) {
      conf.set(READ_COLUMN_IDS_CONF_STR, READ_COLUMN_IDS_CONF_STR_DEFAULT);
    } else {
      conf.set(READ_COLUMN_IDS_CONF_STR, id);
    }
  }

  private static void appendReadColumnNames(Configuration conf, List<String> cols) {
    String old = conf.get(READ_COLUMN_NAMES_CONF_STR, "");
    StringBuilder result = new StringBuilder(old);
    boolean first = old.isEmpty();
    for(String col: cols) {
      if (first) {
        first = false;
      } else {
        result.append(',');
      }
      result.append(col);
    }
    conf.set(READ_COLUMN_NAMES_CONF_STR, result.toString());
  }

  private static String toReadColumnIDString(List<Integer> ids) {
    String id = "";
    for (int i = 0; i < ids.size(); i++) {
      if (i == 0) {
        id = id + ids.get(i);
      } else {
        id = id + StringUtils.COMMA_STR + ids.get(i);
      }
    }
    return id;
  }

  private ColumnProjectionUtils() {
    // prevent instantiation
  }

}
