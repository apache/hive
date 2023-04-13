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

package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WarehouseUtils {
  private WarehouseUtils() {
  }

  public static final String DEFAULT_CATALOG_NAME = "hive";
  public static final String DEFAULT_CATALOG_COMMENT = "Default catalog, for Hive";
  public static final String DEFAULT_DATABASE_NAME = "default";
  public static final String DEFAULT_DATABASE_COMMENT = "Default Hive database";
  public static final String DEFAULT_SERIALIZATION_FORMAT = "1";
  public static final String DATABASE_WAREHOUSE_SUFFIX = ".db";

  public static String escapePathName(String path) {
    return FileUtils.escapePathName(path);
  }

  /**
   * Makes a partition name from a specification
   * @param spec The partition specification, key and value pairs.
   * @param addTrailingSeperator If true, adds a trailing separator e.g. 'ds=1/'.
   * @param dynamic If true, create a dynamic partition name.
   * @return partition name
   * @throws MetaException
   */
  public static String makePartNameUtil(Map<String, String> spec, boolean addTrailingSeperator, boolean dynamic)
      throws MetaException {
    StringBuilder suffixBuf = new StringBuilder();
    int i = 0;
    for (Map.Entry<String, String> e : spec.entrySet()) {
      // Throw an exception if it is not a dynamic partition.
      if (e.getValue() == null || e.getValue().length() == 0) {
        if (dynamic) {
          break;
        } else {
          throw new MetaException("Partition spec is incorrect. " + spec);
        }
      }

      if (i > 0) {
        suffixBuf.append(Path.SEPARATOR);
      }
      suffixBuf.append(escapePathName(e.getKey()));
      suffixBuf.append('=');
      suffixBuf.append(escapePathName(e.getValue()));
      i++;
    }

    if (addTrailingSeperator && i > 0) {
      suffixBuf.append(Path.SEPARATOR);
    }

    return suffixBuf.toString();
  }

  /**
   * Makes a partition name from a specification
   * @param spec
   * @param addTrailingSeperator if true, adds a trailing separator e.g. 'ds=1/'
   * @return partition name
   * @throws MetaException
   */
  public static String makePartName(Map<String, String> spec, boolean addTrailingSeperator) throws MetaException {
    return makePartNameUtil(spec, addTrailingSeperator, false);
  }

  /**
   * Given a dynamic partition specification, return the path corresponding to the
   * static part of partition specification. This is basically similar to makePartName
   * but we get rid of MetaException since it is not serializable.
   * @param spec
   * @return string representation of the static part of the partition specification.
   */
  public static String makeDynamicPartName(Map<String, String> spec) {
    String partName = null;
    try {
      partName = makePartNameUtil(spec, true, true);
    } catch (MetaException e) {
      // This exception is not thrown when dynamic=true. This is a Noop and
      // can be ignored.
    }
    return partName;
  }

  /**
   * Given a dynamic partition specification, return the path corresponding to the
   * static part of partition specification. This is basically similar to makePartName
   * but we get rid of MetaException since it is not serializable. This method skips
   * the trailing path seperator also.
   *
   * @param spec
   * @return string representation of the static part of the partition specification.
   */
  public static String makeDynamicPartNameNoTrailingSeperator(Map<String, String> spec) {
    String partName = null;
    try {
      partName = makePartNameUtil(spec, false, true);
    } catch (MetaException e) {
      // This exception is not thrown when dynamic=true. This is a Noop and
      // can be ignored.
    }
    return partName;
  }

  public static String makePartName(List<FieldSchema> partCols, List<String> vals) throws MetaException {
    return makePartName(partCols, vals, null);
  }

  /**
   * Makes a valid partition name.
   * @param partCols The partition columns
   * @param vals The partition values
   * @param defaultStr
   *    The default name given to a partition value if the respective value is empty or null.
   * @return An escaped, valid partition name.
   * @throws MetaException
   */
  public static String makePartName(List<FieldSchema> partCols, List<String> vals, String defaultStr)
      throws MetaException {
    if ((partCols.size() != vals.size()) || (partCols.size() == 0)) {
      StringBuilder errorStrBuilder = new StringBuilder("Invalid partition key & values; keys [");
      for (FieldSchema fs : partCols) {
        errorStrBuilder.append(fs.getName()).append(", ");
      }
      errorStrBuilder.append("], values [");
      for (String val : vals) {
        errorStrBuilder.append(val).append(", ");
      }
      throw new MetaException(errorStrBuilder.append("]").toString());
    }
    List<String> colNames = new ArrayList<>();
    for (FieldSchema col : partCols) {
      colNames.add(col.getName());
    }
    return FileUtils.makePartName(colNames, vals, defaultStr);
  }

  /**
   * Given a partition specification, return the path corresponding to the
   * partition spec. By default, the specification does not include dynamic partitions.
   * @param spec
   * @return string representation of the partition specification.
   * @throws MetaException
   */
  public static String makePartPath(Map<String, String> spec)
      throws MetaException {
    return makePartName(spec, true);
  }
}
