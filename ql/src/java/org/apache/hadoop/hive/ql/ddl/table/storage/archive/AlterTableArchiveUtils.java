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

package org.apache.hadoop.hive.ql.ddl.table.storage.archive;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Utilities for archiving.
 */
final class AlterTableArchiveUtils {
  private AlterTableArchiveUtils() {
    throw new UnsupportedOperationException("ArchiveUtils should not be instantiated");
  }

  static final String ARCHIVE_NAME = "data.har";

  /**
   * Returns original partition of archived partition, null for unarchived one.
   */
  static String getOriginalLocation(Partition partition) {
    Map<String, String> params = partition.getParameters();
    return params.get(hive_metastoreConstants.ORIGINAL_LOCATION);
  }

  /**
   * Sets original location of partition which is to be archived.
   */
  static void setOriginalLocation(Partition partition, String loc) {
    Map<String, String> params = partition.getParameters();
    if (loc == null) {
      params.remove(hive_metastoreConstants.ORIGINAL_LOCATION);
    } else {
      params.put(hive_metastoreConstants.ORIGINAL_LOCATION, loc);
    }
  }

  /**
   * Checks in partition is in custom (not-standard) location.
   * @param table - table in which partition is
   * @param partition - partition
   * @return true if partition location is custom, false if it is standard
   */
  static boolean partitionInCustomLocation(Table table, Partition partition) throws HiveException {
    String subdir = null;
    try {
      subdir = Warehouse.makePartName(table.getPartCols(), partition.getValues());
    } catch (MetaException e) {
      throw new HiveException("Unable to get partition's directory", e);
    }

    Path tableDir = table.getDataLocation();
    if (tableDir == null) {
      throw new HiveException("Table has no location set");
    }

    String standardLocation = new Path(tableDir, subdir).toString();
    if (ArchiveUtils.isArchived(partition)) {
      return !getOriginalLocation(partition).equals(standardLocation);
    } else {
      return !partition.getLocation().equals(standardLocation);
    }
  }

  static Path getInterMediateDir(Path dir, Configuration conf, ConfVars suffixConfig) {
    String intermediateDirSuffix = HiveConf.getVar(conf, suffixConfig);
    return new Path(dir.getParent(), dir.getName() + intermediateDirSuffix);
  }

  static void deleteDir(Path dir, boolean shouldEnableCm, Configuration conf) throws HiveException {
    try {
      Warehouse wh = new Warehouse(conf);
      wh.deleteDir(dir, true, false, shouldEnableCm);
    } catch (MetaException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Sets archiving flag locally; it has to be pushed into metastore.
   * @param partition partition to set flag
   * @param state desired state of IS_ARCHIVED flag
   * @param level desired level for state == true, anything for false
   */
  static void setIsArchived(Partition partition, boolean state, int level) {
    Map<String, String> params = partition.getParameters();
    if (state) {
      params.put(hive_metastoreConstants.IS_ARCHIVED, "true");
      params.put(ArchiveUtils.ARCHIVING_LEVEL, Integer.toString(level));
    } else {
      params.remove(hive_metastoreConstants.IS_ARCHIVED);
      params.remove(ArchiveUtils.ARCHIVING_LEVEL);
    }
  }
}
