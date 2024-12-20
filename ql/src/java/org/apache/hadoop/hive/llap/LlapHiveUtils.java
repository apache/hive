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
package org.apache.hadoop.hive.llap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;

/**
 * Covers utility functions that are used by LLAP code and depend on Hive constructs e.g. ql code.
 */
public final class LlapHiveUtils {

  public static final Logger LOG = LoggerFactory.getLogger(LlapHiveUtils.class);

  private LlapHiveUtils() {
    // Not to be used;
  }

  /**
   * Takes a Path and looks up the PartitionDesc instance associated with it in a map of Path-&gt;PartitionDesc entries.
   * If it is not found (e.g. Path denotes a partition path, but map contains table level instances only) we will try
   * to do the same with the parent of this path, traversing up until there's a match, if any.
   * @param path the absolute path used for the look up
   * @param partitionDescMap the map
   * @return the PartitionDesc instance if found, null if not found
   */
  public static PartitionDesc partitionDescForPath(Path path, Map<Path, PartitionDesc> partitionDescMap) {
    assert(partitionDescMap != null);

    // Look for PartitionDesc instance matching our Path
    Path parentPath = path;
    PartitionDesc part = partitionDescMap.get(parentPath);
    while (!parentPath.isRoot() && part == null) {
      parentPath = parentPath.getParent();
      part = partitionDescMap.get(parentPath);
    }
    return part;
  }

  public static CacheTag getDbAndTableNameForMetrics(Path path, boolean includeParts,
      PartitionDesc part) {

    // Fallback to legacy cache tag creation logic.
    if (part == null) {
      return CacheTag.build(LlapUtil.getDbAndTableNameForMetrics(path, includeParts));
    }

    if (!includeParts || !part.isPartitioned()) {
      return CacheTag.build(part.getTableName());
    } else {
      return CacheTag.build(part.getTableName(), part.getPartSpec());
    }
  }

  public static int getSchemaHash(PartitionDesc part) {
    if (part == null) {
      return SchemaAwareCacheKey.NO_SCHEMA_HASH;
    } else {
      Object columnTypes = part.getProperties().get(META_TABLE_COLUMN_TYPES);
      if (columnTypes != null) {
        return columnTypes.toString().hashCode();
      } else {
        return SchemaAwareCacheKey.NO_SCHEMA_HASH;
      }
    }
  }

  /**
   * Returns MapWork based what is serialized in the JobConf instance provided.
   * @param job
   * @return the MapWork instance. Might be null if missing.
   */
  public static MapWork findMapWork(JobConf job) {
    String inputName = job.get(Utilities.INPUT_NAME, null);
    LOG.debug("Initializing for input {}", inputName);

    String prefixes = job.get(DagUtils.TEZ_MERGE_WORK_FILE_PREFIXES);
    if (prefixes != null && !StringUtils.isBlank(prefixes)) {
      // Currently SMB is broken, so we cannot check if it's  compatible with IO elevator.
      // So, we don't use the below code that would get the correct MapWork. See HIVE-16985.
      return null;
    }

    BaseWork work = null;
    // HIVE-16985: try to find the fake merge work for SMB join, that is really another MapWork.
    if (inputName != null) {
      if (prefixes == null ||
              !Lists.newArrayList(prefixes.split(",")).contains(inputName)) {
        inputName = null;
      }
    }
    if (inputName != null) {
      work = Utilities.getMergeWork(job, inputName);
    }

    if (!(work instanceof MapWork)) {
      work = Utilities.getMapWork(job);
    }
    return (MapWork) work;
  }

  public static void throwIfCacheOnlyRead(boolean isCacheOnlyRead) throws IOException {
    if (isCacheOnlyRead) {
      throw new IOException("LLAP cache miss happened while reading. Aborting query as cache only reading is set. " +
          "Set " + HiveConf.ConfVars.LLAP_IO_CACHE_ONLY.varname + " to false and repeat query if this was unintended.");
    }
  }

  public static boolean isLlapMode(Configuration conf) {
    return "llap".equalsIgnoreCase(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_MODE));
  }

  /**
   * Determines the fileID for the given path using the FileSystem type provided while considering daemon configuration.
   * Invokes HdfsUtils.getFileId(), the resulting file ID can be of types Long (inode) or SyntheticFileId depending
   * on the FS type and the actual daemon configuration.
   * Can be costly on cloud file systems.
   * @param fs FileSystem type
   * @param path Path associated to this file
   * @param daemonConf Llap daemon configuration
   * @return the generated fileID, can be null in special cases (e.g. conf disallows synthetic ID on a non-HDFS FS)
   * @throws IOException
   */
  public static Object createFileIdUsingFS(FileSystem fs, Path path, Configuration daemonConf) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Will invoke HdfsUtils.getFileId - this is costly on cloud file systems. " +
          "Turn on TRACE level logging to show call trace.");
      if (LOG.isTraceEnabled()) {
        LOG.trace(Arrays.deepToString(Thread.currentThread().getStackTrace()));
      }
    }
    boolean allowSynthetic = HiveConf.getBoolVar(daemonConf, HiveConf.ConfVars.LLAP_CACHE_ALLOW_SYNTHETIC_FILEID);
    boolean checkDefaultFs = HiveConf.getBoolVar(daemonConf, HiveConf.ConfVars.LLAP_CACHE_DEFAULT_FS_FILE_ID);
    boolean forceSynthetic = !HiveConf.getBoolVar(daemonConf, HiveConf.ConfVars.LLAP_IO_USE_FILEID_PATH);

    return HdfsUtils.getFileId(fs, path, allowSynthetic, checkDefaultFs, forceSynthetic);
  }

}
