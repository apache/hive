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

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Covers utility functions that are used by LLAP code and depend on Hive constructs e.g. ql code.
 */
public final class LlapHiveUtils {

  public static final Logger LOG = LoggerFactory.getLogger(LlapHiveUtils.class);

  private LlapHiveUtils() {
    // Not to be used;
  }

  public static CacheTag getDbAndTableNameForMetrics(Path path, boolean includeParts,
        Map<Path, PartitionDesc> parts) {

    assert(parts != null);

    // Look for PartitionDesc instance matching our Path
    Path parentPath = path;
    PartitionDesc part = parts.get(parentPath);
    while (!parentPath.isRoot() && part == null) {
      parentPath = parentPath.getParent();
      part = parts.get(parentPath);
    }

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

  /**
   * Returns MapWork based what is serialized in the JobConf instance provided.
   * @param job
   * @return the MapWork instance. Might be null if missing.
   * @throws HiveException
   */
  public static MapWork findMapWork(JobConf job) throws HiveException {
    String inputName = job.get(Utilities.INPUT_NAME, null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing for input " + inputName);
    }
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

}
