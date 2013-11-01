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

package org.apache.hadoop.hive.ql.io.rcfile.stats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.RCFile.KeyBuffer;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileKeyBufferWrapper;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileValueBufferWrapper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.shims.CombineHiveKey;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * PartialScanMapper.
 *
 * It reads through block header and aggregates statistics at the end.
 *
 * https://issues.apache.org/jira/browse/HIVE-3958
 */
@SuppressWarnings("deprecation")
public class PartialScanMapper extends MapReduceBase implements
    Mapper<Object, RCFileValueBufferWrapper, Object, Object> {

  private JobConf jc;
  private String statsAggKeyPrefix;
  private long uncompressedFileSize = 0;
  private long rowNo = 0;
  private boolean exception = false;

  public final static Log LOG = LogFactory.getLog("PartialScanMapper");

  public PartialScanMapper() {
  }

  @Override
  public void configure(JobConf job) {
    jc = job;
    statsAggKeyPrefix = HiveConf.getVar(job,
        HiveConf.ConfVars.HIVE_STATS_KEY_PREFIX);
  }


  @Override
  public void map(Object k, RCFileValueBufferWrapper value,
      OutputCollector<Object, Object> output, Reporter reporter)
      throws IOException {
    try {
      //CombineHiveInputFormat is set in PartialScanTask.
      RCFileKeyBufferWrapper key = (RCFileKeyBufferWrapper) ((CombineHiveKey) k).getKey();

      // calculate rawdatasize
      KeyBuffer keyBuffer = key.getKeyBuffer();
      long[] uncompressedColumnSizes = new long[keyBuffer.getColumnNumber()];
      for (int i = 0; i < keyBuffer.getColumnNumber(); i++) {
        uncompressedColumnSizes[i] += keyBuffer.getEachColumnUncompressedValueLen()[i];
      }
      if (uncompressedColumnSizes != null) {
        for (int i = 0; i < uncompressedColumnSizes.length; i++) {
          uncompressedFileSize += uncompressedColumnSizes[i];
        }
      }

      // calculate no. of rows
      rowNo += keyBuffer.getNumberRows();
    } catch (Throwable e) {
      this.exception = true;
      close();
      throw new IOException(e);
    }

  }


  @Override
  public void close() throws IOException {
    try {
      // Only publish stats if this operator's flag was set to gather stats
      if (!exception) {
        publishStats();
      }
    } catch (HiveException e) {
      this.exception = true;
      throw new RuntimeException(e);
    }
  }


  /**
   * Publish statistics.
   * similar to FileSinkOperator.java publishStats()
   *
   * @throws HiveException
   */
  private void publishStats() throws HiveException {
    // Initializing a stats publisher
    StatsPublisher statsPublisher = Utilities.getStatsPublisher(jc);

    if (statsPublisher == null) {
      // just return, stats gathering should not block the main query
      LOG.error("StatsPublishing error: StatsPublisher is not initialized.");
      throw new HiveException(ErrorMsg.STATSPUBLISHER_NOT_OBTAINED.getErrorCodedMsg());
    }

    if (!statsPublisher.connect(jc)) {
      // should fail since stats gathering is main purpose of the job
      LOG.error("StatsPublishing error: cannot connect to database");
      throw new HiveException(ErrorMsg.STATSPUBLISHER_CONNECTION_ERROR.getErrorCodedMsg());
    }

    // construct key used to store stats in intermediate db
    String taskID = Utilities.getTaskIdFromFilename(Utilities.getTaskId(jc));
    String keyPrefix = Utilities.getHashedStatsPrefix(
        statsAggKeyPrefix, HiveConf.getIntVar(jc, ConfVars.HIVE_STATS_KEY_PREFIX_MAX_LENGTH));
    String key = keyPrefix + taskID;

    // construct statistics to be stored
    Map<String, String> statsToPublish = new HashMap<String, String>();
    statsToPublish.put(StatsSetupConst.RAW_DATA_SIZE, Long.toString(uncompressedFileSize));
    statsToPublish.put(StatsSetupConst.ROW_COUNT, Long.toString(rowNo));

    if (!statsPublisher.publishStat(key, statsToPublish)) {
      // The original exception is lost.
      // Not changing the interface to maintain backward compatibility
      throw new HiveException(ErrorMsg.STATSPUBLISHER_PUBLISHING_ERROR.getErrorCodedMsg());
    }

    if (!statsPublisher.closeConnection()) {
      // The original exception is lost.
      // Not changing the interface to maintain backward compatibility
      throw new HiveException(ErrorMsg.STATSPUBLISHER_CLOSING_ERROR.getErrorCodedMsg());
    }
  }

}
