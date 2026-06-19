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

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.tez.tools.KeyValueInputMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.AbstractMapOperator;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.mapreduce.lib.MRReader;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * Process input from tez LogicalInput and write output - for a map plan Just pump the records
 * through the query plan.
 */

public class MapRecordSource implements RecordSource {

  public static final Logger LOG = LoggerFactory.getLogger(MapRecordSource.class);
  private ExecMapperContext execContext = null;
  private AbstractMapOperator mapOp = null;
  private KeyValueReader reader = null;
  private final boolean grouped = false;

  // Flush the last record when reader is out of records
  private boolean flushLastRecord = false;

  void init(JobConf jconf, AbstractMapOperator mapOp, KeyValueReader reader) throws IOException {
    execContext = mapOp.getExecContext();
    this.mapOp = mapOp;
    if (reader instanceof KeyValueInputMerger) {
      KeyValueInputMerger kvMerger = (KeyValueInputMerger) reader;
      kvMerger.setIOCxt(execContext.getIoCxt());
    }
    this.reader = reader;
  }

  @Override
  public final boolean isGrouped() {
    return grouped;
  }

  @Override
  public void setFlushLastRecord(boolean flushLastRecord) {
    this.flushLastRecord = flushLastRecord;
  }

  @Override
  public boolean pushRecord() throws HiveException {
    execContext.resetRow();

    try {
      if (reader.next()) {
        Object value;
        try {
          value = reader.getCurrentValue();
        } catch (IOException e) {
          closeReader();
          throw new HiveException(e);
        }
        return processRow(value);
      } else if (flushLastRecord) {
        mapOp.flushRecursive();
      }
    } catch (IOException e) {
      closeReader();
      throw new HiveException(e);
    }
    return false;
  }

  private boolean processRow(Object value) {
    try {
      if (mapOp.getDone()) {
        return false; // done
      } else {
        // Since there is no concept of a group, we don't invoke
        // startGroup/endGroup for a mapper
        mapOp.process((Writable) value);
      }
    } catch (Throwable e) {
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        LOG.error("Failed to process row", e);
        closeReader();
        throw new RuntimeException(e);
      }
    }
    return true; // give me more
  }

  private void closeReader() {
    if (!(reader instanceof MRReader)) {
      LOG.warn("Cannot close " + (reader == null ? null : reader.getClass()));
      return;
    }
    if (reader instanceof KeyValueInputMerger) {
      // cleanup
      KeyValueInputMerger kvMerger = (KeyValueInputMerger) reader;
      kvMerger.clean();
    }

    LOG.info("Closing MRReader on error");
    MRReader mrReader = (MRReader)reader;
    try {
      mrReader.close();
    } catch (IOException ex) {
      LOG.error("Failed to close the reader; ignoring", ex);
    }
  }

}
