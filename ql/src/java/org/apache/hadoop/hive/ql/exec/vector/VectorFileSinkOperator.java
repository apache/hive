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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * File Sink operator implementation.
 **/
public class VectorFileSinkOperator extends FileSinkOperator {

  private static final long serialVersionUID = 1L;

  public VectorFileSinkOperator(VectorizationContext context,
      OperatorDesc conf) {
    super();
    this.conf = (FileSinkDesc) conf;
  }

  public VectorFileSinkOperator() {

  }

  @Override
  public void processOp(Object data, int tag) throws HiveException {

    VectorizedRowBatch vrg = (VectorizedRowBatch)data;

    Writable [] records = null;
    boolean vectorizedSerde = false;
    int outputIterations = 1;
    try {
      if (serializer instanceof VectorizedSerde) {
        recordValue = ((VectorizedSerde) serializer).serializeVector(vrg,
            inputObjInspectors[0]);
        records = (Writable[]) ((ObjectWritable) recordValue).get();
        vectorizedSerde = true;
        outputIterations = vrg.size;
      }
    } catch (SerDeException e1) {
      throw new HiveException(e1);
    }

    for (int i = 0; i < outputIterations; i++) {
      Writable row = null;
      if (vectorizedSerde) {
        row = records[i];
      } else {
        if (vrg.valueWriters == null) {
          vrg.setValueWriters(VectorExpressionWriterFactory.getExpressionWriters(
              (StructObjectInspector)inputObjInspectors[0]));
        }
        row = new Text(vrg.toString());
      }
    /* Create list bucketing sub-directory only if stored-as-directories is on. */
    String lbDirName = null;
    lbDirName = (lbCtx == null) ? null : generateListBucketingDirName(row);

    FSPaths fpaths;

    if (!bDynParts && !filesCreated) {
      if (lbDirName != null) {
        FSPaths fsp2 = lookupListBucketingPaths(lbDirName);
      } else {
        createBucketFiles(fsp);
      }
    }

    // Since File Sink is a terminal operator, forward is not called - so,
    // maintain the number of output rows explicitly
    if (counterNameToEnum != null) {
      ++outputRows;
      if (outputRows % 1000 == 0) {
        incrCounter(numOutputRowsCntr, outputRows);
        outputRows = 0;
      }
    }

    try {
      updateProgress();

      // if DP is enabled, get the final output writers and prepare the real output row
      assert inputObjInspectors[0].getCategory() == ObjectInspector.Category.STRUCT : "input object inspector is not struct";

      if (bDynParts) {
        // copy the DP column values from the input row to dpVals
        dpVals.clear();
        dpWritables.clear();
        ObjectInspectorUtils.partialCopyToStandardObject(dpWritables, row, dpStartCol, numDynParts,
            (StructObjectInspector) inputObjInspectors[0], ObjectInspectorCopyOption.WRITABLE);
        // get a set of RecordWriter based on the DP column values
        // pass the null value along to the escaping process to determine what the dir should be
        for (Object o : dpWritables) {
          if (o == null || o.toString().length() == 0) {
            dpVals.add(dpCtx.getDefaultPartitionName());
          } else {
            dpVals.add(o.toString());
          }
        }
        fpaths = getDynOutPaths(dpVals, lbDirName);

      } else {
        if (lbDirName != null) {
          fpaths = lookupListBucketingPaths(lbDirName);
        } else {
          fpaths = fsp;
        }
      }

      rowOutWriters = fpaths.getOutWriters();
      if (conf.isGatherStats()) {
        if (statsCollectRawDataSize) {
          SerDeStats stats = serializer.getSerDeStats();
          if (stats != null) {
            fpaths.getStat().addToStat(StatsSetupConst.RAW_DATA_SIZE, stats.getRawDataSize());
          }
        }
        fpaths.getStat().addToStat(StatsSetupConst.ROW_COUNT, 1);
      }


      if (row_count != null) {
        row_count.set(row_count.get() + 1);
      }

      if (!multiFileSpray) {
        rowOutWriters[0].write(row);
      } else {
        int keyHashCode = 0;
        key.setHashCode(keyHashCode);
        int bucketNum = prtner.getBucket(key, null, totalFiles);
        int idx = bucketMap.get(bucketNum);
        rowOutWriters[idx].write(row);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
    }
  }
}
