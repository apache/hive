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

package org.apache.hadoop.hive.ql.exec.vector;

import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorBatchDebug {
  private static final Logger LOG = LoggerFactory.getLogger(VectorBatchDebug.class);

  public static String displayBytes(byte[] bytes, int start, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = start; i < start + length; i++) {
      char ch = (char) bytes[i];
      if (ch < ' ' || ch > '~') {
        sb.append(String.format("\\%03d", bytes[i] & 0xff));
      } else {
        sb.append(ch);
      }
    }
    return sb.toString();
  }

  public static void debugDisplayOneRow(VectorizedRowBatch batch, int index, String prefix) {
    StringBuilder sb = new StringBuilder();
    sb.append(prefix + " row " + index + " ");
    for (int p = 0; p < batch.projectionSize; p++) {
      int column = batch.projectedColumns[p];
      if (p == column) {
        sb.append("(col " + p + ") ");
      } else {
        sb.append("(proj col " + p + " col " + column + ") ");
      }
      ColumnVector colVector = batch.cols[column];
      if (colVector == null) {
        sb.append("(null ColumnVector)");
      } else {
        boolean isRepeating = colVector.isRepeating;
        if (isRepeating) {
          sb.append("(repeating)");
        }
        index = (isRepeating ? 0 : index);
        if (colVector.noNulls || !colVector.isNull[index]) {
          if (colVector instanceof LongColumnVector) {
            sb.append(((LongColumnVector) colVector).vector[index]);
          } else if (colVector instanceof DoubleColumnVector) {
            sb.append(((DoubleColumnVector) colVector).vector[index]);
          } else if (colVector instanceof BytesColumnVector) {
            BytesColumnVector bytesColumnVector = (BytesColumnVector) colVector;
            byte[] bytes = bytesColumnVector.vector[index];
            int start = bytesColumnVector.start[index];
            int length = bytesColumnVector.length[index];
            if (bytes == null) {
              sb.append("(Unexpected null bytes with start " + start + " length " + length + ")");
            } else {
              sb.append("bytes: '" + displayBytes(bytes, start, length) + "'");
            }
          } else if (colVector instanceof DecimalColumnVector) {
            sb.append(((DecimalColumnVector) colVector).vector[index].toString());
          } else if (colVector instanceof TimestampColumnVector) {
            Timestamp timestamp = new Timestamp(0);
            ((TimestampColumnVector) colVector).timestampUpdate(timestamp, index);
            sb.append(timestamp.toString());
          } else if (colVector instanceof IntervalDayTimeColumnVector) {
            HiveIntervalDayTime intervalDayTime = ((IntervalDayTimeColumnVector) colVector).asScratchIntervalDayTime(index);
            sb.append(intervalDayTime.toString());
          } else {
            sb.append("Unknown");
          }
        } else {
          sb.append("NULL");
        }
      }
      sb.append(" ");
    }
    System.err.println(sb.toString());
    // LOG.info(sb.toString());
  }

  public static void debugDisplayBatch(VectorizedRowBatch batch, String prefix) {
    for (int i = 0; i < batch.size; i++) {
      int index = (batch.selectedInUse ? batch.selected[i] : i);
      debugDisplayOneRow(batch, index, prefix);
    }
  }
}