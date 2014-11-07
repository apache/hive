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

package org.apache.hadoop.hive.ql.exec.vector.util;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampUtils;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Test helper class that creates vectorized execution batches from arbitrary type iterables.
 */
public class FakeVectorRowBatchFromObjectIterables extends FakeVectorRowBatchBase {

  private final String[] types;
  private final List<Iterator<Object>> iterators;
  private final VectorizedRowBatch batch;
  private boolean eof;
  private final int batchSize;

  public String[] getTypes() {
    return this.types;
  }

  /**
   * Helper interface for assigning values to primitive vector column types.
   */
  private static interface ColumnVectorAssign {
    void assign(
        ColumnVector columnVector,
        int row,
        Object value);
  }

  private final ColumnVectorAssign[] columnAssign;

  public FakeVectorRowBatchFromObjectIterables(int batchSize, String[] types,
      Iterable<Object> ...iterables) throws HiveException {
    this.types = types;
    this.batchSize = batchSize;
    iterators = new ArrayList<Iterator<Object>>(types.length);
    columnAssign = new ColumnVectorAssign[types.length];

    batch = new VectorizedRowBatch(types.length, batchSize);
    for(int i=0; i< types.length; ++i) {
      if (types[i].equalsIgnoreCase("tinyint") ||
          types[i].equalsIgnoreCase("smallint")||
          types[i].equalsIgnoreCase("int")||
          types[i].equalsIgnoreCase("bigint")||
          types[i].equalsIgnoreCase("long")) {
        batch.cols[i] = new LongColumnVector(batchSize);
        columnAssign[i] = new ColumnVectorAssign() {
          @Override
          public void assign(
              ColumnVector columnVector,
              int row,
              Object value) {
            LongColumnVector lcv = (LongColumnVector) columnVector;
            lcv.vector[row] = Long.valueOf(value.toString());
          }
        };
      } else if (types[i].equalsIgnoreCase("boolean")) {
        batch.cols[i] = new LongColumnVector(batchSize);
        columnAssign[i] = new ColumnVectorAssign() {
          @Override
          public void assign(
              ColumnVector columnVector,
              int row,
              Object value) {
            LongColumnVector lcv = (LongColumnVector) columnVector;
            lcv.vector[row] = (Boolean) value ? 1 : 0;
          }
        };
      } else if (types[i].equalsIgnoreCase("timestamp")) {
        batch.cols[i] = new LongColumnVector(batchSize);
        columnAssign[i] = new ColumnVectorAssign() {
          @Override
          public void assign(
              ColumnVector columnVector,
              int row,
              Object value) {
            LongColumnVector lcv = (LongColumnVector) columnVector;
            Timestamp t = (Timestamp) value;
            lcv.vector[row] = TimestampUtils.getTimeNanoSec(t);
          }
        };

      } else if (types[i].equalsIgnoreCase("string")) {
        batch.cols[i] = new BytesColumnVector(batchSize);
        columnAssign[i] = new ColumnVectorAssign() {
          @Override
          public void assign(
              ColumnVector columnVector,
              int row,
              Object value) {
            BytesColumnVector bcv = (BytesColumnVector) columnVector;
            String s = (String) value;
            byte[] bytes = s.getBytes();
            bcv.vector[row] = bytes;
            bcv.start[row] = 0;
            bcv.length[row] = bytes.length;
          }
        };
      } else if (types[i].equalsIgnoreCase("double") ||
          types[i].equalsIgnoreCase("float")) {
        batch.cols[i] = new DoubleColumnVector(batchSize);
        columnAssign[i] = new ColumnVectorAssign() {
          @Override
          public void assign(
              ColumnVector columnVector,
              int row,
              Object value) {
            DoubleColumnVector dcv = (DoubleColumnVector) columnVector;
            dcv.vector[row] = Double.valueOf(value.toString());
          }
        };
      } else if (types[i].toLowerCase().startsWith("decimal")) {
            Pattern decimalPattern = Pattern.compile(
                "decimal(?:\\((\\d+)(?:\\,(\\d+))?\\))?", Pattern.CASE_INSENSITIVE);
            Matcher mr = decimalPattern.matcher(types[i]);
            int precission = 38;
            int scale = 0;
            if (mr.matches()) {
              String typePrecission = mr.group(1);
              if (typePrecission != null) {
                precission = Integer.parseInt(typePrecission);
              }
              String typeScale = mr.group(2);
              if (typeScale != null) {
                scale = Integer.parseInt(typeScale);
              }
            }

            batch.cols[i] = new DecimalColumnVector(batchSize, precission, scale);
            columnAssign[i] = new ColumnVectorAssign() {
                @Override
                public void assign(
                        ColumnVector columnVector,
                        int row,
                        Object value) {
                    DecimalColumnVector dcv = (DecimalColumnVector) columnVector;
                    dcv.set(row, (HiveDecimal) value);
                }
            };
      } else {
        throw new HiveException("Unimplemented type " + types[i]);
      }
      iterators.add(iterables[i].iterator());
    }
  }

  @Override
  public VectorizedRowBatch produceNextBatch() {
    batch.size = 0;
    batch.selectedInUse = false;
    for (int i=0; i < types.length; ++i) {
      ColumnVector col = batch.cols[i];
      col.noNulls = true;
      col.isRepeating = false;
    }
    while (!eof && batch.size < this.batchSize){
      int r = batch.size;
      for (int i=0; i < types.length; ++i) {
        Iterator<Object> it = iterators.get(i);
        if (!it.hasNext()) {
          eof = true;
          break;
        }
        Object value = it.next();
        if (null == value) {
          batch.cols[i].isNull[batch.size] = true;
          batch.cols[i].noNulls = false;
        } else {
          // Must reset the isNull, could be set from prev batch use
          batch.cols[i].isNull[batch.size] = false;
          columnAssign[i].assign(batch.cols[i], batch.size, value);
        }
      }
      if (!eof) {
        batch.size += 1;
      }
    }
    return batch;
  }
}

