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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.exec.Utilities.RecordReaderStatus;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FooterBuffer {
  private ArrayList<ObjectPair<WritableComparable, Writable>> buffer;
  private int cur;
  private int footerRowCount;
  private int bufferRowCount;

  private static final Logger LOG = LoggerFactory.getLogger(FooterBuffer.class.getName());

  public FooterBuffer() {
  }

  public void setCursor(int cur) {
    this.cur = cur;
  }

  /**
   * Initialize footer buffer in order to keep footer records at the end of file.
   *
   * @param job
   *          Current job configuration.
   *
   * @param recordreader
   *          Record reader.
   *
   * @param rrStatus
   *          Record reader status. CURRENT means the given key/value is not utilized yet. NEXT means
   *          need to fetch next key/value pair.
   *
   * @param footerCount
   *          Footer line number of the table files.
   *
   * @param key
   *          Key of current reading record.
   *
   * @param value
   *          Value of current reading record.
   *
   * @return Return true if there are 0 or more records left in the file
   *         after initializing the footer buffer, otherwise return false.
   */
  public boolean initializeBuffer(JobConf job, RecordReader recordreader, RecordReaderStatus rrStatus,
                                  int footerCount, WritableComparable key, Writable value) throws IOException {
    this.footerRowCount = footerCount;

    // Fill the buffer with key value pairs.
    this.buffer = new ArrayList<>();
    this.bufferRowCount = 0;
    LOG.debug("FooterCount: {}", footerCount);
    while (bufferRowCount < footerCount) {
      if (rrStatus == RecordReaderStatus.CURRENT) {
        LOG.debug("RR Status: CURRENT");
        rrStatus = RecordReaderStatus.NEXT;
      } else {
        boolean notEOF = recordreader.next(key, value);
        if (!notEOF) {
          return false;
        }
      }
      ObjectPair<WritableComparable, Writable> tem = new ObjectPair<>();
      tem.setFirst(ReflectionUtils.copy(job, key, tem.getFirst()));
      if (value instanceof VectorizedRowBatch) {
        VectorizedRowBatch vrb = (VectorizedRowBatch)value;
        tem.setSecond(copyVrb(vrb, (VectorizedRowBatch)recordreader.createValue()));
        bufferRowCount += vrb.size;

        LOG.debug("[initializeBuffer] Added Batch-{}: Size={}, BufferRowCount={}", buffer.size(), vrb.size, bufferRowCount);
      } else {
        tem.setSecond(ReflectionUtils.copy(job, value, tem.getSecond()));
        bufferRowCount++;
      }
      buffer.add(tem);
    }
    this.cur = 0;
    return true;
  }

  /**
   * Enqueue most recent record read, and dequeue earliest result in the queue.
   *
   * @param job
   *          Current job configuration.
   *
   * @param recordreader
   *          Record reader.
   *
   * @param key
   *          Key of current reading record.
   *
   * @param value
   *          Value of current reading record.
   *
   * @return Return false if reaches the end of file, otherwise return true.
   */
  public boolean updateBuffer(JobConf job, RecordReader recordreader,
      WritableComparable key, Writable value) throws IOException {
    if (buffer.size() == 0) {
      LOG.debug("[updateBuffer] EOF - Fetched all rows.");
      return false;
    }

    key = ReflectionUtils.copy(job, (WritableComparable)buffer.get(cur).getFirst(), key);
    boolean notEOF = true;
    if (value instanceof VectorizedRowBatch) {
      value = copyVrb((VectorizedRowBatch)buffer.get(cur).getSecond(), (VectorizedRowBatch)value);
      VectorizedRowBatch vrb = (VectorizedRowBatch)value;
      bufferRowCount -= vrb.size;

      LOG.debug("[updateBuffer] Fetched Batch-{}: Size={}, BufferRowCount={}", cur, vrb.size, bufferRowCount);
      ObjectPair<WritableComparable, Writable> kvPair = buffer.remove(cur);
      WritableComparable nextKey = (WritableComparable)kvPair.getFirst();
      Writable nextValue = (Writable)kvPair.getSecond();
      while (bufferRowCount < footerRowCount) {
        notEOF = recordreader.next(nextKey, nextValue);
        if (!notEOF) {
          break;
        }
        VectorizedRowBatch nextVrb = (VectorizedRowBatch)nextValue;
        ObjectPair<WritableComparable, Writable> tem = new ObjectPair<>();
        tem.setFirst(ReflectionUtils.copy(job, nextKey, tem.getFirst()));
        tem.setSecond(copyVrb(nextVrb, (VectorizedRowBatch)recordreader.createValue()));
        buffer.add(tem);
        bufferRowCount += nextVrb.size;
        LOG.debug("[updateBuffer] Added Batch-{}: Size={}, BufferRowCount={}", buffer.size(), nextVrb.size, bufferRowCount);
      }
      if (!notEOF) {
        int balanceFooterCnt = footerRowCount - bufferRowCount;
        if (vrb.size > balanceFooterCnt) {
          int size = vrb.size - balanceFooterCnt;
          int[] selected = new int[size];
          for (int i = 0; i < size; i++) {
            if (vrb.selectedInUse) {
              selected[i] = vrb.selected[i];
            } else {
              selected[i] = i;
            }
          }
          vrb.selectedInUse = true;
          vrb.selected = selected;
          vrb.size = size;
          notEOF = true;
          LOG.debug("[updateBuffer] Trimmed Batch-{}: Size={}, balanceFooterCnt={}", cur, size, balanceFooterCnt);
        }
      }
    } else {
      LOG.debug("[updateBuffer] Non-VectorizedRowBatch flow.");
      value = ReflectionUtils.copy(job, (Writable)buffer.get(cur).getSecond(), value);
      notEOF = recordreader.next(buffer.get(cur).getFirst(), buffer.get(cur).getSecond());
      if (notEOF) {
        cur = (++cur) % buffer.size();
      }
    }
    return notEOF;
  }

  private VectorizedRowBatch copyVrb(VectorizedRowBatch srcVrb, VectorizedRowBatch destVrb) {
    destVrb.size = srcVrb.size;
    destVrb.selectedInUse = srcVrb.selectedInUse;
    destVrb.endOfFile = srcVrb.endOfFile;
    int selectedLength = srcVrb.selected.length;
    if (selectedLength > 0) {
      destVrb.selected = new int[selectedLength];
      System.arraycopy(srcVrb.selected, 0, destVrb.selected, 0, selectedLength);
    }
    if (srcVrb.numCols > 0) {
      System.arraycopy(srcVrb.cols, 0, destVrb.cols, 0, srcVrb.numCols);
    }
    return destVrb;
  }
}
