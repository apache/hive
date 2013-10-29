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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.hive.ql.io.IOContext.Comparison;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/** This class prepares an IOContext, and provides the ability to perform a binary search on the
  * data.  The binary search can be used by setting the value of inputFormatSorted in the
  * MapreduceWork to true, but it should only be used if the data is going to a FilterOperator,
  * which filters by comparing a value in the data with a constant, using one of the comparisons
  * =, <, >, <=, >=.  If the RecordReader's underlying format is an RCFile, this object can perform
  * a binary search to find the block to begin reading from, and stop reading once it can be
  * determined no other entries will match the filter.
  */
public abstract class HiveContextAwareRecordReader<K, V> implements RecordReader<K, V> {

  private static final Log LOG = LogFactory.getLog(HiveContextAwareRecordReader.class.getName());

  private boolean initDone = false;
  private long rangeStart;
  private long rangeEnd;
  private long splitEnd;
  private long previousPosition = -1;
  private boolean wasUsingSortedSearch = false;
  private String genericUDFClassName = null;
  private final List<Comparison> stopComparisons = new ArrayList<Comparison>();

  protected RecordReader recordReader;
  protected JobConf jobConf;
  protected boolean isSorted = false;

  public HiveContextAwareRecordReader(JobConf conf) throws IOException {
    this(null, conf);
  }

  public HiveContextAwareRecordReader(RecordReader recordReader) {
    this.recordReader = recordReader;
  }

  public HiveContextAwareRecordReader(RecordReader recordReader, JobConf conf)
      throws IOException {
    this.recordReader = recordReader;
    this.jobConf = conf;
  }

  public void setRecordReader(RecordReader recordReader) {
    this.recordReader = recordReader;
  }

  /**
   * Close this {@link InputSplit} to future operations.
   *
   * @throws IOException
   */
  public abstract void doClose() throws IOException;

  private IOContext ioCxtRef =  null;

  @Override
  public void close() throws IOException {
    doClose();
    initDone = false;
    ioCxtRef = null;
  }

  @Override
  public boolean next(K key, V value) throws IOException {
    if(!initDone) {
      throw new IOException("Hive IOContext is not inited.");
    }
    updateIOContext();
    try {
      return doNext(key, value);
    } catch (IOException e) {
      ioCxtRef.setIOExceptions(true);
      throw e;
    }
  }

  protected void updateIOContext()
      throws IOException {
    long pointerPos = this.getPos();
    if (!ioCxtRef.isBlockPointer) {
      ioCxtRef.currentBlockStart = pointerPos;
      ioCxtRef.currentRow = 0;
      return;
    }

    ioCxtRef.currentRow++;

    if (ioCxtRef.nextBlockStart == -1) {
      ioCxtRef.nextBlockStart = pointerPos;
      ioCxtRef.currentRow = 0;
    }
    if (pointerPos != ioCxtRef.nextBlockStart) {
      // the reader pointer has moved to the end of next block, or the end of
      // current record.

      ioCxtRef.currentRow = 0;

      if (ioCxtRef.currentBlockStart == ioCxtRef.nextBlockStart) {
        ioCxtRef.currentRow = 1;
      }

      ioCxtRef.currentBlockStart = ioCxtRef.nextBlockStart;
      ioCxtRef.nextBlockStart = pointerPos;
    }
  }

  public IOContext getIOContext() {
    return IOContext.get();
  }

  public void initIOContext(long startPos, boolean isBlockPointer, Path inputPath) {
    ioCxtRef = this.getIOContext();
    ioCxtRef.currentBlockStart = startPos;
    ioCxtRef.isBlockPointer = isBlockPointer;
    ioCxtRef.inputPath = inputPath;
    LOG.info("Processing file " + inputPath);
    initDone = true;
  }

  public void initIOContext(FileSplit split, JobConf job,
      Class inputFormatClass) throws IOException {
    this.initIOContext(split, job, inputFormatClass, null);
  }

  public void initIOContext(FileSplit split, JobConf job,
      Class inputFormatClass, RecordReader recordReader) throws IOException {

    boolean blockPointer = false;
    long blockStart = -1;
    FileSplit fileSplit = (FileSplit) split;
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(job);
    if (inputFormatClass.getName().contains("SequenceFile")) {
      SequenceFile.Reader in = new SequenceFile.Reader(fs, path, job);
      blockPointer = in.isBlockCompressed();
      in.sync(fileSplit.getStart());
      blockStart = in.getPosition();
      in.close();
    } else if (recordReader instanceof RCFileRecordReader) {
      blockPointer = true;
      blockStart = ((RCFileRecordReader) recordReader).getStart();
    } else if (inputFormatClass.getName().contains("RCFile")) {
      blockPointer = true;
      RCFile.Reader in = new RCFile.Reader(fs, path, job);
      in.sync(fileSplit.getStart());
      blockStart = in.getPosition();
      in.close();
    }
    this.initIOContext(blockStart, blockPointer, path.makeQualified(fs));

    this.initIOContextSortedProps(split, recordReader, job);
  }

  public void initIOContextSortedProps(FileSplit split, RecordReader recordReader, JobConf job) {
    this.getIOContext().resetSortingValues();
    this.isSorted = jobConf.getBoolean("hive.input.format.sorted", false);

    this.rangeStart = split.getStart();
    this.rangeEnd = split.getStart() + split.getLength();
    this.splitEnd = rangeEnd;
    if (recordReader instanceof RCFileRecordReader && rangeEnd != 0 && this.isSorted) {
      // Binary search only works if we know the size of the split, and the recordReader is an
      // RCFileRecordReader
      this.getIOContext().setUseSorted(true);
      this.getIOContext().setIsBinarySearching(true);
      this.wasUsingSortedSearch = true;
    } else {
      // Use the defalut methods for next in the child class
      this.isSorted = false;
    }
  }

  @Override
  public float getProgress() throws IOException {
    if (this.getIOContext().isBinarySearching()) {
      return 0;
    } else {
      return recordReader.getProgress();
    }
  }

  public boolean doNext(K key, V value) throws IOException {
    if (this.isSorted) {
      if (this.getIOContext().shouldEndBinarySearch() ||
          (!this.getIOContext().useSorted() && this.wasUsingSortedSearch)) {
        beginLinearSearch();
        this.wasUsingSortedSearch = false;
        this.getIOContext().setEndBinarySearch(false);
      }

      if (this.getIOContext().useSorted()) {
        if (this.genericUDFClassName == null &&
            this.getIOContext().getGenericUDFClassName() != null) {
          setGenericUDFClassName(this.getIOContext().getGenericUDFClassName());
        }

        if (this.getIOContext().isBinarySearching()) {
          // Proceed with a binary search
          if (this.getIOContext().getComparison() != null) {
            switch (this.getIOContext().getComparison()) {
              case GREATER:
              case EQUAL:
                // Indexes have only one entry per value, could go linear from here, if we want to
                // use this for any sorted table, we'll need to continue the search
                rangeEnd = previousPosition;
                break;
              case LESS:
                rangeStart = previousPosition;
                break;
              default:
                break;
            }
          }

          long position = (rangeStart + rangeEnd) / 2;
          sync(position);

          long newPosition = getSyncedPosition();
          // If the newPosition is the same as the previousPosition, we've reached the end of the
          // binary search, if the new position at least as big as the size of the split, any
          // matching rows must be in the final block, so we can end the binary search.
          if (newPosition == previousPosition || newPosition >= splitEnd) {
            this.getIOContext().setIsBinarySearching(false);
            sync(rangeStart);
          }

          previousPosition = newPosition;
        }  else if (foundAllTargets()) {
          // Found all possible rows which will not be filtered
          return false;
        }
      }
    }

    try {
      return recordReader.next(key,  value);
    } catch (Exception e) {
      return HiveIOExceptionHandlerUtil.handleRecordReaderNextException(e, jobConf);
    }
  }

  private void sync(long position) throws IOException {
     ((RCFileRecordReader)recordReader).sync(position);
     ((RCFileRecordReader)recordReader).resetBuffer();
  }

  private long getSyncedPosition() throws IOException {
    return recordReader.getPos();
  }
  /**
   * This uses the name of the generic UDF being used by the filter to determine whether we should
   * perform a binary search, and what the comparisons we should use to signal the end of the
   * linear scan are.
   * @param genericUDFClassName
   * @throws IOException
   */
  private void setGenericUDFClassName(String genericUDFClassName) throws IOException {
    this.genericUDFClassName = genericUDFClassName;
    if (genericUDFClassName.equals(GenericUDFOPEqual.class.getName())) {
      stopComparisons.add(Comparison.GREATER);
    } else if (genericUDFClassName.equals(GenericUDFOPLessThan.class.getName())) {
      stopComparisons.add(Comparison.EQUAL);
      stopComparisons.add(Comparison.GREATER);
      if (this.getIOContext().isBinarySearching()) {
        beginLinearSearch();
      }
    } else if (genericUDFClassName.equals(GenericUDFOPEqualOrLessThan.class.getName())) {
      stopComparisons.add(Comparison.GREATER);
      if (this.getIOContext().isBinarySearching()) {
        beginLinearSearch();
      }
    } else if (genericUDFClassName.equals(GenericUDFOPGreaterThan.class.getName()) ||
        genericUDFClassName.equals(GenericUDFOPEqualOrGreaterThan.class.getName())) {
      // Do nothing
    } else {
      // This is an unsupported operator
      LOG.debug(genericUDFClassName + " is not the name of a supported class.  " +
                "Continuing linearly.");
      if (this.getIOContext().isBinarySearching()) {
        beginLinearSearch();
      }
    }
  }

  /**
   * This should be called after the binary search is finished and before the linear scan begins
   * @throws IOException
   */
  private void beginLinearSearch() throws IOException {
    sync(rangeStart);
    this.getIOContext().setIsBinarySearching(false);
    this.wasUsingSortedSearch = false;
  }

  /**
   * Returns true if the current comparison is in the list of stop comparisons, i.e. we've found
   * all records which won't be filtered
   * @return true if the current comparison is found
   */
  public boolean foundAllTargets() {
    if (this.getIOContext().getComparison() == null ||
        !stopComparisons.contains(this.getIOContext().getComparison())) {
      return false;
    }

    return true;
  }
}
