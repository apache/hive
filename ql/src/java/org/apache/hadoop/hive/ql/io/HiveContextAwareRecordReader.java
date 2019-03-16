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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.FooterBuffer;
import org.apache.hadoop.hive.ql.io.IOContext.Comparison;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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

  private static final Logger LOG = LoggerFactory.getLogger(HiveContextAwareRecordReader.class.getName());

  private boolean initDone = false;
  private long rangeStart;
  private long rangeEnd;
  private long splitEnd;
  private long previousPosition = -1;
  private boolean wasUsingSortedSearch = false;
  private String genericUDFClassName = null;
  private final List<Comparison> stopComparisons = new ArrayList<Comparison>();
  private Map<Path, PartitionDesc> pathToPartitionInfo;

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
      boolean retVal = doNext(key, value);
      if(retVal) {
        if(key instanceof RecordIdentifier) {
          //supports AcidInputFormat which uses the KEY pass ROW__ID info
          ioCxtRef.setRecordIdentifier((RecordIdentifier)key);
        }
        else if(recordReader instanceof AcidInputFormat.AcidRecordReader) {
          //supports AcidInputFormat which do not use the KEY pass ROW__ID info
          ioCxtRef.setRecordIdentifier(((AcidInputFormat.AcidRecordReader) recordReader).getRecordIdentifier());
        }
      }
      return retVal;
    } catch (IOException e) {
      ioCxtRef.setIOExceptions(true);
      throw e;
    }
  }

  protected void updateIOContext()
      throws IOException {
    long pointerPos = this.getPos();
    if (!ioCxtRef.isBlockPointer()) {
      ioCxtRef.setCurrentBlockStart(pointerPos);
      ioCxtRef.setCurrentRow(0);
      return;
    }

    ioCxtRef.setCurrentRow(ioCxtRef.getCurrentRow() + 1);

    if (ioCxtRef.getNextBlockStart() == -1) {
      ioCxtRef.setNextBlockStart(pointerPos);
      ioCxtRef.setCurrentRow(0);
    }
    if (pointerPos != ioCxtRef.getNextBlockStart()) {
      // the reader pointer has moved to the end of next block, or the end of
      // current record.

      ioCxtRef.setCurrentRow(0);

      if (ioCxtRef.getCurrentBlockStart() == ioCxtRef.getNextBlockStart()) {
        ioCxtRef.setCurrentRow(1);
      }

      ioCxtRef.setCurrentBlockStart(ioCxtRef.getNextBlockStart());
      ioCxtRef.setNextBlockStart(pointerPos);
    }
  }

  public IOContext getIOContext() {
    return IOContextMap.get(jobConf);
  }

  private void initIOContext(long startPos, boolean isBlockPointer,
      Path inputPath) {
    ioCxtRef = this.getIOContext();
    ioCxtRef.setCurrentBlockStart(startPos);
    ioCxtRef.setBlockPointer(isBlockPointer);
    ioCxtRef.setInputPath(inputPath);
    LOG.debug("Processing file " + inputPath); // Logged at INFO in multiple other places.
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
    FileSplit fileSplit = split;
    Path path = fileSplit.getPath();
    if (inputFormatClass.getName().contains("SequenceFile")) {
      FileSystem fs = path.getFileSystem(job);
      SequenceFile.Reader in = new SequenceFile.Reader(fs, path, job);
      blockPointer = in.isBlockCompressed();
      in.sync(fileSplit.getStart());
      blockStart = in.getPosition();
      in.close();
    } else if (recordReader instanceof RCFileRecordReader) {
      blockPointer = true;
      blockStart = ((RCFileRecordReader) recordReader).getStart();
    } else if (inputFormatClass.getName().contains("RCFile")) {
      FileSystem fs = path.getFileSystem(job);
      blockPointer = true;
      RCFile.Reader in = new RCFile.Reader(fs, path, job);
      in.sync(fileSplit.getStart());
      blockStart = in.getPosition();
      in.close();
    }
    this.jobConf = job;
    this.initIOContext(blockStart, blockPointer, path);

    this.initIOContextSortedProps(split, recordReader, job);
  }

  public void initIOContextSortedProps(FileSplit split, RecordReader recordReader, JobConf job) {
    this.jobConf = job;

    this.getIOContext().resetSortingValues();
    this.isSorted = jobConf.getBoolean("hive.input.format.sorted", false);

    this.rangeStart = split.getStart();
    this.rangeEnd = split.getStart() + split.getLength();
    this.splitEnd = rangeEnd;
    if (recordReader instanceof RCFileRecordReader && rangeEnd != 0 && this.isSorted) {
      // Binary search only works if we know the size of the split, and the recordReader is an
      // RCFileRecordReader
      this.getIOContext().setUseSorted(true);
      this.getIOContext().setBinarySearching(true);
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

  private FooterBuffer footerBuffer = null;
  private int headerCount = 0;
  private int footerCount = 0;

  protected FooterBuffer getFooterBuffer() {
       return footerBuffer;
  }

  protected void setFooterBuffer( FooterBuffer buf) {
    footerBuffer = buf;
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
            this.getIOContext().setBinarySearching(false);
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

      /**
       * When start reading new file, check header, footer rows.
       * If file contains header, skip header lines before reading the records.
       * If file contains footer, used a FooterBuffer to remove footer lines
       * at the end of the table file.
       **/
      if (this.ioCxtRef.getCurrentBlockStart() == 0) {

        // Check if the table file has header to skip.
        footerBuffer = null;
        Path filePath = this.ioCxtRef.getInputPath();
        PartitionDesc part = null;
        try {
          if (pathToPartitionInfo == null) {
            pathToPartitionInfo = Utilities
              .getMapWork(jobConf).getPathToPartitionInfo();
          }
          part = HiveFileFormatUtils
              .getFromPathRecursively(pathToPartitionInfo,
                  filePath, IOPrepareCache.get().getPartitionDescMap());
        } catch (AssertionError ae) {
          LOG.info("Cannot get partition description from " + this.ioCxtRef.getInputPath()
              + "because " + ae.getMessage());
          part = null;
        } catch (Exception e) {
          LOG.info("Cannot get partition description from " + this.ioCxtRef.getInputPath()
              + "because " + e.getMessage());
          part = null;
        }
        TableDesc table = (part == null) ? null : part.getTableDesc();
        if (table != null) {
          headerCount = Utilities.getHeaderCount(table);
          footerCount = Utilities.getFooterCount(table, jobConf);
        }

        // If input contains header, skip header.
        if (!Utilities.skipHeader(recordReader, headerCount, (WritableComparable)key, (Writable)value)) {
          return false;
        }
        if (footerCount > 0) {
          footerBuffer = new FooterBuffer();
          if (!footerBuffer.initializeBuffer(jobConf, recordReader, footerCount, (WritableComparable)key, (Writable)value)) {
            return false;
          }
        }
      }
      if (footerBuffer == null) {

        // Table files don't have footer rows.
        return recordReader.next(key,  value);
      } else {
        return footerBuffer.updateBuffer(jobConf, recordReader, (WritableComparable)key, (Writable)value);
      }
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
    this.getIOContext().setBinarySearching(false);
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
