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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Simple persistent container for rows.
 *
 * This container interface only accepts adding or appending new rows and iterating through the rows
 * in the order of their insertions.
 *
 * The iterator interface is a lightweight first()/next() API rather than the Java Iterator
 * interface. This way we do not need to create an Iterator object every time we want to start a new
 * iteration. Below is simple example of how to convert a typical Java's Iterator code to the LW
 * iterator interface.
 *
 * Iterator itr = rowContainer.iterator(); while (itr.hasNext()) { v = itr.next(); // do anything
 * with v }
 *
 * can be rewritten to:
 *
 * for ( v = rowContainer.first(); v != null; v = rowContainer.next()) { // do anything with v }
 *
 * Once the first is called, it will not be able to write again. So there can not be any writes
 * after read. It can be read multiple times, but it does not support multiple reader interleaving
 * reading.
 *
 */
public class RowContainer<ROW extends List<Object>> extends AbstractRowContainer<ROW> {

  protected static Log LOG = LogFactory.getLog(RowContainer.class);

  // max # of rows can be put into one block
  private static final int BLOCKSIZE = 25000;

  private ROW[] currentWriteBlock; // the last block that add() should append to
  private ROW[] currentReadBlock; // the current block where the cursor is in
  // since currentReadBlock may assigned to currentWriteBlock, we need to store
  // original read block
  private ROW[] firstReadBlockPointer;
  private int blockSize; // number of objects in the block before it is spilled
  // to disk
  private int numFlushedBlocks; // total # of blocks
  private long size;    // total # of elements in the RowContainer
  private File tmpFile; // temporary file holding the spilled blocks
  Path tempOutPath = null;
  private File parentFile;
  private int itrCursor; // iterator cursor in the currBlock
  private int readBlockSize; // size of current read block
  private int addCursor; // append cursor in the lastBlock
  private SerDe serde; // serialization/deserialization for the row
  private ObjectInspector standardOI; // object inspector for the row

  private List<Object> keyObject;

  private TableDesc tblDesc;

  boolean firstCalled = false; // once called first, it will never be able to
  // write again.
  int acutalSplitNum = 0;
  int currentSplitPointer = 0;
  org.apache.hadoop.mapred.RecordReader rr = null; // record reader
  FSRecordWriter rw = null;
  InputFormat<WritableComparable, Writable> inputFormat = null;
  InputSplit[] inputSplits = null;
  private ROW dummyRow = null;
  private final Reporter reporter;

  Writable val = null; // cached to use serialize data

  Configuration jc;
  JobConf jobCloneUsingLocalFs = null;
  private LocalFileSystem localFs;

  public RowContainer(Configuration jc, Reporter reporter) throws HiveException {
    this(BLOCKSIZE, jc, reporter);
  }

  public RowContainer(int bs, Configuration jc, Reporter reporter
                     ) throws HiveException {
    // no 0-sized block
    this.blockSize = bs <= 0 ? BLOCKSIZE : bs;
    this.size = 0;
    this.itrCursor = 0;
    this.addCursor = 0;
    this.numFlushedBlocks = 0;
    this.tmpFile = null;
    this.currentWriteBlock = (ROW[]) new ArrayList[blockSize];
    this.currentReadBlock = this.currentWriteBlock;
    this.firstReadBlockPointer = currentReadBlock;
    this.serde = null;
    this.standardOI = null;
    this.jc = jc;
    if (reporter == null) {
      this.reporter = Reporter.NULL;
    } else {
      this.reporter = reporter;
    }
  }

  private JobConf getLocalFSJobConfClone(Configuration jc) {
    if (this.jobCloneUsingLocalFs == null) {
      this.jobCloneUsingLocalFs = new JobConf(jc);
      HiveConf.setVar(jobCloneUsingLocalFs, HiveConf.ConfVars.HADOOPFS, Utilities.HADOOP_LOCAL_FS);
    }
    return this.jobCloneUsingLocalFs;
  }


  public void setSerDe(SerDe sd, ObjectInspector oi) {
    this.serde = sd;
    this.standardOI = oi;
  }

  @Override
  public void add(ROW t) throws HiveException {
    if (this.tblDesc != null) {
      if (willSpill()) { // spill the current block to tmp file
        spillBlock(currentWriteBlock, addCursor);
        addCursor = 0;
        if (numFlushedBlocks == 1) {
          currentWriteBlock = (ROW[]) new ArrayList[blockSize];
        }
      }
      currentWriteBlock[addCursor++] = t;
    } else if (t != null) {
      // the tableDesc will be null in the case that all columns in that table
      // is not used. we use a dummy row to denote all rows in that table, and
      // the dummy row is added by caller.
      this.dummyRow = t;
    }
    ++size;
  }

  @Override
  public ROW first() throws HiveException {
    if (size == 0) {
      return null;
    }

    try {
      firstCalled = true;
      // when we reach here, we must have some data already (because size >0).
      // We need to see if there are any data flushed into file system. If not,
      // we can
      // directly read from the current write block. Otherwise, we need to read
      // from the beginning of the underlying file.
      this.itrCursor = 0;
      closeWriter();
      closeReader();

      if (tblDesc == null) {
        this.itrCursor++;
        return dummyRow;
      }

      this.currentReadBlock = this.firstReadBlockPointer;
      if (this.numFlushedBlocks == 0) {
        this.readBlockSize = this.addCursor;
        this.currentReadBlock = this.currentWriteBlock;
      } else {
        JobConf localJc = getLocalFSJobConfClone(jc);
        if (inputSplits == null) {
          if (this.inputFormat == null) {
            inputFormat = (InputFormat<WritableComparable, Writable>) ReflectionUtils.newInstance(
                tblDesc.getInputFileFormatClass(), localJc);
          }

          HiveConf.setVar(localJc, HiveConf.ConfVars.HADOOPMAPREDINPUTDIR,
              org.apache.hadoop.util.StringUtils.escapeString(parentFile.getAbsolutePath()));
          inputSplits = inputFormat.getSplits(localJc, 1);
          acutalSplitNum = inputSplits.length;
        }
        currentSplitPointer = 0;
        rr = inputFormat.getRecordReader(inputSplits[currentSplitPointer],
          localJc, reporter);
        currentSplitPointer++;

        nextBlock(0);
      }
      // we are guaranteed that we can get data here (since 'size' is not zero)
      ROW ret = currentReadBlock[itrCursor++];
      removeKeys(ret);
      return ret;
    } catch (Exception e) {
      throw new HiveException(e);
    }

  }

  @Override
  public ROW next() throws HiveException {

    if (!firstCalled) {
      throw new RuntimeException("Call first() then call next().");
    }

    if (size == 0) {
      return null;
    }

    if (tblDesc == null) {
      if (this.itrCursor < size) {
        this.itrCursor++;
        return dummyRow;
      }
      return null;
    }

    ROW ret;
    if (itrCursor < this.readBlockSize) {
      ret = this.currentReadBlock[itrCursor++];
      removeKeys(ret);
      return ret;
    } else {
      nextBlock(0);
      if (this.readBlockSize == 0) {
        if (currentWriteBlock != null && currentReadBlock != currentWriteBlock) {
          setWriteBlockAsReadBlock();
        } else {
          return null;
        }
      }
      return next();
    }
  }

  private void removeKeys(ROW ret) {
    if (this.keyObject != null && this.currentReadBlock != this.currentWriteBlock) {
      int len = this.keyObject.size();
      int rowSize = ((ArrayList) ret).size();
      for (int i = 0; i < len; i++) {
        ((ArrayList) ret).remove(rowSize - i - 1);
      }
    }
  }

  private final ArrayList<Object> row = new ArrayList<Object>(2);

  private void spillBlock(ROW[] block, int length) throws HiveException {
    try {
      if (tmpFile == null) {
        setupWriter();
      } else if (rw == null) {
        throw new HiveException("RowContainer has already been closed for writing.");
      }

      row.clear();
      row.add(null);
      row.add(null);

      if (this.keyObject != null) {
        row.set(1, this.keyObject);
        for (int i = 0; i < length; ++i) {
          ROW currentValRow = block[i];
          row.set(0, currentValRow);
          Writable outVal = serde.serialize(row, standardOI);
          rw.write(outVal);
        }
      } else {
        for (int i = 0; i < length; ++i) {
          ROW currentValRow = block[i];
          Writable outVal = serde.serialize(currentValRow, standardOI);
          rw.write(outVal);
        }
      }

      if (block == this.currentWriteBlock) {
        this.addCursor = 0;
      }

      this.numFlushedBlocks++;
    } catch (Exception e) {
      clear();
      LOG.error(e.toString(), e);
      if ( e instanceof HiveException ) {
        throw (HiveException) e;
      }
      throw new HiveException(e);
    }
  }

  /**
   * Get the number of elements in the RowContainer.
   *
   * @return number of elements in the RowContainer
   */
  @Override
  public long size() {
    return size;
  }

  protected boolean nextBlock(int readIntoOffset) throws HiveException {
    itrCursor = 0;
    this.readBlockSize = 0;
    if (this.numFlushedBlocks == 0) {
      return false;
    }

    try {
      if (val == null) {
        val = serde.getSerializedClass().newInstance();
      }
      boolean nextSplit = true;
      int i = readIntoOffset;

      if (rr != null) {
        Object key = rr.createKey();
        while (i < this.currentReadBlock.length && rr.next(key, val)) {
          nextSplit = false;
          this.currentReadBlock[i++] = (ROW) ObjectInspectorUtils.copyToStandardObject(serde
              .deserialize(val), serde.getObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
        }
      }

      if (nextSplit && this.currentSplitPointer < this.acutalSplitNum) {
        JobConf localJc = getLocalFSJobConfClone(jc);
        // open record reader to read next split
        rr = inputFormat.getRecordReader(inputSplits[currentSplitPointer], jobCloneUsingLocalFs,
            reporter);
        currentSplitPointer++;
        return nextBlock(0);
      }

      this.readBlockSize = i;
      return this.readBlockSize > 0;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      try {
        this.clear();
      } catch (HiveException e1) {
        LOG.error(e.getMessage(), e);
      }
      throw new HiveException(e);
    }
  }

  public void copyToDFSDirecory(FileSystem destFs, Path destPath) throws IOException, HiveException {
    if (addCursor > 0) {
      this.spillBlock(this.currentWriteBlock, addCursor);
    }
    if (tempOutPath == null || tempOutPath.toString().trim().equals("")) {
      return;
    }
    this.closeWriter();
    LOG.info("RowContainer copied temp file " + tmpFile.getAbsolutePath() + " to dfs directory "
        + destPath.toString());
    destFs
        .copyFromLocalFile(true, tempOutPath, new Path(destPath, new Path(tempOutPath.getName())));
    clear();
  }

  /**
   * Remove all elements in the RowContainer.
   */
  @Override
  public void clear() throws HiveException {
    itrCursor = 0;
    addCursor = 0;
    numFlushedBlocks = 0;
    this.readBlockSize = 0;
    this.acutalSplitNum = 0;
    this.currentSplitPointer = -1;
    this.firstCalled = false;
    this.inputSplits = null;
    tempOutPath = null;
    addCursor = 0;

    size = 0;
    try {
      if (rw != null) {
        rw.close(false);
      }
      if (rr != null) {
        rr.close();
      }
    } catch (Exception e) {
      LOG.error(e.toString());
      throw new HiveException(e);
    } finally {
      rw = null;
      rr = null;
      tmpFile = null;
      deleteLocalFile(parentFile, true);
      parentFile = null;
    }
  }

  private void deleteLocalFile(File file, boolean recursive) {
    try {
      if (file != null) {
        if (!file.exists()) {
          return;
        }
        if (file.isDirectory() && recursive) {
          File[] files = file.listFiles();
          for (File file2 : files) {
            deleteLocalFile(file2, true);
          }
        }
        boolean deleteSuccess = file.delete();
        if (!deleteSuccess) {
          LOG.error("Error deleting tmp file:" + file.getAbsolutePath());
        }
      }
    } catch (Exception e) {
      LOG.error("Error deleting tmp file:" + file.getAbsolutePath(), e);
    }
  }

  private void closeWriter() throws IOException {
    if (this.rw != null) {
      this.rw.close(false);
      this.rw = null;
    }
  }

  private void closeReader() throws IOException {
    if (this.rr != null) {
      this.rr.close();
      this.rr = null;
    }
  }

  public void setKeyObject(List<Object> dummyKey) {
    this.keyObject = dummyKey;
  }

  public void setTableDesc(TableDesc tblDesc) {
    this.tblDesc = tblDesc;
  }

  protected int getAddCursor() {
    return addCursor;
  }

  protected final boolean willSpill() {
    return addCursor >= blockSize;
  }

  protected int getBlockSize() {
    return blockSize;
  }

  protected void setupWriter() throws HiveException {
    try {

      if ( tmpFile != null ) {
        return;
      }

      String suffix = ".tmp";
      if (this.keyObject != null) {
        suffix = "." + this.keyObject.toString() + suffix;
      }

      while (true) {
        parentFile = File.createTempFile("hive-rowcontainer", "");
        boolean success = parentFile.delete() && parentFile.mkdir();
        if (success) {
          break;
        }
        LOG.debug("retry creating tmp row-container directory...");
      }

      tmpFile = File.createTempFile("RowContainer", suffix, parentFile);
      LOG.info("RowContainer created temp file " + tmpFile.getAbsolutePath());
      // Delete the temp file if the JVM terminate normally through Hadoop job
      // kill command.
      // Caveat: it won't be deleted if JVM is killed by 'kill -9'.
      parentFile.deleteOnExit();
      tmpFile.deleteOnExit();

      // rFile = new RandomAccessFile(tmpFile, "rw");
      HiveOutputFormat<?, ?> hiveOutputFormat = tblDesc.getOutputFileFormatClass().newInstance();
      tempOutPath = new Path(tmpFile.toString());
      JobConf localJc = getLocalFSJobConfClone(jc);
      rw = HiveFileFormatUtils.getRecordWriter(this.jobCloneUsingLocalFs,
          hiveOutputFormat, serde.getSerializedClass(), false,
          tblDesc.getProperties(), tempOutPath, reporter);
    } catch (Exception e) {
      clear();
      LOG.error(e.toString(), e);
      throw new HiveException(e);
    }

  }

  protected FSRecordWriter getRecordWriter() {
    return rw;
  }

  protected InputSplit[] getInputSplits() {
    return inputSplits;
  }

  protected boolean endOfCurrentReadBlock() {
    if (tblDesc == null) {
      return false;
    }
    return itrCursor >= this.readBlockSize;
  }

  protected int getCurrentReadBlockSize() {
    return readBlockSize;
  }

  protected void setWriteBlockAsReadBlock() {
    this.itrCursor = 0;
    this.readBlockSize = this.addCursor;
    this.firstReadBlockPointer = this.currentReadBlock;
    currentReadBlock = currentWriteBlock;
  }

  protected org.apache.hadoop.mapred.RecordReader setReaderAtSplit(int splitNum)
      throws IOException {
    JobConf localJc = getLocalFSJobConfClone(jc);
    currentSplitPointer = splitNum;
    if ( rr != null ) {
      rr.close();
    }
    // open record reader to read next split
    rr = inputFormat.getRecordReader(inputSplits[currentSplitPointer], jobCloneUsingLocalFs,
        reporter);
    currentSplitPointer++;
    return rr;
  }

  protected ROW getReadBlockRow(int rowOffset) {
    itrCursor = rowOffset + 1;
    return currentReadBlock[rowOffset];
  }

  protected void resetCurrentReadBlockToFirstReadBlock() {
    currentReadBlock = firstReadBlockPointer;
  }

  protected void resetReadBlocks() {
    this.currentReadBlock = this.currentWriteBlock;
    this.firstReadBlockPointer = currentReadBlock;
  }

  protected void close() throws HiveException {
    clear();
    currentReadBlock = firstReadBlockPointer = currentWriteBlock = null;
  }

}
