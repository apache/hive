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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PTFDeserializer;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.util.Progressable;


/**
 * Extends the RowContainer functionality to provide random access <code>getAt(i)</code>.
 * It extends RowContainer behavior in the following ways:
 * <ol>
 * <li> You must continue to call <b>first</b> to signal the transition from writing to the
 * Container to reading from it.
 * <li> As rows are being added, positions at which a <i>spill</i> occurs is captured as a
 * BlockInfo object. At this point it captures the offset in the File at which the current
 * Block will be written.
 * <li> When first is called: we associate with each BlockInfo the File Split that it
 * occurs in.
 * <li> So in order to read a random row from the Container we do the following:
 * <ul>
 * <li> Convert the row index into a block number. This is easy because all blocks are
 * the same size, given by the <code>blockSize</code>
 * <li> The corresponding BlockInfo tells us the Split that this block starts in. Also
 * by looking at the next Block in the BlockInfos list, we know which Split this block ends in.
 * <li> So we arrange to read all the Splits that contain rows for this block. For the first
 * Split we seek to the startOffset that we captured in BlockInfo.
 * <li> So after reading the Splits, all rows in this block are in the 'currentReadBlock'
 * </ul>
 * <li> We track the span of the currentReadBlock, using
 * <code>currentReadBlockStartRow,blockSize</code>. So if a row is requested in this span,
 * we don't need to read rows from disk.
 * <li> If the requested row is in the 'last' block; we point the currentReadBlock to
 * the currentWriteBlock; the same as what RowContainer does.
 * <li> the <code>getAt</code> leaves the Container in the same state as a
 * <code>next</code> call; so a getAt and next calls can be interspersed.
 * </ol>
 */
public class PTFRowContainer<Row extends List<Object>> extends RowContainer<Row> {

  ArrayList<BlockInfo> blockInfos;
  int currentReadBlockStartRow;

  public PTFRowContainer(int bs, Configuration jc, Reporter reporter
      ) throws HiveException {
    super(bs, jc, reporter);
    blockInfos = new ArrayList<PTFRowContainer.BlockInfo>();
  }

  @Override
  public void add(Row t) throws HiveException {
    if ( willSpill() ) {
      setupWriter();
      PTFRecordWriter rw = (PTFRecordWriter) getRecordWriter();
      BlockInfo blkInfo = new BlockInfo();
      try {
        blkInfo.startOffset = rw.outStream.getLength();
        blockInfos.add(blkInfo);
      } catch(IOException e) {
        clear();
        LOG.error(e.toString(), e);
        throw new HiveException(e);
      }
    }
    super.add(t);
  }

  @Override
  public Row first() throws HiveException {
    Row r = super.first();

    if ( blockInfos.size() > 0 ) {
      InputSplit[] inputSplits = getInputSplits();
      FileSplit fS = null;
      BlockInfo bI = blockInfos.get(0);
      bI.startingSplit = 0;
      int i = 1;
      bI = i < blockInfos.size() ? blockInfos.get(i) : null;
      for(int j=1; j < inputSplits.length && bI != null; j++) {
        fS = (FileSplit) inputSplits[j];
        while (bI != null && bI.startOffset < fS.getStart() ) {
          bI.startingSplit = j - 1;
          i++;
          bI = i < blockInfos.size() ? blockInfos.get(i) : null;
        }
      }

      while ( i < blockInfos.size()  ) {
        bI = blockInfos.get(i);
        bI.startingSplit = inputSplits.length - 1;
        i++;
      }
    }

    currentReadBlockStartRow = 0;
    return r;
  }

  @Override
  public Row next() throws HiveException {
    boolean endOfCurrBlock = endOfCurrentReadBlock();
    if ( endOfCurrBlock ) {
      currentReadBlockStartRow += getCurrentReadBlockSize();
    }
    return super.next();
  }

  @Override
  public void clear() throws HiveException {
    super.clear();
    resetReadBlocks();
    blockInfos = new ArrayList<PTFRowContainer.BlockInfo>();
  }

  @Override
  public void close() throws HiveException {
    super.close();
    blockInfos = null;
  }

  public Row getAt(int rowIdx) throws HiveException {
    int blockSize = getBlockSize();
    if ( rowIdx < currentReadBlockStartRow || rowIdx >= currentReadBlockStartRow + blockSize ) {
      readBlock(getBlockNum(rowIdx));
    }
    return getReadBlockRow(rowIdx - currentReadBlockStartRow);
  }

  private int numBlocks() {
    return blockInfos.size() + 1;
  }

  private int getBlockNum(int rowIdx) {
    int blockSize = getBlockSize();
    return rowIdx / blockSize;
  }

  private void readBlock(int blockNum)  throws HiveException {
    currentReadBlockStartRow = getBlockSize() * blockNum;

    if ( blockNum == numBlocks() - 1 ) {
      setWriteBlockAsReadBlock();
      return;
    }

    resetCurrentReadBlockToFirstReadBlock();

    BlockInfo bI = blockInfos.get(blockNum);
    int startSplit = bI.startingSplit;
    int endSplit = startSplit;
    if ( blockNum != blockInfos.size() - 1) {
      endSplit = blockInfos.get(blockNum+1).startingSplit;
    }

    try {
      int readIntoOffset = 0;
      for(int i = startSplit; i <= endSplit; i++ ) {
        org.apache.hadoop.mapred.RecordReader rr = setReaderAtSplit(i);
        if ( i == startSplit ) {
          ((PTFSequenceFileRecordReader)rr).seek(bI.startOffset);
        }
        nextBlock(readIntoOffset);
        readIntoOffset = getCurrentReadBlockSize();
      }

    } catch(Exception e) {
      clear();
      LOG.error(e.toString(), e);
      if ( e instanceof HiveException ) {
        throw (HiveException) e;
      }
      throw new HiveException(e);
    }
  }

  private static class BlockInfo {
    // position in file where the first row in this block starts
    long startOffset;

    // inputSplitNum that contains the first row in this block.
    int startingSplit;
  }

  public static TableDesc createTableDesc(StructObjectInspector oI) {
    Map<String,String> props = new HashMap<String,String>();
    PTFDeserializer.addOIPropertiestoSerDePropsMap(oI, props);
    String colNames = props.get(serdeConstants.LIST_COLUMNS);
    String colTypes = props.get(serdeConstants.LIST_COLUMN_TYPES);
    TableDesc tblDesc = new TableDesc(
        PTFSequenceFileInputFormat.class, PTFHiveSequenceFileOutputFormat.class,
        Utilities.makeProperties(
        serdeConstants.SERIALIZATION_FORMAT, ""+ Utilities.ctrlaCode,
        serdeConstants.LIST_COLUMNS, colNames.toString(),
        serdeConstants.LIST_COLUMN_TYPES,colTypes.toString(),
        serdeConstants.SERIALIZATION_LIB,LazyBinarySerDe.class.getName()));
    return tblDesc;
  }


  private static class PTFRecordWriter implements FSRecordWriter {
    BytesWritable EMPTY_KEY = new BytesWritable();

    SequenceFile.Writer outStream;

    public PTFRecordWriter(SequenceFile.Writer outStream) {
      this.outStream = outStream;
    }

    public void write(Writable r) throws IOException {
      outStream.append(EMPTY_KEY, r);
    }

    public void close(boolean abort) throws IOException {
      outStream.close();
    }
  }

  public static class PTFHiveSequenceFileOutputFormat<K,V>
    extends HiveSequenceFileOutputFormat<K,V> {

    @Override
    public FSRecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
        Class<? extends Writable> valueClass, boolean isCompressed,
        Properties tableProperties, Progressable progress) throws IOException {

      FileSystem fs = finalOutPath.getFileSystem(jc);
      final SequenceFile.Writer outStream = Utilities.createSequenceWriter(jc,
          fs, finalOutPath, BytesWritable.class, valueClass, isCompressed);

      return new PTFRecordWriter(outStream);
    }

  }

  public static class PTFSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {

    public PTFSequenceFileInputFormat() {
      super();
    }

    @Override
    public org.apache.hadoop.mapred.RecordReader<K, V> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter)
        throws IOException {
      reporter.setStatus(split.toString());
      return new PTFSequenceFileRecordReader<K, V>(job, (FileSplit) split);
    }
  }

  public static class PTFSequenceFileRecordReader<K,V> extends SequenceFileRecordReader<K, V> {

    public PTFSequenceFileRecordReader(Configuration conf, FileSplit split)
        throws IOException {
      super(conf, split);

    }
    @Override
    public void seek(long pos) throws IOException {
      super.seek(pos);
    }
  }
}
