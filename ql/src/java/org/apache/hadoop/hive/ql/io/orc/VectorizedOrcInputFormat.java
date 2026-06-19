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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketIdentifier;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.RowPositionAwareVectorizedRecordReader;
import org.apache.hadoop.hive.ql.io.SelfDescribingInputFormatInterface;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.TypeDescription;

/**
 * A MapReduce/Hive input format for ORC files.
 */
public class VectorizedOrcInputFormat extends FileInputFormat<NullWritable, VectorizedRowBatch>
    implements InputFormatChecker, VectorizedInputFormatInterface,
    SelfDescribingInputFormatInterface {

  static class VectorizedOrcRecordReader
      implements RecordReader<NullWritable, VectorizedRowBatch>, RowPositionAwareVectorizedRecordReader {
    private final org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
    private final long offset;
    private final long length;
    private float progress = 0.0f;
    private VectorizedRowBatchCtx rbCtx;
    private final Object[] partitionValues;
    private boolean addPartitionCols = true;
    private final BucketIdentifier bucketIdentifier;

    VectorizedOrcRecordReader(Reader file, Configuration conf,
        FileSplit fileSplit) throws IOException {

      boolean isAcidRead = AcidUtils.isFullAcidScan(conf);
      if (isAcidRead) {
        OrcInputFormat.raiseAcidTablesMustBeReadWithAcidReaderException(conf);
      }

      rbCtx = Utilities.getVectorizedRowBatchCtx(conf);
      /**
       * Do we have schema on read in the configuration variables?
       */
      int dataColumns = rbCtx.getDataColumnCount();
      String orcSchemaOverrideString = conf.get(ColumnProjectionUtils.ORC_SCHEMA_STRING);
      TypeDescription schema = orcSchemaOverrideString == null ?
          OrcInputFormat.getDesiredRowTypeDescr(conf, false, dataColumns) :
          TypeDescription.fromString(orcSchemaOverrideString);
      if (schema == null) {
        schema = file.getSchema();
        // Even if the user isn't doing schema evolution, cut the schema
        // to the desired size.
        if (schema.getCategory() == TypeDescription.Category.STRUCT &&
            schema.getChildren().size() > dataColumns) {
          schema = schema.clone();
          List<TypeDescription> children = schema.getChildren();
          for(int c = children.size() - 1; c >= dataColumns; --c) {
            children.remove(c);
          }
        }
      }
      List<OrcProto.Type> types = OrcUtils.getOrcTypes(schema);
      Reader.Options options = new Reader.Options(conf).schema(schema);

      this.offset = fileSplit.getStart();
      this.length = fileSplit.getLength();
      options.range(offset, length);
      options.include(OrcInputFormat.genIncludedColumns(schema, conf));
      OrcInputFormat.setSearchArgument(options, types, conf, true);

      this.reader = file.rowsOptions(options, conf);

      int partitionColumnCount = rbCtx.getPartitionColumnCount();
      if (partitionColumnCount > 0) {
        partitionValues = new Object[partitionColumnCount];
        rbCtx.getPartitionValues(rbCtx, conf, fileSplit, partitionValues);
      } else {
        partitionValues = null;
      }

      this.bucketIdentifier = BucketIdentifier.from(conf, fileSplit.getPath());
    }

    @Override
    public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {

      try {
        VectorizedBatchUtil.resetNonPartitionColumns(value);
        // Check and update partition cols if necessary. Ideally, this should be done
        // in CreateValue as the partition is constant per split. But since Hive uses
        // CombineHiveRecordReader and
        // as this does not call CreateValue for each new RecordReader it creates, this check is
        // required in next()
        if (addPartitionCols) {
          if (partitionValues != null) {
            rbCtx.addPartitionColsToBatch(value, partitionValues);
          }
          addPartitionCols = false;
        }
        if (!reader.nextBatch(value)) {
          return false;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      progress = reader.getProgress();

      if (bucketIdentifier != null) {
        rbCtx.setBucketAndWriteIdOf(value, bucketIdentifier);
      }

      return true;
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public VectorizedRowBatch createValue() {
      return rbCtx.createVectorizedRowBatch();
    }

    @Override
    public long getPos() throws IOException {
      return offset + (long) (progress * length);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return progress;
    }

    @Override
    public long getRowNumber() throws IOException {
      return reader.getRowNumber();
    }
  }

  public VectorizedOrcInputFormat() {
    // just set a really small lower bound
    setMinSplitSize(16 * 1024);
  }

  @Override
  public RecordReader<NullWritable, VectorizedRowBatch>
      getRecordReader(InputSplit inputSplit, JobConf conf,
          Reporter reporter) throws IOException {
    FileSplit fSplit = (FileSplit)inputSplit;
    reporter.setStatus(fSplit.toString());

    Path path = fSplit.getPath();

    OrcFile.ReaderOptions opts = OrcFile.readerOptions(conf);
    if(fSplit instanceof OrcSplit){
      OrcSplit orcSplit = (OrcSplit) fSplit;
      if (orcSplit.hasFooter()) {
        opts.orcTail(orcSplit.getOrcTail());
      }
      opts.maxLength(orcSplit.getFileLength());
    }
    Reader reader = OrcFile.createReader(path, opts);
    return new VectorizedOrcRecordReader(reader, conf, fSplit);
  }

  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf,
      List<FileStatus> files
      ) throws IOException {
    if (files.size() <= 0) {
      return false;
    }
    for (FileStatus file : files) {
      try (Reader notUsed = OrcFile.createReader(file.getPath(), OrcFile.readerOptions(conf).filesystem(fs))) {
        // We do not use the reader itself. We just check if we can open the file.
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }

  @Override
  public VectorizedSupport.Support[] getSupportedFeatures() {
    return new VectorizedSupport.Support[] {VectorizedSupport.Support.DECIMAL_64};
  }
}
