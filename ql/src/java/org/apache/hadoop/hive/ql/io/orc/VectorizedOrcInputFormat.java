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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * A MapReduce/Hive input format for ORC files.
 */
public class VectorizedOrcInputFormat extends FileInputFormat<NullWritable, VectorizedRowBatch>
    implements InputFormatChecker, VectorizedInputFormatInterface {

  private static class VectorizedOrcRecordReader
      implements RecordReader<NullWritable, VectorizedRowBatch> {
    private final org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
    private final long offset;
    private final long length;
    private float progress = 0.0f;
    private VectorizedRowBatchCtx rbCtx;
    private boolean addPartitionCols = true;

    VectorizedOrcRecordReader(Reader file, Configuration conf,
        FileSplit fileSplit) throws IOException {

      this.offset = fileSplit.getStart();
      this.length = fileSplit.getLength();
      this.reader = file.rows(offset, length,
          findIncludedColumns(file.getTypes(), conf));

      try {
        rbCtx = new VectorizedRowBatchCtx();
        rbCtx.Init(conf, fileSplit);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {

      if (!reader.hasNext()) {
        return false;
      }
      try {
        // Check and update partition cols if necessary. Ideally, this should be done
        // in CreateValue as the partition is constant per split. But since Hive uses
        // CombineHiveRecordReader and
        // as this does not call CreateValue for each new RecordReader it creates, this check is
        // required in next()
        if (addPartitionCols) {
          rbCtx.AddPartitionColsToBatch(value);
          addPartitionCols = false;
        }
        reader.nextBatch(value);
        rbCtx.ConvertRowBatchBlobToVectorizedBatch((Object) value, value.size, value);
      } catch (Exception e) {
        new RuntimeException(e);
      }
      progress = reader.getProgress();
      return true;
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public VectorizedRowBatch createValue() {
      VectorizedRowBatch result = null;
      try {
        result = rbCtx.CreateVectorizedRowBatch();
      } catch (HiveException e) {
        new RuntimeException("Error creating a batch", e);
      }
      return result;
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
  }

  public VectorizedOrcInputFormat() {
    // just set a really small lower bound
    setMinSplitSize(16 * 1024);
  }

  /**
   * Recurse down into a type subtree turning on all of the sub-columns.
   *
   * @param types
   *          the types of the file
   * @param result
   *          the global view of columns that should be included
   * @param typeId
   *          the root of tree to enable
   */
  private static void includeColumnRecursive(List<OrcProto.Type> types,
      boolean[] result,
      int typeId) {
    result[typeId] = true;
    OrcProto.Type type = types.get(typeId);
    int children = type.getSubtypesCount();
    for (int i = 0; i < children; ++i) {
      includeColumnRecursive(types, result, type.getSubtypes(i));
    }
  }

  /**
   * Take the configuration and figure out which columns we need to include.
   *
   * @param types
   *          the types of the file
   * @param conf
   *          the configuration
   * @return true for each column that should be included
   */
  private static boolean[] findIncludedColumns(List<OrcProto.Type> types,
      Configuration conf) {
    String includedStr =
        conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
    if (includedStr == null || includedStr.trim().length() == 0) {
      return null;
    } else {
      int numColumns = types.size();
      boolean[] result = new boolean[numColumns];
      result[0] = true;
      OrcProto.Type root = types.get(0);
      List<Integer> included = ColumnProjectionUtils.getReadColumnIDs(conf);
      for (int i = 0; i < root.getSubtypesCount(); ++i) {
        if (included.contains(i)) {
          includeColumnRecursive(types, result, root.getSubtypes(i));
        }
      }
      // if we are filtering at least one column, return the boolean array
      for (boolean include : result) {
        if (!include) {
          return result;
        }
      }
      return null;
    }
  }

  @Override
  public RecordReader<NullWritable, VectorizedRowBatch>
      getRecordReader(InputSplit inputSplit, JobConf conf,
          Reporter reporter) throws IOException {
    FileSplit fileSplit = (FileSplit) inputSplit;
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    reporter.setStatus(fileSplit.toString());
    return new VectorizedOrcRecordReader(OrcFile.createReader(fs, path), conf, fileSplit);
  }

  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf,
      ArrayList<FileStatus> files
      ) throws IOException {
    if (files.size() <= 0) {
      return false;
    }
    for (FileStatus file : files) {
      try {
        OrcFile.createReader(fs, file.getPath());
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }
}
