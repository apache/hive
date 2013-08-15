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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A MapReduce/Hive input format for ORC files.
 */
public class OrcInputFormat  extends FileInputFormat<NullWritable, OrcStruct>
  implements InputFormatChecker {

  private static final Log LOG = LogFactory.getLog(OrcInputFormat.class);

  private static class OrcRecordReader
      implements RecordReader<NullWritable, OrcStruct> {
    private final org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
    private final long offset;
    private final long length;
    private final int numColumns;
    private float progress = 0.0f;

    OrcRecordReader(Reader file, Configuration conf,
                    long offset, long length) throws IOException {
      String serializedPushdown = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
      String columnNamesString =
          conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
      String[] columnNames = null;
      SearchArgument sarg = null;
      List<OrcProto.Type> types = file.getTypes();
      if (types.size() == 0) {
        numColumns = 0;
      } else {
        numColumns = types.get(0).getSubtypesCount();
      }
      columnNames = new String[types.size()];
      LOG.info("included column ids = " +
          conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "null"));
      LOG.info("included columns names = " +
          conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "null"));
      boolean[] includeColumn = findIncludedColumns(types, conf);
      if (serializedPushdown != null && columnNamesString != null) {
        sarg = SearchArgument.FACTORY.create
            (Utilities.deserializeExpression(serializedPushdown, conf));
        LOG.info("ORC pushdown predicate: " + sarg);
        String[] neededColumnNames = columnNamesString.split(",");
        int i = 0;
        for(int columnId: types.get(0).getSubtypesList()) {
          if (includeColumn[columnId]) {
            columnNames[columnId] = neededColumnNames[i++];
          }
        }
      } else {
        LOG.info("No ORC pushdown predicate");
      }
      this.reader = file.rows(offset, length,includeColumn, sarg, columnNames);
      this.offset = offset;
      this.length = length;
    }

    @Override
    public boolean next(NullWritable key, OrcStruct value) throws IOException {
      if (reader.hasNext()) {
        reader.next(value);
        progress = reader.getProgress();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public OrcStruct createValue() {
      return new OrcStruct(numColumns);
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

  public OrcInputFormat() {
    // just set a really small lower bound
    setMinSplitSize(16 * 1024);
  }

  /**
   * Recurse down into a type subtree turning on all of the sub-columns.
   * @param types the types of the file
   * @param result the global view of columns that should be included
   * @param typeId the root of tree to enable
   */
  private static void includeColumnRecursive(List<OrcProto.Type> types,
                                             boolean[] result,
                                             int typeId) {
    result[typeId] = true;
    OrcProto.Type type = types.get(typeId);
    int children = type.getSubtypesCount();
    for(int i=0; i < children; ++i) {
      includeColumnRecursive(types, result, type.getSubtypes(i));
    }
  }

  /**
   * Take the configuration and figure out which columns we need to include.
   * @param types the types of the file
   * @param conf the configuration
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
      for(int i=0; i < root.getSubtypesCount(); ++i) {
        if (included.contains(i)) {
          includeColumnRecursive(types, result, root.getSubtypes(i));
        }
      }
      // if we are filtering at least one column, return the boolean array
      for(boolean include: result) {
        if (!include) {
          return result;
        }
      }
      return null;
    }
  }

  @Override
  public RecordReader<NullWritable, OrcStruct>
      getRecordReader(InputSplit inputSplit, JobConf conf,
                      Reporter reporter) throws IOException {
    FileSplit fileSplit = (FileSplit) inputSplit;
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    reporter.setStatus(fileSplit.toString());
    return new OrcRecordReader(OrcFile.createReader(fs, path), conf,
                               fileSplit.getStart(), fileSplit.getLength());
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
