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
package org.apache.hadoop.hive.ql.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;

/**
 * HiveIndexResult parses the input stream from an index query
 * to generate a list of file splits to query.
 */
public class HiveIndexResult {

  public static final Log l4j =
    LogFactory.getLog(HiveIndexResult.class.getSimpleName());

  // IndexBucket
  static class IBucket {
    private String name = null;
    private final SortedSet<Long> offsets = new TreeSet<Long>();

    public IBucket(String n) {
      name = n;
    }

    public void add(Long offset) {
      offsets.add(offset);
    }

    public String getName() {
      return name;
    }

    public SortedSet<Long> getOffsets() {
      return offsets;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj.getClass() != this.getClass()) {
        return false;
      }
      return (((IBucket) obj).name.compareToIgnoreCase(this.name) == 0);
    }
  }

  JobConf job = null;
  BytesRefWritable[] bytesRef = new BytesRefWritable[2];
  boolean ignoreHdfsLoc = false;

  public HiveIndexResult(List<String> indexFiles, JobConf conf) throws IOException,
      HiveException {
    job = conf;

    bytesRef[0] = new BytesRefWritable();
    bytesRef[1] = new BytesRefWritable();
    ignoreHdfsLoc = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_INDEX_IGNORE_HDFS_LOC);

    if (indexFiles != null && indexFiles.size() > 0) {
      List<Path> paths = new ArrayList<Path>();
      for (String indexFile : indexFiles) {
        Path indexFilePath = new Path(indexFile);
        FileSystem fs = indexFilePath.getFileSystem(conf);
        FileStatus indexStat = fs.getFileStatus(indexFilePath);
        if (indexStat.isDir()) {
          FileStatus[] fss = fs.listStatus(indexFilePath);
          for (FileStatus f : fss) {
            paths.add(f.getPath());
          }
        } else {
          paths.add(indexFilePath);
        }
      }

      long maxEntriesToLoad = HiveConf.getLongVar(conf, HiveConf.ConfVars.HIVE_INDEX_COMPACT_QUERY_MAX_ENTRIES);
      if (maxEntriesToLoad < 0) {
        maxEntriesToLoad=Long.MAX_VALUE;
      }

      long lineCounter = 0;
      for (Path indexFinalPath : paths) {
        FileSystem fs = indexFinalPath.getFileSystem(conf);
        FSDataInputStream ifile = fs.open(indexFinalPath);
        LineReader lr = new LineReader(ifile, conf);
        try {
          Text line = new Text();
          while (lr.readLine(line) > 0) {
            if (++lineCounter > maxEntriesToLoad) {
              throw new HiveException("Number of compact index entries loaded during the query exceeded the maximum of " + maxEntriesToLoad
                  + " set in " + HiveConf.ConfVars.HIVE_INDEX_COMPACT_QUERY_MAX_ENTRIES.varname);
            }
            add(line);
          }
        }
        finally {
          // this will close the input stream
          lr.close();
        }
      }
    }
  }

  Map<String, IBucket> buckets = new HashMap<String, IBucket>();

  private void add(Text line) throws HiveException {
    String l = line.toString();
    byte[] bytes = l.getBytes();
    int firstEnd = 0;
    int i = 0;
    for (int index = 0; index < bytes.length; index++) {
      if (bytes[index] == LazySimpleSerDe.DefaultSeparators[0]) {
        i++;
        firstEnd = index;
      }
    }
    if (i > 1) {
      throw new HiveException(
          "Bad index file row (index file should only contain two columns: bucket_file_name and offset lists.) ."
              + line.toString());
    }
    String bucketFileName = new String(bytes, 0, firstEnd);

    if (ignoreHdfsLoc) {
      Path tmpPath = new Path(bucketFileName);
      bucketFileName = tmpPath.toUri().getPath();
    }
    IBucket bucket = buckets.get(bucketFileName);
    if (bucket == null) {
      bucket = new IBucket(bucketFileName);
      buckets.put(bucketFileName, bucket);
    }

    int currentStart = firstEnd + 1;
    int currentEnd = firstEnd + 1;
    for (; currentEnd < bytes.length; currentEnd++) {
      if (bytes[currentEnd] == LazySimpleSerDe.DefaultSeparators[1]) {
        String one_offset = new String(bytes, currentStart, currentEnd
            - currentStart);
        Long offset = Long.parseLong(one_offset);
        bucket.getOffsets().add(offset);
        currentStart = currentEnd + 1;
      }
    }
    String one_offset = new String(bytes, currentStart, currentEnd
        - currentStart);
    bucket.getOffsets().add(Long.parseLong(one_offset));
  }

  public boolean contains(FileSplit split) throws HiveException {

    if (buckets == null) {
      return false;
    }
    String bucketName = split.getPath().toString();
    IBucket bucket = buckets.get(bucketName);
    if (bucket == null) {
      bucketName = split.getPath().toUri().getPath();
      bucket = buckets.get(bucketName);
      if (bucket == null) {
        return false;
      }
    }

    for (Long offset : bucket.getOffsets()) {
      if ((offset >= split.getStart())
          && (offset <= split.getStart() + split.getLength())) {
        return true;
      }
    }
    return false;
  }
}
