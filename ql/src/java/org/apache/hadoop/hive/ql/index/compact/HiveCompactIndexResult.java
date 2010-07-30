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
package org.apache.hadoop.hive.ql.index.compact;

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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;

public class HiveCompactIndexResult {

  public static final Log l4j = LogFactory.getLog("HiveCompactIndexResult");

  // IndexBucket
  static class IBucket {
    private String name = null;
    private SortedSet<Long> offsets = new TreeSet<Long>();

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

    public boolean equals(Object obj) {
      if (obj.getClass() != this.getClass()) {
        return false;
      }
      return (((IBucket) obj).name.compareToIgnoreCase(this.name) == 0);
    }
  }

  JobConf job = null;
  BytesRefWritable[] bytesRef = new BytesRefWritable[2];

  public HiveCompactIndexResult(String indexFile, JobConf conf) throws IOException,
      HiveException {
    job = conf;

    bytesRef[0] = new BytesRefWritable();
    bytesRef[1] = new BytesRefWritable();

    if (indexFile != null) {
      Path indexFilePath = new Path(indexFile);
      FileSystem fs = FileSystem.get(conf);
      FileStatus indexStat = fs.getFileStatus(indexFilePath);
      List<Path> paths = new ArrayList<Path>();
      if (indexStat.isDir()) {
        FileStatus[] fss = fs.listStatus(indexFilePath);
        for (FileStatus f : fss) {
          paths.add(f.getPath());
        }
      } else {
        paths.add(indexFilePath);
      }

      for (Path indexFinalPath : paths) {
        FSDataInputStream ifile = fs.open(indexFinalPath);
        LineReader lr = new LineReader(ifile, conf);
        Text line = new Text();
        while (lr.readLine(line) > 0) {
          add(line);
        }
        // this will close the input stream
        lr.close();
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
