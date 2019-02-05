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
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

/**
 * HiveSequenceFileInputFormat.
 *  This input format is used by Fetch Operator. This input format does list status
 *    on list of files (kept in listsToFetch) instead of doing list on whole directory
 *    as done by previously used SequenceFileFormat.
 *    To use this FileFormat make sure to provide the list of files
 * @param <K>
 * @param <V>
 */
public class HiveSequenceFileInputFormat<K extends LongWritable, V extends BytesRefArrayWritable>
    extends SequenceFileInputFormat<K, V> {

  public HiveSequenceFileInputFormat() {
    setMinSplitSize(SequenceFile.SYNC_INTERVAL);
  }

  Set<Path> listsToFetch = null;

  public void setListsToFetch(Set<Path> listsToFetch) {
    this.listsToFetch = listsToFetch;
  }

  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    if(listsToFetch == null || listsToFetch.isEmpty()) {
      return super.listStatus(job);
    }
    List<FileStatus> fsStatusList = new ArrayList<>();
    for(Path path:listsToFetch) {
      FileSystem fs = path.getFileSystem(job);
      FileStatus fsStatus = fs.getFileStatus(path);
      fsStatusList.add(fsStatus);
    }
    FileStatus[] fsStatusArray = new FileStatus[fsStatusList.size()];
    return fsStatusList.toArray(fsStatusArray);
  }
}
