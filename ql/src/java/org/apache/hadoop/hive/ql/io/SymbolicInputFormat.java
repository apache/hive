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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.TextInputFormat;

public class SymbolicInputFormat implements ReworkMapredInputFormat {

  public void rework(HiveConf job, MapredWork work) throws IOException {
    Map<String, PartitionDesc> pathToParts = work.getMapWork().getPathToPartitionInfo();
    List<String> toRemovePaths = new ArrayList<String>();
    Map<String, PartitionDesc> toAddPathToPart = new HashMap<String, PartitionDesc>();
    Map<String, ArrayList<String>> pathToAliases = work.getMapWork().getPathToAliases();

    for (Map.Entry<String, PartitionDesc> pathPartEntry : pathToParts
        .entrySet()) {
      String path = pathPartEntry.getKey();
      PartitionDesc partDesc = pathPartEntry.getValue();
      // this path points to a symlink path
      if (partDesc.getInputFileFormatClass().equals(
          SymlinkTextInputFormat.class)) {
        // change to TextInputFormat
        partDesc.setInputFileFormatClass(TextInputFormat.class);
        Path symlinkDir = new Path(path);
        FileSystem fileSystem = symlinkDir.getFileSystem(job);
        FileStatus fStatus = fileSystem.getFileStatus(symlinkDir);
        FileStatus[] symlinks = null;
        if (!fStatus.isDir()) {
          symlinks = new FileStatus[] { fStatus };
        } else {
          symlinks = fileSystem.listStatus(symlinkDir);
        }
        toRemovePaths.add(path);
        ArrayList<String> aliases = pathToAliases.remove(path);
        for (FileStatus symlink : symlinks) {
          BufferedReader reader = null;
          try {
            reader = new BufferedReader(new InputStreamReader(
                fileSystem.open(symlink.getPath())));

            partDesc.setInputFileFormatClass(TextInputFormat.class);

            String line;
            while ((line = reader.readLine()) != null) {
              // no check for the line? How to check?
              // if the line is invalid for any reason, the job will fail.
              toAddPathToPart.put(line, partDesc);
              pathToAliases.put(line, aliases);
            }
          } finally {
            org.apache.hadoop.io.IOUtils.closeStream(reader);
          }
        }
      }
    }

    pathToParts.putAll(toAddPathToPart);
    for (String toRemove : toRemovePaths) {
      pathToParts.remove(toRemove);
    }
  }
}
