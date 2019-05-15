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

package org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpTask.ConstraintFileType;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.EximUtil;

public class ConstraintEventsIterator implements Iterator<FSConstraintEvent> {
  private FileStatus[] dbDirs;
  private int currentDbIndex;
  private FileStatus[] constraintFiles = null;
  private int currentConstraintIndex;
  private FileSystem fs;
  private Path path;
  private ConstraintFileType mode = ConstraintFileType.COMMON;

  public ConstraintEventsIterator(String dumpDirectory, HiveConf hiveConf) throws IOException {
    path = new Path(dumpDirectory);
    fs = path.getFileSystem(hiveConf);
  }

  private FileStatus[] listConstraintFilesInDBDir(FileSystem fs, Path dbDir, String prefix) {
    try {
      return fs.listStatus(new Path(dbDir, ReplUtils.CONSTRAINTS_ROOT_DIR_NAME), new PathFilter() {
        public boolean accept(Path p) {
          return p.getName().startsWith(prefix);
        }
      });
    } catch (FileNotFoundException e) {
      return new FileStatus[]{};
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  boolean hasNext(ConstraintFileType type) {
    if (dbDirs == null) {
      try {
        dbDirs = fs.listStatus(path, EximUtil.getDirectoryFilter(fs));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      currentDbIndex = 0;
      if (dbDirs.length != 0) {
        currentConstraintIndex = 0;
        constraintFiles = listConstraintFilesInDBDir(fs, dbDirs[0].getPath(), type.getPrefix());
      }
    }
    if ((currentDbIndex < dbDirs.length) && (currentConstraintIndex < constraintFiles.length)) {
      return true;
    }
    while ((currentDbIndex < dbDirs.length) && (currentConstraintIndex == constraintFiles.length)) {
      currentDbIndex ++;
      if (currentDbIndex < dbDirs.length) {
        currentConstraintIndex = 0;
        constraintFiles = listConstraintFilesInDBDir(fs, dbDirs[currentDbIndex].getPath(), type.getPrefix());
      } else {
        constraintFiles = null;
      }
    }
    return constraintFiles != null;
  }

  @Override
  public boolean hasNext() {
    if (mode == ConstraintFileType.COMMON) {
      if (hasNext(ConstraintFileType.COMMON)) {
        return true;
      } else {
        // Switch to iterate foreign keys
        mode = ConstraintFileType.FOREIGNKEY;
        currentDbIndex = 0;
        currentConstraintIndex = 0;
        dbDirs = null;
      }
    }
    return hasNext(ConstraintFileType.FOREIGNKEY);
  }

  @Override
  public FSConstraintEvent next() {
    int thisIndex = currentConstraintIndex;
    currentConstraintIndex++;
    return new FSConstraintEvent(constraintFiles[thisIndex].getPath());
  }
}
