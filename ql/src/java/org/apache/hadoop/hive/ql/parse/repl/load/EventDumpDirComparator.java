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
package org.apache.hadoop.hive.ql.parse.repl.load;

import java.util.Comparator;
import org.apache.hadoop.fs.FileStatus;

public class EventDumpDirComparator implements Comparator<FileStatus>{
  @Override
  public int compare(FileStatus o1, FileStatus o2) {
    // It is enough to compare the last level sub-directory which has the name as event ID
    String dir1 = o1.getPath().getName();
    String dir2 = o2.getPath().getName();

    // First compare the length and then compare the directory name
    if (dir1.length() > dir2.length()) {
      return 1;
    } else if (dir1.length() < dir2.length()) {
      return -1;
    }
    return dir1.compareTo(dir2);
  }
}
