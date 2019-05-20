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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestEventDumpDirComparator {

  @Test
  public void fileStatusArraySortingWithEventDumpDirComparator() {
    int size = 30;
    FileStatus[] dirList = new FileStatus[size];

    for (int i = 0; i < size; i++){
      dirList[i] = new FileStatus(5, true, 1, 64, 100,
              new Path("hdfs://tmp/"+Integer.toString(size-i)));
    }

    Arrays.sort(dirList, new EventDumpDirComparator());
    for (int i = 0; i < 30; i++){
      assertTrue(dirList[i].getPath().getName().equalsIgnoreCase(Integer.toString(i+1)));
    }
  }
}
