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

package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.exec.tez.HiveSplitGenerator.InputSplitComparator;
import static org.junit.Assert.assertEquals;

public class InputSplitComparatorTest {

  private static final String[] EMPTY = new String[]{};

  @Test
  public void testCompare1() throws Exception {
    FileSplit split1 = new FileSplit(new Path("/abc/def"), 2000L, 500L, EMPTY);
    FileSplit split2 = new FileSplit(new Path("/abc/def"), 1000L, 500L, EMPTY);
    InputSplitComparator comparator = new InputSplitComparator();
    assertEquals(1, comparator.compare(split1, split2));
  }
}
