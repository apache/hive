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

package org.apache.hadoop.hive.ql.parse.repl.dump;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test class for Utils.
 */
public class TestUtils {

  @Test
  public void testHdfsSameAndDifferentHost() {
    Path pathOne = new Path("hdfs://localhost:8020/path/to/staging");
    Path pathTwo = new Path("hdfs://localhost:8020/path/to/warehouse");
    assertTrue(Utils.onSameHDFSFileSystem(pathOne, pathTwo));
    pathOne = new Path("hdfs://localhost:8020/path/to/staging");
    pathTwo = new Path("hdfs://localhost:8020");
    assertTrue(Utils.onSameHDFSFileSystem(pathOne, pathTwo));
    pathOne = new Path("hdfs://localhost:8020/path/to/staging");
    pathTwo = new Path("hdfs://otherHost:8020/path/to/warehouse");
    assertFalse(Utils.onSameHDFSFileSystem(pathOne, pathTwo));
    pathOne = new Path("hdfs://localhost:8020/path/to/somedir");
    pathTwo = new Path("hdfs://otherhost:8020/path/to/somedir");
    assertFalse(Utils.onSameHDFSFileSystem(pathOne, pathTwo));
  }

  @Test
  public void testHCFSSameAndDifferentHost() {
    Path pathOne = new Path("hdfs://localhost:8020/path/to/staging");
    Path pathTwo = new Path("s3a://localhost:8020/path/to/warehouse");
    assertFalse(Utils.onSameHDFSFileSystem(pathOne, pathTwo));
    pathOne = new Path("s3a://localhost:8020/path/to/warehouse");
    pathTwo = new Path("s3a://localhost:8020/path/to/warehouse");
    assertFalse(Utils.onSameHDFSFileSystem(pathOne, pathTwo));
    pathOne = new Path("hdfs://localhost:8020/path/to/warehouse");
    pathTwo = new Path("s3a://localhost:8020/path/to/warehouse");
    assertFalse(Utils.onSameHDFSFileSystem(pathOne, pathTwo));
  }
}