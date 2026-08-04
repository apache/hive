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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.common.util.MockFileSystem;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

/**
 * Tests the method MoveTask.flattenUnionSubdirectories().
 */
public class TestMoveTask {
  @Test
  public void flattenUnionSubdirectories() throws IOException, HiveException {
    String initialPath = "/table_users/" + AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "1/000000_0";
    // The flattened name matches ORIGINAL_PATTERN_COPY ([0-9]+_[0-9]+_copy_[0-9]+) so a
    // subsequent non-ACID→ACID conversion isn't rejected by the metastore validator.
    String flattenPath = "/table_users/000000_0_copy_1";

    MockFileSystem.MockFile file1 = new MockFileSystem.MockFile("mock://" + initialPath, 0, new byte[1]);
    MockFileSystem fs = new MockFileSystem(new Configuration(), file1);

    new MoveTask().flattenUnionSubdirectories(new MockFileSystem.MockPath(fs, initialPath));

    assertFalse(fs.exists(new MockFileSystem.MockPath(fs, initialPath)));
    assertTrue(fs.exists(new MockFileSystem.MockPath(fs, flattenPath)));
  }
}
