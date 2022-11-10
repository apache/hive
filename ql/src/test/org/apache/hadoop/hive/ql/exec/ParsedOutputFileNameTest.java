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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

public class ParsedOutputFileNameTest {
  @Test
  public void testStandardNoAttemptId() {
    ParsedOutputFileName p = ParsedOutputFileName.parse("00001");
    Assert.assertTrue(p.matches());
    Assert.assertNull(p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("00001", p.getPrefixedTaskId());
    Assert.assertNull(p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertNull(p.getSuffix());
  }

  @Test
  public void testStandard() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("00001_02");
    Assert.assertTrue(p.matches());
    Assert.assertNull(p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertNull(p.getSuffix());
    Assert.assertEquals("00001_02_copy_3", p.makeFilenameWithCopyIndex(3));
  }

  @Test
  public void testStandardPrefix() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("(prefix)00001_02");
    Assert.assertTrue(p.matches());
    Assert.assertEquals("(prefix)", p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("(prefix)00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertNull(p.getSuffix());
    Assert.assertEquals("(prefix)00001_02_copy_3", p.makeFilenameWithCopyIndex(3));
  }

  @Test
  public void testStandardSuffix() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("00001_02.snappy.orc");
    Assert.assertTrue(p.matches());
    Assert.assertNull(p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertEquals(".snappy.orc", p.getSuffix());
    Assert.assertEquals("00001_02_copy_3", p.makeFilenameWithCopyIndex(3));
  }

  @Test
  public void testPrefixAndSuffix() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("tmp_(prefix)00001_02.snappy.orc");
    Assert.assertTrue(p.matches());
    Assert.assertEquals("(prefix)", p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("(prefix)00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertEquals(".snappy.orc", p.getSuffix());
    Assert.assertEquals("tmp_(prefix)00001_02_copy_3", p.makeFilenameWithCopyIndex(3));
  }

  @Test
  public void testCopy() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("00001_02_copy_3");
    Assert.assertTrue(p.matches());
    Assert.assertNull(p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertEquals("3", p.getCopyIndex());
    Assert.assertTrue(p.isCopyFile());
    Assert.assertNull(p.getSuffix());
    Assert.assertEquals("00001_02_copy_4", p.makeFilenameWithCopyIndex(4));
  }

  @Test
  public void testCopyAllParts() throws Exception {
    ParsedOutputFileName p = ParsedOutputFileName.parse("tmp_(prefix)00001_02_copy_3.snappy.orc");
    Assert.assertTrue(p.matches());
    Assert.assertEquals("(prefix)", p.getTaskIdPrefix());
    Assert.assertEquals("00001", p.getTaskId());
    Assert.assertEquals("(prefix)00001", p.getPrefixedTaskId());
    Assert.assertEquals("02", p.getAttemptId());
    Assert.assertEquals("3", p.getCopyIndex());
    Assert.assertTrue(p.isCopyFile());
    Assert.assertEquals(".snappy.orc", p.getSuffix());
    Assert.assertEquals("tmp_(prefix)00001_02_copy_4", p.makeFilenameWithCopyIndex(4));
  }

  @Test
  public void testNoMatch() {
    ParsedOutputFileName p = ParsedOutputFileName.parse("ZfsLke");
    Assert.assertFalse(p.matches());
    Assert.assertNull(p.getTaskId());
    Assert.assertNull(p.getPrefixedTaskId());
    Assert.assertNull(p.getTaskIdPrefix());
    Assert.assertNull(p.getAttemptId());
    Assert.assertNull(p.getCopyIndex());
    Assert.assertFalse(p.isCopyFile());
    Assert.assertNull(p.getSuffix());
    try {
      p.makeFilenameWithCopyIndex(1);
      Assert.fail("Expected HiveException");
    } catch(HiveException e) {
    }
  }
}
