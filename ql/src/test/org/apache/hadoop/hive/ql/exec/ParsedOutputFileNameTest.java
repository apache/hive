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

import org.junit.Assert;
import org.junit.Test;

public class ParsedOutputFileNameTest {
  @Test
  public void testStandard() {
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
  public void testStandardPrefix() {
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
  public void testStandardSuffix() {
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
  public void testPrefixAndSuffix() {
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
  public void testCopy() {
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
  public void testCopyAllParts() {
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
}
