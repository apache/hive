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

package org.apache.hadoop.hive.ql.anon.e2e;

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.FileType;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Test;

public class TestOrc extends BaseEndToEndTest {

  private final FileType fileType = FileType.ORC;

  @Test
  public void testNoIx() throws CommandProcessorException {
    for (final ColumnInternalFormat format : ColumnInternalFormat.values()) {
      LOG.info("no ix - testing format: {}", format);
      testNoIx(format, fileType);
    }
  }

  @Test
  public void testBtree() throws CommandProcessorException {
    for (final ColumnInternalFormat format : ColumnInternalFormat.values()) {
      LOG.info("btree - testing format: {}", format);
      testBtree(format, fileType);
    }
  }

  @Test
  public void testDirectory() throws CommandProcessorException {
    for (final ColumnInternalFormat format : ColumnInternalFormat.values()) {
      LOG.info("directory - testing format: {}", format);
      testDirectory(format, fileType);
    }
  }

  @Test
  public void testTabular() throws CommandProcessorException {
    for (final ColumnInternalFormat format : ColumnInternalFormat.values()) {
      LOG.info("tabular - testing format: {}", format);
      testTabular(format, fileType);
    }
  }

}
