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
package org.apache.hadoop.hive.metastore.tools.schematool;

import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SchemaToolTaskRebuildIndexes} orchestration logic.
 */
@Category(MetastoreUnitTest.class)
public class TestSchemaToolTaskRebuildIndexes {

  private static final IndexInfo UNIQUE_INDEX = new IndexInfo(
      "idx", "tbl", true, false, List.of("col"));

  private static final IndexInfo NON_UNIQUE_INDEX = new IndexInfo(
      "idx2", "tbl", false, false, List.of("col"));

  private MetastoreSchemaTool schemaTool;
  private SchemaToolTaskRebuildIndexes task;

  @Before
  public void setUp() {
    schemaTool = mock(MetastoreSchemaTool.class);
    task = new SchemaToolTaskRebuildIndexes();
    task.schemaTool = schemaTool;
  }

  @Test
  public void duplicatesBlockRebuildAndNeverCallRebuildIndex() throws Exception {
    IndexRebuilder rebuilder = mock(IndexRebuilder.class);
    when(rebuilder.loadIndexes()).thenReturn(List.of(UNIQUE_INDEX));
    when(rebuilder.findDuplicates(UNIQUE_INDEX)).thenReturn(2L);

    assertThrows(HiveMetaException.class, () -> task.executeWithRebuilder(rebuilder));

    verify(rebuilder, never()).rebuildIndex(UNIQUE_INDEX);
  }

  @Test
  public void dryRunCallsDescribeButNeverCallsRebuildIndex() throws Exception {
    IndexRebuilder rebuilder = mock(IndexRebuilder.class);
    when(rebuilder.loadIndexes()).thenReturn(List.of(NON_UNIQUE_INDEX));
    when(rebuilder.findDuplicates(NON_UNIQUE_INDEX)).thenReturn(0L);
    when(rebuilder.describeRebuildDDL(NON_UNIQUE_INDEX)).thenReturn("DROP ...\nCREATE ...");
    when(schemaTool.isDryRun()).thenReturn(true);

    task.executeWithRebuilder(rebuilder);

    verify(rebuilder).describeRebuildDDL(NON_UNIQUE_INDEX);
    verify(rebuilder, never()).rebuildIndex(NON_UNIQUE_INDEX);
  }

  @Test
  public void normalRunCallsRebuildIndex() throws Exception {
    IndexRebuilder rebuilder = mock(IndexRebuilder.class);
    when(rebuilder.loadIndexes()).thenReturn(List.of(NON_UNIQUE_INDEX));
    when(rebuilder.findDuplicates(NON_UNIQUE_INDEX)).thenReturn(0L);
    when(rebuilder.describeRebuildDDL(NON_UNIQUE_INDEX)).thenReturn("DROP ...\nCREATE ...");
    when(schemaTool.isDryRun()).thenReturn(false);

    task.executeWithRebuilder(rebuilder);

    verify(rebuilder).rebuildIndex(NON_UNIQUE_INDEX);
  }
}
