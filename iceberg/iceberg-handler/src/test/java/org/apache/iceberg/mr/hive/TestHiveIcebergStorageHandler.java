/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hive.common.type.SnapshotContext;
import org.apache.iceberg.Snapshot;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveIcebergStorageHandler {

  @Mock
  private Snapshot anySnapshot;
  @Mock
  private Snapshot appendSnapshot;
  @Mock
  private Snapshot deleteSnapshot;

  @Before
  public void before() {
    when(anySnapshot.snapshotId()).thenReturn(42L);
    Mockito.lenient().when(appendSnapshot.snapshotId()).thenReturn(20L);
    when(appendSnapshot.operation()).thenReturn("append");
    Mockito.lenient().when(deleteSnapshot.snapshotId()).thenReturn(100L);
    when(deleteSnapshot.operation()).thenReturn("delete");
  }

  @Test
  public void testHasAppendsOnlyReturnsNullWhenTableIsEmpty() {
    SnapshotContext since = new SnapshotContext(42);

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(Collections.emptyList(), since);

    assertThat(result, is(nullValue()));
  }

  @Test
  public void testHasAppendsOnlyReturnsNullWhenTableIsEmptyAndGivenSnapShotIsNull() {
    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(Collections.emptyList(), null);

    assertThat(result, is(true));
  }

  @Test
  public void testHasAppendsOnlyTrueWhenGivenSnapShotIsNull() {
    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(singletonList(appendSnapshot), null);

    assertThat(result, is(true));
  }

  @Test
  public void testHasAppendsOnlyFalseWhenGivenSnapShotIsNullButHasNonAppend() {
    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(asList(appendSnapshot, deleteSnapshot), null);

    assertThat(result, is(false));
  }

  @Test
  public void testHasAppendsOnlyTrueWhenOnlyAppendsAfterGivenSnapshot() {
    SnapshotContext since = new SnapshotContext(42);
    List<Snapshot> snapshotList = asList(anySnapshot, appendSnapshot);

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(snapshotList, since);

    assertThat(result, is(true));
  }

  @Test
  public void testHasAppendsOnlyFalseWhenNotOnlyAppendsAfterGivenSnapshot() {
    SnapshotContext since = new SnapshotContext(42);
    List<Snapshot> snapshotList = Arrays.asList(anySnapshot, appendSnapshot, deleteSnapshot);

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(snapshotList, since);

    assertThat(result, is(false));
  }

  @Test
  public void testHasAppendsOnlyReturnsNullWhenGivenSnapshotNotInTheList() {
    SnapshotContext since = new SnapshotContext(1);
    List<Snapshot> snapshotList = Arrays.asList(anySnapshot, appendSnapshot, deleteSnapshot);

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(snapshotList, since);

    assertThat(result, is(nullValue()));
  }
}
