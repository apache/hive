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

package org.apache.iceberg.rest;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveCatalog;
import org.junit.jupiter.api.Test;

class TestHMSCachingCatalog {

  private static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");
  private static final long CACHE_EXPIRATION_MS = 60_000L;

  @Test
  void loadTableAuthorizesEveryRequestEvenWhenCached() {
    HiveCatalog hiveCatalog = mock(HiveCatalog.class);
    Table table = mock(Table.class);
    when(hiveCatalog.loadTable(TABLE)).thenReturn(table);
    when(hiveCatalog.tableExists(TABLE)).thenReturn(true, true, false);

    HMSCachingCatalog catalog = new HMSCachingCatalog(hiveCatalog, CACHE_EXPIRATION_MS);

    Table firstLoad = catalog.loadTable(TABLE);
    Table secondLoad = catalog.loadTable(TABLE);
    assertSame(firstLoad, secondLoad);

    assertThrows(NoSuchTableException.class, () -> catalog.loadTable(TABLE));

    verify(hiveCatalog, times(1)).loadTable(TABLE);
    verify(hiveCatalog, times(3)).tableExists(TABLE);
  }

  @Test
  void tableExistsDelegatesToWrappedCatalogOnEveryCall() {
    HiveCatalog hiveCatalog = mock(HiveCatalog.class);
    when(hiveCatalog.tableExists(TABLE)).thenReturn(true);

    HMSCachingCatalog catalog = new HMSCachingCatalog(hiveCatalog, CACHE_EXPIRATION_MS);

    assertTrue(catalog.tableExists(TABLE));
    assertTrue(catalog.tableExists(TABLE));

    verify(hiveCatalog, times(2)).tableExists(TABLE);
    verify(hiveCatalog, never()).loadTable(any());
  }
}
