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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AuthorizationMetaStoreFilterHook#filterTables}.
 *
 * Validates correctness of the O(n) HashMap-based fix replacing the original
 * O(n^2) nested-loop implementation, including catName null-handling logic.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestAuthorizationMetaStoreFilterHook {

  private AuthorizationMetaStoreFilterHook hook;
  private MockedStatic<SessionState> mockedSessionState;

  @Mock private SessionState sessionState;
  @Mock private HiveAuthorizer authorizer;

  @Before
  public void setUp() {
    hook = new AuthorizationMetaStoreFilterHook(new Configuration());
    when(sessionState.getUserIpAddress()).thenReturn("127.0.0.1");
    when(sessionState.getForwardedAddresses()).thenReturn(Collections.emptyList());
    when(sessionState.getAuthorizerV2()).thenReturn(authorizer);
    mockedSessionState = mockStatic(SessionState.class);
    mockedSessionState.when(SessionState::get).thenReturn(sessionState);
  }

  @After
  public void tearDown() {
    mockedSessionState.close();
  }


  /** Create a Table with catName. */
  private Table createTable(String catName, String dbName, String tableName) {
    Table table = new Table();
    table.setCatName(catName);
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setOwner("testowner");
    table.setOwnerType(PrincipalType.USER);
    return table;
  }

  /** Create a Table without catName (catName = null). */
  private Table createTable(String dbName, String tableName) {
    return createTable(null, dbName, tableName);
  }

  /**
   * Stub authorizer to return HivePrivilegeObjects with the given catName
   * for tables matching dbName + any of the allowedTableNames.
   */
  private void allowTablesWithCatName(String catName, String dbName, String... allowedTableNames)
       throws HiveAuthzPluginException, HiveAccessControlException {
    when(authorizer.filterListCmdObjects(any(), any())).thenAnswer(invocation -> {
      List<HivePrivilegeObject> input = invocation.getArgument(0);
      List<HivePrivilegeObject> result = new ArrayList<>();
      Set<String> seen = new HashSet<>();
      for (HivePrivilegeObject obj : input) {
        for (String allowed : allowedTableNames) {
          if (dbName.equals(obj.getDbname()) && allowed.equals(obj.getObjectName())) {
            String key = (catName != null ? catName : "") + "\0" + dbName + "\0" + allowed;
            if (seen.add(key)) {
              result.add(new HivePrivilegeObject(
                    HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW, catName, dbName, allowed));
            }
            break;
          }
        }
      }
      return result;
    });
  }

  /** Stub authorizer to return all objects unchanged (allow all, catName stays null). */
  private void allowAll() throws HiveAuthzPluginException, HiveAccessControlException {
    when(authorizer.filterListCmdObjects(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
  }

  /** Stub authorizer to deny everything. */
  private void denyAll() throws HiveAuthzPluginException, HiveAccessControlException {
    when(authorizer.filterListCmdObjects(any(), any())).thenReturn(Collections.emptyList());
  }

  @Test
  public void testFilterTablesEmptyInput() throws Exception {
    denyAll();
    List<Table> result = hook.filterTables(Collections.emptyList());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testFilterTablesAllAllowed() throws Exception {
    List<Table> input = List.of(
        createTable("db1", "tbl1"),
        createTable("db1", "tbl2"),
        createTable("db1", "tbl3"));
    allowAll();
    List<Table> result = hook.filterTables(input);
    assertEquals(3, result.size());
  }

  @Test
  public void testFilterTablesAllFiltered() throws Exception {
    List<Table> input = List.of(
        createTable("db1", "tbl1"),
        createTable("db1", "tbl2"));
    denyAll();
    List<Table> result = hook.filterTables(input);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testFilterTablesSomeFiltered() throws Exception {
    List<Table> input = List.of(
        createTable("db1", "tbl1"),
        createTable("db1", "tbl2"),
        createTable("db1", "tbl3"));
      // authorize returns tbl1 and tbl3 with null catName
    when(authorizer.filterListCmdObjects(any(), any())).thenAnswer(invocation -> {
      List<HivePrivilegeObject> objs = invocation.getArgument(0);
      return objs.stream()
                    .filter(o -> "tbl1".equals(o.getObjectName()) || "tbl3".equals(o.getObjectName()))
                    .toList();
    });
    List<Table> result = hook.filterTables(input);
    assertEquals(2, result.size());
    List<String> names = result.stream().map(Table::getTableName).toList();
    assertTrue(names.contains("tbl1"));
    assertTrue(names.contains("tbl3"));
  }

  /**
   * Case 2: obj.catName == null â†’ must match a table that also has null catName.
   * Note: HivePrivilegeObject normalizes null catName to the default catalog ("hive"),
   * so the null-catName-obj path in production code is unreachable in practice.
   * The live equivalent is tested via testFilterTables_tableCatNameNull_matchesObjWithNonNullCatName.
   */
  @Test
  public void testFilterTablesObjCatNameNullMatchesTableWithNullCatName() throws Exception {
    List<Table> input = List.of(createTable(null, "db1", "tbl1"));
    allowTablesWithCatName(null, "db1", "tbl1");
    List<Table> result = hook.filterTables(input);
    assertEquals("obj with null catName should match table with null catName", 1, result.size());
  }

  /**
   * Table with null catName: tablesToPrivilegeObjs normalizes null â†’ default catalog ("hive"),
   * so the authorizer receives and returns a "hive"-catName HPO. The lookup normalizes the
   * table's null catName to "hive" as well, producing an exact match.
   */
  @Test
  public void testFilterTablesTableCatNameNullMatchesObjWithNonNullCatName() throws Exception {
    List<Table> input = List.of(createTable(null, "db1", "tbl1")); // table has null catName
    // authorizer echoes back the default-catalog HPO it received
    allowTablesWithCatName(null, "db1", "tbl1");
    List<Table> result = hook.filterTables(input);
    assertEquals("table with null catName should match when authorizer returns default-catalog HPO", 1, result.size());
  }

  /**
   * Case 3: obj.catName != null, table.catName != null, and they match â†’ matches.
   */
  @Test
  public void testFilterTablesSameCatNameMatches() throws Exception {
    List<Table> input = List.of(createTable("cat1", "db1", "tbl1"));
    allowTablesWithCatName("cat1", "db1", "tbl1");
    List<Table> result = hook.filterTables(input);
    assertEquals("same catName on obj and table should match", 1, result.size());
    assertEquals("cat1", result.get(0).getCatName());
  }

  /**
   * Both obj.catName and table.catName are non-null but DIFFERENT â†’ no match.
   * Original logic: condition is true â†’ continue (skip).
   */
  @Test
  public void testFilterTablesDifferentCatNameNoMatch() throws Exception {
    List<Table> input = List.of(createTable("cat2", "db1", "tbl1")); // table has cat2
    // authorizer says tbl1 is allowed but with catName=cat1
    allowTablesWithCatName("cat1", "db1", "tbl1");
    List<Table> result = hook.filterTables(input);
    assertTrue("obj.catName=cat1 vs table.catName=cat2 should not match", result.isEmpty());
  }

  /**
   * Multiple catalogs, same db+table name.
   * obj.catName=cat1 should return only the cat1 table, not cat2.
   */
  @Test
  public void testFilterTablesMultipleCatalogsSameDbTableExactCatNameMatch() throws Exception {
    Table cat1Table = createTable("cat1", "db1", "tbl1");
    Table cat2Table = createTable("cat2", "db1", "tbl1");
    List<Table> input = List.of(cat1Table, cat2Table);

    allowTablesWithCatName("cat1", "db1", "tbl1");

    List<Table> result = hook.filterTables(input);

    assertEquals("Only cat1 table should be returned", 1, result.size());
    assertEquals("cat1", result.get(0).getCatName());
  }

  /**
   * Table with null catName is normalized to the default catalog for lookup.
   * When the authorizer allows it (returning the default-catalog HPO it received),
   * the null-catName table is included; a table with a different catName ("cat2")
   * that was not authorized is excluded.
   */
  @Test
  public void testFilterTablesNullCatTableIncludedWhenAuthorizedOtherCatExcluded() throws Exception {
    Table nullCatTable = createTable(null, "db1", "tbl1");   // normalized to "hive" for lookup
    Table cat2Table    = createTable("cat2", "db1", "tbl1"); // not authorized
    List<Table> input  = List.of(nullCatTable, cat2Table);

    // authorizer allows only the default-catalog ("hive") entry, not cat2
    allowTablesWithCatName(null, "db1", "tbl1");

    List<Table> result = hook.filterTables(input);

    assertEquals("only the null-catName table (authorized via default catalog) should be returned", 1, result.size());
    assertEquals(null, result.get(0).getCatName());
  }

  /**
   * Mixed: some tables have catName, some don't; authorizer excludes one table entirely.
   * - tbl1 (catName=cat1): authorized â†’ exact match on "cat1" â†’ included.
   * - tbl2 (catName=null): normalized to "hive" â†’ authorizer echoes "hive" HPO â†’ included.
   * - tbl3 (catName=cat2): not authorized â†’ excluded.
   */
  @Test
  public void testFilterTablesMixedCatNameCombinations() throws Exception {
    List<Table> input = List.of(
        createTable("cat1", "db1", "tbl1"),  // authorized, exact catName match
        createTable(null,   "db1", "tbl2"),  // authorized, null catName normalized to "hive"
        createTable("cat2", "db1", "tbl3")); // not authorized â†’ excluded

    // authorizer passes through tbl1 and tbl2 unchanged, drops tbl3
    when(authorizer.filterListCmdObjects(any(), any())).thenAnswer(invocation -> {
      List<HivePrivilegeObject> objs = invocation.getArgument(0);
      return objs.stream()
          .filter(o -> !"tbl3".equals(o.getObjectName()))
          .toList();
    });

    List<Table> result = hook.filterTables(input);

    List<String> names = result.stream().map(Table::getTableName).toList();
    assertEquals("Should return tbl1 (cat1 exact match) and tbl2 (null catName normalized to hive)", 2, result.size());
    assertTrue(names.contains("tbl1"));
    assertTrue(names.contains("tbl2"));
    assertTrue(!names.contains("tbl3"));
  }

  /**
   * 1 lakh tables must complete within 5 seconds.
   * The original O(n^2) implementation would take 200-300 seconds.
   * Note: uses inline time assertion instead of @Test(timeout) to avoid JUnit 4's
   * separate-thread execution, which breaks Mockito's ThreadLocal-based MockedStatic.
   */
  @Test
  public void testFilterTablesPerformance100kTables() throws Exception {
    int total = 100_000;
    List<Table> input = IntStream.range(0, total)
        .mapToObj(i -> createTable("cat1", "db1", "tbl_" + i))
        .toList();

    when(authorizer.filterListCmdObjects(any(), any())).thenAnswer(invocation -> {
      List<HivePrivilegeObject> objs = invocation.getArgument(0);
      return objs.stream()
          .filter(o -> Integer.parseInt(o.getObjectName().split("_")[1]) % 2 == 0)
          .map(o -> new HivePrivilegeObject(
             HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW, "cat1", o.getDbname(), o.getObjectName()))
          .toList();
    });

    long start = System.currentTimeMillis();
    List<Table> result = hook.filterTables(input);
    long elapsed = System.currentTimeMillis() - start;

    assertEquals(total / 2, result.size());
    assertTrue(String.format("filterTables with %d tables took %d ms, expected < 5000 ms", total, elapsed),
        elapsed < 5000);
  }
}
