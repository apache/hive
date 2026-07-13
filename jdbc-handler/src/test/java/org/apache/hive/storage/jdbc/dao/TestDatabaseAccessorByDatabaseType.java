/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.storage.jdbc.conf.DatabaseType;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Enclosed.class)
public class TestDatabaseAccessorByDatabaseType {

  private static final String BASE = "select * from test_strategy";
  private static final String H2_DRIVER = "org.h2.Driver";
  private static final String R = OracleDatabaseAccessor.ROW_NUM_COLUMN_NAME;

  @RunWith(Parameterized.class)
  public static class AddLimitToQueryWithLimit {
    private final DatabaseType databaseType;
    private final String expectedQuery;

    public AddLimitToQueryWithLimit(DatabaseType databaseType, String expectedQuery) {
      this.databaseType = databaseType;
      this.expectedQuery = expectedQuery;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return queryRows(database -> database.limit2);
    }

    @Test
    public void testAddLimitToQuery_withLimit() {
      assertThat(accessor(databaseType).addLimitToQuery(BASE, 2), is(equalTo(expectedQuery)));
    }
  }

  @RunWith(Parameterized.class)
  public static class AddLimitToQueryNoLimit {
    private final DatabaseType databaseType;
    private final String expectedQuery;

    public AddLimitToQueryNoLimit(DatabaseType databaseType, String expectedQuery) {
      this.databaseType = databaseType;
      this.expectedQuery = expectedQuery;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return queryRows(database -> database.noLimit);
    }

    @Test
    public void testAddLimitToQuery_noLimit() {
      assertThat(accessor(databaseType).addLimitToQuery(BASE, -1), is(equalTo(expectedQuery)));
    }
  }

  @RunWith(Parameterized.class)
  public static class AddLimitAndOffsetToQueryLimitAndOffset {
    private final DatabaseType databaseType;
    private final String expectedQuery;

    public AddLimitAndOffsetToQueryLimitAndOffset(DatabaseType databaseType, String expectedQuery) {
      this.databaseType = databaseType;
      this.expectedQuery = expectedQuery;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return queryRows(database -> database.limitAndOffset);
    }

    @Test
    public void testAddLimitAndOffsetToQuery_limitAndOffset() {
      assertThat(accessor(databaseType).addLimitAndOffsetToQuery(BASE, 2, 1), is(equalTo(expectedQuery)));
    }
  }

  @RunWith(Parameterized.class)
  public static class AddLimitAndOffsetToQueryOffsetOnly {
    private final DatabaseType databaseType;
    private final String expectedQuery;

    public AddLimitAndOffsetToQueryOffsetOnly(DatabaseType databaseType, String expectedQuery) {
      this.databaseType = databaseType;
      this.expectedQuery = expectedQuery;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return queryRows(database -> database.offsetOnly);
    }

    @Test
    public void testAddLimitAndOffsetToQuery_offsetOnly() {
      assertThat(accessor(databaseType).addLimitAndOffsetToQuery(BASE, -1, 2), is(equalTo(expectedQuery)));
    }
  }

  @RunWith(Parameterized.class)
  public static class AddLimitAndOffsetToQueryOffsetZero {
    private final DatabaseType databaseType;
    private final String expectedQuery;

    public AddLimitAndOffsetToQueryOffsetZero(DatabaseType databaseType, String expectedQuery) {
      this.databaseType = databaseType;
      this.expectedQuery = expectedQuery;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return queryRows(database -> database.offsetZero);
    }

    @Test
    public void testAddLimitAndOffsetToQuery_offsetZero() {
      assertThat(accessor(databaseType).addLimitAndOffsetToQuery(BASE, 2, 0), is(equalTo(expectedQuery)));
    }
  }

  @RunWith(Parameterized.class)
  public static class NeedColumnQuote {
    private final DatabaseType databaseType;
    private final boolean expectedNeedColumnQuote;

    public NeedColumnQuote(DatabaseType databaseType, boolean expectedNeedColumnQuote) {
      this.databaseType = databaseType;
      this.expectedNeedColumnQuote = expectedNeedColumnQuote;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return rowsFor(database -> database.needColumnQuote);
    }

    @Test
    public void testNeedColumnQuote() {
      assertThat(accessor(databaseType).needColumnQuote(), is(expectedNeedColumnQuote));
    }
  }

  @RunWith(Parameterized.class)
  public static class ConstructQuery {
    private final DatabaseType databaseType;
    private final String expectedQuery;

    public ConstructQuery(DatabaseType databaseType, String expectedQuery) {
      this.databaseType = databaseType;
      this.expectedQuery = expectedQuery;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return queryRows(database -> database.constructQuery);
    }

    @Test
    public void testConstructQuery() {
      assertThat(accessor(databaseType).constructQuery("t", new String[]{"a", "b", "c"}),
          is(equalTo(expectedQuery)));
    }
  }

  @RunWith(Parameterized.class)
  public static class GetMetaDataQuery {
    private final DatabaseType databaseType;
    private final String expectedQuery;

    public GetMetaDataQuery(DatabaseType databaseType, String expectedQuery) {
      this.databaseType = databaseType;
      this.expectedQuery = expectedQuery;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return queryRows(database -> database.metaDataQuery);
    }

    @Test
    public void testGetMetaDataQuery() {
      assertThat(accessor(databaseType).getMetaDataQuery(BASE), is(equalTo(expectedQuery)));
    }
  }

  @RunWith(Parameterized.class)
  public static class AccessorType {
    private final DatabaseType databaseType;

    public AccessorType(DatabaseType databaseType) {
      this.databaseType = databaseType;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return rowsFor(database -> null);
    }

    @Test
    public void testAccessorType() {
      assertThat(DatabaseAccessorFactory.getAccessor(buildConfiguration(databaseType)),
          instanceOf(accessorCase(databaseType).expectedType));
    }
  }

  @RunWith(Parameterized.class)
  public static class FactoryMapping {
    private final DatabaseType databaseType;
    private final String driverClass;
    private final Class<? extends DatabaseAccessor> expectedType;

    public FactoryMapping(DatabaseType databaseType, String driverClass,
        Class<? extends DatabaseAccessor> expectedType) {
      this.databaseType = databaseType;
      this.driverClass = driverClass;
      this.expectedType = expectedType;
    }

    @Parameters(name = "{0}:{1}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][]{
          {DatabaseType.H2, "org.h2.Driver", GenericJdbcDatabaseAccessor.class},
          {DatabaseType.METASTORE, "com.mysql.cj.jdbc.Driver", MySqlDatabaseAccessor.class},
          {DatabaseType.METASTORE, "org.postgresql.Driver", PostgresDatabaseAccessor.class},
          {DatabaseType.METASTORE, "oracle.jdbc.OracleDriver", OracleDatabaseAccessor.class},
          {DatabaseType.METASTORE, "com.microsoft.sqlserver.jdbc.SQLServerDriver", MsSqlDatabaseAccessor.class},
          {DatabaseType.METASTORE, "org.unknown.Driver", GenericJdbcDatabaseAccessor.class},
      });
    }

    @Test
    public void testFactoryMapping() {
      Configuration config = new Configuration();
      config.set(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), databaseType.name());
      config.set(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName(), driverClass);
      assertThat(DatabaseAccessorFactory.getAccessor(config), instanceOf(expectedType));
    }
  }

  private static final AccessorCase[] CASES = {
      db(DatabaseType.MYSQL, MySqlDatabaseAccessor.class,
          BASE + " LIMIT 2",
          BASE,
          BASE + " LIMIT 1,2",
          BASE,
          BASE + " LIMIT 2",
          false,
          "INSERT INTO t VALUES (?,?,?);",
          BASE + " LIMIT 1"),
      db(DatabaseType.POSTGRES, PostgresDatabaseAccessor.class,
          BASE + " LIMIT 2",
          BASE,
          BASE + " LIMIT 2 OFFSET 1",
          BASE,
          BASE + " LIMIT 2",
          true,
          "INSERT INTO t VALUES (?,?,?);",
          BASE + " LIMIT 1"),
      db(DatabaseType.DB2, DB2DatabaseAccessor.class,
          BASE + " LIMIT 2",
          BASE,
          BASE + " LIMIT 2 OFFSET 1",
          BASE,
          BASE + " LIMIT 2",
          true,
          "INSERT INTO t VALUES (?,?,?)",
          BASE + " LIMIT 1"),
      db(DatabaseType.DERBY, DerbyDatabaseAccessor.class,
          BASE + " {LIMIT 2}",
          BASE,
          BASE + " {LIMIT 2 OFFSET 1}",
          BASE + " {OFFSET 2}",
          BASE + " {LIMIT 2}",
          true,
          "INSERT INTO t VALUES (?,?,?)",
          BASE + " {LIMIT 1}"),
      db(DatabaseType.MSSQL, MsSqlDatabaseAccessor.class,
          BASE + " {LIMIT 2}",
          BASE,
          BASE + " ORDER BY 1 OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
          BASE,
          BASE + " {LIMIT 2}",
          true,
          "INSERT INTO t VALUES (?,?,?);",
          BASE + " {LIMIT 1}"),
      db(DatabaseType.ORACLE, OracleDatabaseAccessor.class,
          "SELECT * FROM (" + BASE + ") WHERE ROWNUM <= 2",
          BASE,
          "SELECT * FROM (SELECT t.*, ROWNUM AS " + R + " FROM (" + BASE + ") t) WHERE "
              + R + " >1 AND " + R + " <=3",
          BASE,
          "SELECT * FROM (" + BASE + ") WHERE ROWNUM <= 2",
          true,
          "INSERT INTO t VALUES (?,?,?)",
          "SELECT * FROM (" + BASE + ") WHERE ROWNUM <= 1"),
      db(DatabaseType.HIVE, HiveDatabaseAccessor.class,
          BASE + " LIMIT 2",
          BASE,
          BASE + " LIMIT 2 OFFSET 1",
          BASE,
          BASE + " LIMIT 2",
          true,
          "INSERT INTO t VALUES (?,?,?);",
          BASE + " LIMIT 0"),
      db(DatabaseType.JETHRO_DATA, JethroDatabaseAccessor.class,
          "Select * from (" + BASE + ") as \"tmp\" limit 2",
          "Select * from (" + BASE + ") as \"tmp\" limit -1",
          BASE + " LIMIT 1,2",
          BASE + " LIMIT 2,-1",
          "Select * from (" + BASE + ") as \"tmp\" limit 2",
          true,
          "INSERT INTO t VALUES (?,?,?);",
          "Select * from (" + BASE + ") as \"tmp\" limit 0")
  };

  private static Object[] row(Object... values) {
    return values;
  }

  private static Collection<Object[]> rowsFor(Function<AccessorCase, Object> expectedValue) {
    Object[][] rows = new Object[CASES.length][];
    for (int i = 0; i < CASES.length; i++) {
      AccessorCase database = CASES[i];
      Object expected = expectedValue.apply(database);
      rows[i] = expected == null ? row(database.databaseType) : row(database.databaseType, expected);
    }
    return Arrays.asList(rows);
  }

  private static Collection<Object[]> queryRows(Function<AccessorCase, String> expectedQuery) {
    return rowsFor(database -> expectedQuery.apply(database));
  }

  private static AccessorCase db(
      DatabaseType databaseType,
      Class<? extends GenericJdbcDatabaseAccessor> expectedType,
      String limit2,
      String noLimit,
      String limitAndOffset,
      String offsetOnly,
      String offsetZero,
      boolean needColumnQuote,
      String constructQuery,
      String metaDataQuery) {
    return new AccessorCase(databaseType, expectedType, limit2, noLimit, limitAndOffset,
        offsetOnly, offsetZero, needColumnQuote, constructQuery, metaDataQuery);
  }

  private static AccessorCase accessorCase(DatabaseType databaseType) {
    for (AccessorCase accessorCase : CASES) {
      if (accessorCase.databaseType == databaseType) {
        return accessorCase;
      }
    }
    throw new IllegalArgumentException("Unexpected database type: " + databaseType);
  }

  private static GenericJdbcDatabaseAccessor accessor(DatabaseType databaseType) {
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(buildConfiguration(databaseType));
    assertThat(accessor, instanceOf(accessorCase(databaseType).expectedType));
    return (GenericJdbcDatabaseAccessor) accessor;
  }

  private static Configuration buildConfiguration(DatabaseType databaseType) {
    Configuration config = new Configuration();
    config.set(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), databaseType.name());
    config.set(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName(), H2_DRIVER);
    return config;
  }

  private static class AccessorCase {
    private final DatabaseType databaseType;
    private final Class<? extends GenericJdbcDatabaseAccessor> expectedType;
    private final String limit2;
    private final String noLimit;
    private final String limitAndOffset;
    private final String offsetOnly;
    private final String offsetZero;
    private final boolean needColumnQuote;
    private final String constructQuery;
    private final String metaDataQuery;

    private AccessorCase(
        DatabaseType databaseType,
        Class<? extends GenericJdbcDatabaseAccessor> expectedType,
        String limit2,
        String noLimit,
        String limitAndOffset,
        String offsetOnly,
        String offsetZero,
        boolean needColumnQuote,
        String constructQuery,
        String metaDataQuery) {
      this.databaseType = databaseType;
      this.expectedType = expectedType;
      this.limit2 = limit2;
      this.noLimit = noLimit;
      this.limitAndOffset = limitAndOffset;
      this.offsetOnly = offsetOnly;
      this.offsetZero = offsetZero;
      this.needColumnQuote = needColumnQuote;
      this.constructQuery = constructQuery;
      this.metaDataQuery = metaDataQuery;
    }
  }

}
