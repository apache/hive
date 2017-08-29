/**
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

package org.apache.hive.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.jar.Attributes;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCLIService.Iface;
import org.apache.hive.service.rpc.thrift.TGetCatalogsReq;
import org.apache.hive.service.rpc.thrift.TGetCatalogsResp;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetColumnsResp;
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceReq;
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceResp;
import org.apache.hive.service.rpc.thrift.TGetFunctionsReq;
import org.apache.hive.service.rpc.thrift.TGetFunctionsResp;
import org.apache.hive.service.rpc.thrift.TGetInfoReq;
import org.apache.hive.service.rpc.thrift.TGetInfoResp;
import org.apache.hive.service.rpc.thrift.TGetInfoType;
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysReq;
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysResp;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasResp;
import org.apache.hive.service.rpc.thrift.TGetTableTypesReq;
import org.apache.hive.service.rpc.thrift.TGetTableTypesResp;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.hive.service.rpc.thrift.TGetTablesResp;
import org.apache.hive.service.rpc.thrift.TGetTypeInfoReq;
import org.apache.hive.service.rpc.thrift.TGetTypeInfoResp;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.thrift.TException;

/**
 * HiveDatabaseMetaData.
 *
 */
public class HiveDatabaseMetaData implements DatabaseMetaData {

  private final HiveConnection connection;
  private final TCLIService.Iface client;
  private final TSessionHandle sessHandle;
  private static final String CATALOG_SEPARATOR = ".";

  private static final char SEARCH_STRING_ESCAPE = '\\';

  //  The maximum column length = MFieldSchema.FNAME in metastore/src/model/package.jdo
  private static final int maxColumnNameLength = 128;

  //  Cached values, to save on round trips to database.
  private String dbVersion = null;

  /**
   *
   */
  public HiveDatabaseMetaData(HiveConnection connection, TCLIService.Iface client,
      TSessionHandle sessHandle) {
    this.connection = connection;
    this.client = client;
    this.sessHandle = sessHandle;
  }

  public boolean allProceduresAreCallable() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean deletesAreDetected(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public ResultSet getAttributes(String catalog, String schemaPattern,
      String typeNamePattern, String attributeNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public ResultSet getBestRowIdentifier(String catalog, String schema,
      String table, int scope, boolean nullable) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public String getCatalogSeparator() throws SQLException {
    return CATALOG_SEPARATOR;
  }

  public String getCatalogTerm() throws SQLException {
    return "instance";
  }

  public ResultSet getCatalogs() throws SQLException {
    TGetCatalogsResp catalogResp;

    try {
      catalogResp = client.GetCatalogs(new TGetCatalogsReq(sessHandle));
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(catalogResp.getStatus());

    return new HiveQueryResultSet.Builder(connection)
    .setClient(client)
    .setSessionHandle(sessHandle)
    .setStmtHandle(catalogResp.getOperationHandle())
    .build();
  }

  public ResultSet getClientInfoProperties() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public ResultSet getColumnPrivileges(String catalog, String schema,
      String table, String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public ResultSet getPseudoColumns(String catalog, String schemaPattern,
      String tableNamePattern, String columnNamePattern) throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean generatedKeyAlwaysReturned() throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /**
   * Convert a pattern containing JDBC catalog search wildcards into
   * Java regex patterns.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or
   * these characters escaped using {@link #getSearchStringEscape()}.
   * @return replace %/_ with regex search characters, also handle escaped
   * characters.
   */
  private String convertPattern(final String pattern) {
    if (pattern==null) {
      return ".*";
    } else {
      StringBuilder result = new StringBuilder(pattern.length());

      boolean escaped = false;
      for (int i = 0, len = pattern.length(); i < len; i++) {
        char c = pattern.charAt(i);
        if (escaped) {
          if (c != SEARCH_STRING_ESCAPE) {
            escaped = false;
          }
          result.append(c);
        } else {
          if (c == SEARCH_STRING_ESCAPE) {
            escaped = true;
            continue;
          } else if (c == '%') {
            result.append(".*");
          } else if (c == '_') {
            result.append('.');
          } else {
            result.append(Character.toLowerCase(c));
          }
        }
      }

      return result.toString();
    }
  }

  public ResultSet getColumns(String catalog, String schemaPattern,
      String tableNamePattern, String columnNamePattern) throws SQLException {
    TGetColumnsResp colResp;
    TGetColumnsReq colReq = new TGetColumnsReq();
    colReq.setSessionHandle(sessHandle);
    colReq.setCatalogName(catalog);
    colReq.setSchemaName(schemaPattern);
    colReq.setTableName(tableNamePattern);
    colReq.setColumnName(columnNamePattern);
    try {
      colResp = client.GetColumns(colReq);
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(colResp.getStatus());
    // build the resultset from response
    return new HiveQueryResultSet.Builder(connection)
    .setClient(client)
    .setSessionHandle(sessHandle)
    .setStmtHandle(colResp.getOperationHandle())
    .build();
  }

  /**
   * We sort the output of getColumns to guarantee jdbc compliance.
   * First check by table name then by ordinal position
   */
  private class GetColumnsComparator implements Comparator<JdbcColumn> {

    public int compare(JdbcColumn o1, JdbcColumn o2) {
      int compareName = o1.getTableName().compareTo(o2.getTableName());
      if (compareName==0) {
        if (o1.getOrdinalPos() > o2.getOrdinalPos()) {
          return 1;
        } else if (o1.getOrdinalPos() < o2.getOrdinalPos()) {
          return -1;
        }
        return 0;
      } else {
        return compareName;
      }
    }
  }

  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  public ResultSet getCrossReference(String primaryCatalog,
      String primarySchema, String primaryTable, String foreignCatalog,
      String foreignSchema, String foreignTable) throws SQLException {
   TGetCrossReferenceResp getFKResp;
   TGetCrossReferenceReq getFKReq = new TGetCrossReferenceReq(sessHandle);
   getFKReq.setParentTableName(primaryTable);
   getFKReq.setParentSchemaName(primarySchema);
   getFKReq.setParentCatalogName(primaryCatalog);
   getFKReq.setForeignTableName(foreignTable);
   getFKReq.setForeignSchemaName(foreignSchema);
   getFKReq.setForeignCatalogName(foreignCatalog);

   try {
     getFKResp = client.GetCrossReference(getFKReq);
   } catch (TException e) {
     throw new SQLException(e.getMessage(), "08S01", e);
   }
   Utils.verifySuccess(getFKResp.getStatus());

   return new HiveQueryResultSet.Builder(connection)
     .setClient(client)
     .setSessionHandle(sessHandle)
     .setStmtHandle(getFKResp.getOperationHandle())
     .build();
  }

  public int getDatabaseMajorVersion() throws SQLException {
    return Utils.getVersionPart(getDatabaseProductVersion(), 0);
  }

  public int getDatabaseMinorVersion() throws SQLException {
    return Utils.getVersionPart(getDatabaseProductVersion(), 1);
  }

  public String getDatabaseProductName() throws SQLException {
    TGetInfoResp resp = getServerInfo(GetInfoType.CLI_DBMS_NAME.toTGetInfoType());
    return resp.getInfoValue().getStringValue();
  }

  public String getDatabaseProductVersion() throws SQLException {
    if (dbVersion != null) { //lazy-caching of the version.
      return dbVersion;
    }

    TGetInfoResp resp = getServerInfo(GetInfoType.CLI_DBMS_VER.toTGetInfoType());
    this.dbVersion = resp.getInfoValue().getStringValue();
    return dbVersion;
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  public int getDriverMajorVersion() {
    return HiveDriver.getMajorDriverVersion();
  }

  public int getDriverMinorVersion() {
    return HiveDriver.getMinorDriverVersion();
  }

  public String getDriverName() throws SQLException {
    return HiveDriver.fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_TITLE);
  }

  public String getDriverVersion() throws SQLException {
    return HiveDriver.fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_VERSION);
  }

  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public String getExtraNameCharacters() throws SQLException {
    // TODO: verify that this is correct
    return "";
  }

  public ResultSet getFunctionColumns(String arg0, String arg1, String arg2,
      String arg3) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public ResultSet getFunctions(String catalogName, String schemaPattern, String functionNamePattern)
      throws SQLException {
    TGetFunctionsResp funcResp;
    TGetFunctionsReq getFunctionsReq = new TGetFunctionsReq();
    getFunctionsReq.setSessionHandle(sessHandle);
    getFunctionsReq.setCatalogName(catalogName);
    getFunctionsReq.setSchemaName(schemaPattern);
    getFunctionsReq.setFunctionName(functionNamePattern);

    try {
      funcResp = client.GetFunctions(getFunctionsReq);
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(funcResp.getStatus());

    return new HiveQueryResultSet.Builder(connection)
    .setClient(client)
    .setSessionHandle(sessHandle)
    .setStmtHandle(funcResp.getOperationHandle())
    .build();
  }

  public String getIdentifierQuoteString() throws SQLException {
    return " ";
  }

  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return new HiveQueryResultSet.Builder(connection)
    .setClient(client)
    .setEmptyResultSet(true)
    .setSchema(
        Arrays.asList(
            "PKTABLE_CAT",
            "PKTABLE_SCHEM",
            "PKTABLE_NAME",
            "PKCOLUMN_NAME",
            "FKTABLE_CAT",
            "FKTABLE_SCHEM",
            "FKTABLE_NAME",
            "FKCOLUMN_NAME",
            "KEY_SEQ",
            "UPDATE_RULE",
            "DELETE_RULE",
            "FK_NAME",
            "PK_NAME",
            "DEFERRABILITY"),
        Arrays.asList(
            "STRING",
            "STRING",
            "STRING",
            "STRING",
            "STRING",
            "STRING",
            "STRING",
            "STRING",
            "SMALLINT",
            "SMALLINT",
            "SMALLINT",
            "STRING",
            "STRING",
            "STRING"))
            .build();
  }

  public ResultSet getIndexInfo(String catalog, String schema, String table,
      boolean unique, boolean approximate) throws SQLException {
    return new HiveQueryResultSet.Builder(connection)
        .setClient(client)
        .setEmptyResultSet(true)
        .setSchema(
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME",
                "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"),
            Arrays.asList("STRING", "STRING", "STRING", "BOOLEAN", "STRING", "STRING", "SHORT",
                "SHORT", "STRING", "STRING", "INT", "INT", "STRING")).build();
  }

  public int getJDBCMajorVersion() throws SQLException {
    return 3;
  }

  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  public int getMaxBinaryLiteralLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxCatalogNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxCharLiteralLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /**
   *  Returns the value of maxColumnNameLength.
   *
   */
  public int getMaxColumnNameLength() throws SQLException {
    return maxColumnNameLength;
  }

  public int getMaxColumnsInGroupBy() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxColumnsInIndex() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxColumnsInOrderBy() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxColumnsInSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxColumnsInTable() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxConnections() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxCursorNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxIndexLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxProcedureNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxRowSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxSchemaNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxStatementLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxStatements() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxTableNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxTablesInSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getMaxUserNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public String getNumericFunctions() throws SQLException {
    return "";
  }

  public ResultSet getPrimaryKeys(String catalog, String schema, String table)
      throws SQLException {
    TGetPrimaryKeysResp getPKResp;
    TGetPrimaryKeysReq getPKReq = new TGetPrimaryKeysReq(sessHandle);
    getPKReq.setTableName(table);
    getPKReq.setSchemaName(schema);
    getPKReq.setCatalogName(catalog);
    try {
      getPKResp = client.GetPrimaryKeys(getPKReq);
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(getPKResp.getStatus());

    return new HiveQueryResultSet.Builder(connection)
    .setClient(client)
    .setSessionHandle(sessHandle)
    .setStmtHandle(getPKResp.getOperationHandle())
    .build();
  }

  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
      String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    // Hive doesn't support primary keys
    // using local schema with empty resultset
    return new HiveQueryResultSet.Builder(connection).setClient(client).setEmptyResultSet(true).
                  setSchema(
                    Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE",
                              "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS",
                              "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                              "IS_NULLABLE", "SPECIFIC_NAME"),
                    Arrays.asList("STRING", "STRING", "STRING", "STRING", "SMALLINT", "INT",
                              "STRING", "INT", "INT", "SMALLINT", "SMALLINT", "SMALLINT", "STRING", "STRING",
                              "INT", "INT", "INT", "INT",
                              "STRING", "STRING"))
                  .build();
  }

  public String getProcedureTerm() throws SQLException {
    return new String("UDF");
  }

  public ResultSet getProcedures(String catalog, String schemaPattern,
      String procedureNamePattern) throws SQLException {
    // Hive doesn't support primary keys
    // using local schema with empty resultset
    return new HiveQueryResultSet.Builder(connection).setClient(client).setEmptyResultSet(true).
                  setSchema(
                    Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVERD", "RESERVERD",
                                  "RESERVERD", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"),
                    Arrays.asList("STRING", "STRING", "STRING", "STRING", "STRING",
                                  "STRING", "STRING", "SMALLINT", "STRING"))
                  .build();
  }

  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public String getSQLKeywords() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int getSQLStateType() throws SQLException {
    return DatabaseMetaData.sqlStateSQL99;
  }

  public String getSchemaTerm() throws SQLException {
    return "database";
  }

  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  public ResultSet getSchemas(String catalog, String schemaPattern)
      throws SQLException {
    TGetSchemasResp schemaResp;

    TGetSchemasReq schemaReq = new TGetSchemasReq();
    schemaReq.setSessionHandle(sessHandle);
    if (catalog != null) {
      schemaReq.setCatalogName(catalog);
    }
    if (schemaPattern == null) {
      schemaPattern = "%";
    }
    schemaReq.setSchemaName(schemaPattern);

    try {
      schemaResp = client.GetSchemas(schemaReq);
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(schemaResp.getStatus());

    return new HiveQueryResultSet.Builder(connection)
    .setClient(client)
    .setSessionHandle(sessHandle)
    .setStmtHandle(schemaResp.getOperationHandle())
    .build();
  }

  public String getSearchStringEscape() throws SQLException {
    return String.valueOf(SEARCH_STRING_ESCAPE);
  }

  public String getStringFunctions() throws SQLException {
    return "";
  }

  public ResultSet getSuperTables(String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public ResultSet getSuperTypes(String catalog, String schemaPattern,
      String typeNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public String getSystemFunctions() throws SQLException {
    return "";
  }

  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public ResultSet getTableTypes() throws SQLException {
    TGetTableTypesResp tableTypeResp;

    try {
      tableTypeResp = client.GetTableTypes(new TGetTableTypesReq(sessHandle));
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(tableTypeResp.getStatus());

    return new HiveQueryResultSet.Builder(connection)
    .setClient(client)
    .setSessionHandle(sessHandle)
    .setStmtHandle(tableTypeResp.getOperationHandle())
    .build();
  }

  public ResultSet getTables(String catalog, String schemaPattern,
                             String tableNamePattern, String[] types) throws SQLException {
    TGetTablesResp getTableResp;
    if (schemaPattern == null) {
      // if schemaPattern is null it means that the schemaPattern value should not be used to narrow the search
      schemaPattern = "%";
    }
    TGetTablesReq getTableReq = new TGetTablesReq(sessHandle);
    getTableReq.setTableName(tableNamePattern);

    // TODO: need to set catalog parameter

    if (types != null) {
      getTableReq.setTableTypes(Arrays.asList(types));
    }
    getTableReq.setSchemaName(schemaPattern);

    try {
      getTableResp = client.GetTables(getTableReq);
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(getTableResp.getStatus());

    return new HiveQueryResultSet.Builder(connection)
    .setClient(client)
    .setSessionHandle(sessHandle)
    .setStmtHandle(getTableResp.getOperationHandle())
    .build();
  }

  /**
   * We sort the output of getTables to guarantee jdbc compliance.
   * First check by table type then by table name
   */
  private class GetTablesComparator implements Comparator<JdbcTable> {

    public int compare(JdbcTable o1, JdbcTable o2) {
      int compareType = o1.getType().compareTo(o2.getType());
      if (compareType==0) {
        return o1.getTableName().compareTo(o2.getTableName());
      } else {
        return compareType;
      }
    }
  }

  /**
   * Translate hive table types into jdbc table types.
   * @param hivetabletype
   * @return the type of the table
   */
  public static String toJdbcTableType(String hivetabletype) {
    if (hivetabletype==null) {
      return null;
    } else if (hivetabletype.equals(TableType.MANAGED_TABLE.toString())) {
      return "TABLE";
    } else if (hivetabletype.equals(TableType.VIRTUAL_VIEW.toString())) {
      return "VIEW";
    } else if (hivetabletype.equals(TableType.EXTERNAL_TABLE.toString())) {
      return "EXTERNAL TABLE";
    } else if (hivetabletype.equals(TableType.MATERIALIZED_VIEW.toString())) {
      return "MATERIALIZED VIEW";
    } else {
      return hivetabletype;
    }
  }

  public String getTimeDateFunctions() throws SQLException {
    return "";
  }

  public ResultSet getTypeInfo() throws SQLException {
    TGetTypeInfoResp getTypeInfoResp;
    TGetTypeInfoReq getTypeInfoReq = new TGetTypeInfoReq();
    getTypeInfoReq.setSessionHandle(sessHandle);
    try {
      getTypeInfoResp = client.GetTypeInfo(getTypeInfoReq);
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(getTypeInfoResp.getStatus());
    return new HiveQueryResultSet.Builder(connection)
    .setClient(client)
    .setSessionHandle(sessHandle)
    .setStmtHandle(getTypeInfoResp.getOperationHandle())
    .build();
  }

  public ResultSet getUDTs(String catalog, String schemaPattern,
      String typeNamePattern, int[] types) throws SQLException {

    return new HiveMetaDataResultSet(
            Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE"
                    , "REMARKS", "BASE_TYPE")
            , Arrays.asList("STRING", "STRING", "STRING", "STRING", "INT", "STRING", "INT")
            , null) {

      public boolean next() throws SQLException {
        return false;
      }

      public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        // JDK 1.7
        throw new SQLFeatureNotSupportedException("Method not supported");
      }

      public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        // JDK 1.7
        throw new SQLFeatureNotSupportedException("Method not supported");
        }
    };
  }

  public String getURL() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public String getUserName() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean insertsAreDetected(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean isCatalogAtStart() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean isReadOnly() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean locatorsUpdateCopy() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean nullsAreSortedAtStart() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean nullsAreSortedHigh() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean nullsAreSortedLow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean othersDeletesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean ownDeletesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean ownInsertsAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsANSI92FullSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  public boolean supportsConvert() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsCoreSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsCorrelatedSubqueries() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions()
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsExtendedSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsFullOuterJoins() throws SQLException {
    return true;
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsGroupByUnrelated() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsLikeEscapeClause() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  public boolean supportsMinimumSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsMultipleOpenResults() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  public boolean supportsMultipleTransactions() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsNamedParameters() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsNonNullableColumns() throws SQLException {
    return false;
  }

  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsOrderByUnrelated() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  public boolean supportsResultSetConcurrency(int type, int concurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsResultSetHoldability(int holdability)
      throws SQLException {
    return false;
  }

  public boolean supportsResultSetType(int type) throws SQLException {
    return true;
  }

  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }

  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return true;
  }

  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  public boolean supportsStatementPooling() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsSubqueriesInExists() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsSubqueriesInIns() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsTableCorrelationNames() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsTransactionIsolationLevel(int level)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  public boolean supportsUnion() throws SQLException {
    return false;
  }

  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  public boolean updatesAreDetected(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean usesLocalFilePerTable() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean usesLocalFiles() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public static void main(String[] args) throws SQLException {
    HiveDatabaseMetaData meta = new HiveDatabaseMetaData(null, null, null);
    System.out.println("DriverName: " + meta.getDriverName());
    System.out.println("DriverVersion: " + meta.getDriverVersion());
  }

  private TGetInfoResp getServerInfo(TGetInfoType type) throws SQLException {
    TGetInfoReq req = new TGetInfoReq(sessHandle, type);
    TGetInfoResp resp;
    try {
      resp = client.GetInfo(req);
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(resp.getStatus());
    return resp;
  }
}
