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

package org.apache.hadoop.hive.jdbc;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * HiveDatabaseMetaData.
 *
 */
public class HiveDatabaseMetaData implements java.sql.DatabaseMetaData {

  /**
   *
   */
  public HiveDatabaseMetaData() {
    // TODO Auto-generated constructor stub
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#allProceduresAreCallable()
   */

  public boolean allProceduresAreCallable() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#allTablesAreSelectable()
   */

  public boolean allTablesAreSelectable() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#autoCommitFailureClosesAllResultSets()
   */

  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#dataDefinitionCausesTransactionCommit()
   */

  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#dataDefinitionIgnoredInTransactions()
   */

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#deletesAreDetected(int)
   */

  public boolean deletesAreDetected(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#doesMaxRowSizeIncludeBlobs()
   */

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getAttributes(java.lang.String,
   * java.lang.String, java.lang.String, java.lang.String)
   */

  public ResultSet getAttributes(String catalog, String schemaPattern,
      String typeNamePattern, String attributeNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getBestRowIdentifier(java.lang.String,
   * java.lang.String, java.lang.String, int, boolean)
   */

  public ResultSet getBestRowIdentifier(String catalog, String schema,
      String table, int scope, boolean nullable) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getCatalogSeparator()
   */

  public String getCatalogSeparator() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getCatalogTerm()
   */

  public String getCatalogTerm() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getCatalogs()
   */

  public ResultSet getCatalogs() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getClientInfoProperties()
   */

  public ResultSet getClientInfoProperties() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getColumnPrivileges(java.lang.String,
   * java.lang.String, java.lang.String, java.lang.String)
   */

  public ResultSet getColumnPrivileges(String catalog, String schema,
      String table, String columnNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getColumns(java.lang.String,
   * java.lang.String, java.lang.String, java.lang.String)
   */

  public ResultSet getColumns(String catalog, String schemaPattern,
      String tableNamePattern, String columnNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getConnection()
   */

  public Connection getConnection() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getCrossReference(java.lang.String,
   * java.lang.String, java.lang.String, java.lang.String, java.lang.String,
   * java.lang.String)
   */

  public ResultSet getCrossReference(String primaryCatalog,
      String primarySchema, String primaryTable, String foreignCatalog,
      String foreignSchema, String foreignTable) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getDatabaseMajorVersion()
   */

  public int getDatabaseMajorVersion() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getDatabaseMinorVersion()
   */

  public int getDatabaseMinorVersion() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getDatabaseProductName()
   */

  public String getDatabaseProductName() throws SQLException {
    return "Hive";
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
   */

  public String getDatabaseProductVersion() throws SQLException {
    return "0";
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getDefaultTransactionIsolation()
   */

  public int getDefaultTransactionIsolation() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getDriverMajorVersion()
   */

  public int getDriverMajorVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getDriverMinorVersion()
   */

  public int getDriverMinorVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getDriverName()
   */

  public String getDriverName() throws SQLException {
    return fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_TITLE);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getDriverVersion()
   */

  public String getDriverVersion() throws SQLException {
    return fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_VERSION);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getExportedKeys(java.lang.String,
   * java.lang.String, java.lang.String)
   */

  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getExtraNameCharacters()
   */

  public String getExtraNameCharacters() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getFunctionColumns(java.lang.String,
   * java.lang.String, java.lang.String, java.lang.String)
   */

  public ResultSet getFunctionColumns(String arg0, String arg1, String arg2,
      String arg3) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getFunctions(java.lang.String,
   * java.lang.String, java.lang.String)
   */

  public ResultSet getFunctions(String arg0, String arg1, String arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getIdentifierQuoteString()
   */

  public String getIdentifierQuoteString() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getImportedKeys(java.lang.String,
   * java.lang.String, java.lang.String)
   */

  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getIndexInfo(java.lang.String,
   * java.lang.String, java.lang.String, boolean, boolean)
   */

  public ResultSet getIndexInfo(String catalog, String schema, String table,
      boolean unique, boolean approximate) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getJDBCMajorVersion()
   */

  public int getJDBCMajorVersion() throws SQLException {
    return 3;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getJDBCMinorVersion()
   */

  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxBinaryLiteralLength()
   */

  public int getMaxBinaryLiteralLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxCatalogNameLength()
   */

  public int getMaxCatalogNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxCharLiteralLength()
   */

  public int getMaxCharLiteralLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxColumnNameLength()
   */

  public int getMaxColumnNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxColumnsInGroupBy()
   */

  public int getMaxColumnsInGroupBy() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxColumnsInIndex()
   */

  public int getMaxColumnsInIndex() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxColumnsInOrderBy()
   */

  public int getMaxColumnsInOrderBy() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxColumnsInSelect()
   */

  public int getMaxColumnsInSelect() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxColumnsInTable()
   */

  public int getMaxColumnsInTable() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxConnections()
   */

  public int getMaxConnections() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxCursorNameLength()
   */

  public int getMaxCursorNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxIndexLength()
   */

  public int getMaxIndexLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxProcedureNameLength()
   */

  public int getMaxProcedureNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxRowSize()
   */

  public int getMaxRowSize() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxSchemaNameLength()
   */

  public int getMaxSchemaNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxStatementLength()
   */

  public int getMaxStatementLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxStatements()
   */

  public int getMaxStatements() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxTableNameLength()
   */

  public int getMaxTableNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxTablesInSelect()
   */

  public int getMaxTablesInSelect() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getMaxUserNameLength()
   */

  public int getMaxUserNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getNumericFunctions()
   */

  public String getNumericFunctions() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getPrimaryKeys(java.lang.String,
   * java.lang.String, java.lang.String)
   */

  public ResultSet getPrimaryKeys(String catalog, String schema, String table)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getProcedureColumns(java.lang.String,
   * java.lang.String, java.lang.String, java.lang.String)
   */

  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
      String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getProcedureTerm()
   */

  public String getProcedureTerm() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getProcedures(java.lang.String,
   * java.lang.String, java.lang.String)
   */

  public ResultSet getProcedures(String catalog, String schemaPattern,
      String procedureNamePattern) throws SQLException {
    // TODO: return empty result set here
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getResultSetHoldability()
   */

  public int getResultSetHoldability() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getRowIdLifetime()
   */

  public RowIdLifetime getRowIdLifetime() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getSQLKeywords()
   */

  public String getSQLKeywords() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getSQLStateType()
   */

  public int getSQLStateType() throws SQLException {
    return DatabaseMetaData.sqlStateSQL99;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getSchemaTerm()
   */

  public String getSchemaTerm() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getSchemas()
   */

  public ResultSet getSchemas() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getSchemas(java.lang.String,
   * java.lang.String)
   */

  public ResultSet getSchemas(String catalog, String schemaPattern)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getSearchStringEscape()
   */

  public String getSearchStringEscape() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getStringFunctions()
   */

  public String getStringFunctions() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getSuperTables(java.lang.String,
   * java.lang.String, java.lang.String)
   */

  public ResultSet getSuperTables(String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getSuperTypes(java.lang.String,
   * java.lang.String, java.lang.String)
   */

  public ResultSet getSuperTypes(String catalog, String schemaPattern,
      String typeNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getSystemFunctions()
   */

  public String getSystemFunctions() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getTablePrivileges(java.lang.String,
   * java.lang.String, java.lang.String)
   */

  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getTableTypes()
   */

  public ResultSet getTableTypes() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getTables(java.lang.String,
   * java.lang.String, java.lang.String, java.lang.String[])
   */

  public ResultSet getTables(String catalog, String schemaPattern,
      String tableNamePattern, String[] types) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getTimeDateFunctions()
   */

  public String getTimeDateFunctions() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getTypeInfo()
   */

  public ResultSet getTypeInfo() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getUDTs(java.lang.String, java.lang.String,
   * java.lang.String, int[])
   */

  public ResultSet getUDTs(String catalog, String schemaPattern,
      String typeNamePattern, int[] types) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getURL()
   */

  public String getURL() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getUserName()
   */

  public String getUserName() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#getVersionColumns(java.lang.String,
   * java.lang.String, java.lang.String)
   */

  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#insertsAreDetected(int)
   */

  public boolean insertsAreDetected(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#isCatalogAtStart()
   */

  public boolean isCatalogAtStart() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#isReadOnly()
   */

  public boolean isReadOnly() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#locatorsUpdateCopy()
   */

  public boolean locatorsUpdateCopy() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#nullPlusNonNullIsNull()
   */

  public boolean nullPlusNonNullIsNull() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#nullsAreSortedAtEnd()
   */

  public boolean nullsAreSortedAtEnd() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#nullsAreSortedAtStart()
   */

  public boolean nullsAreSortedAtStart() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#nullsAreSortedHigh()
   */

  public boolean nullsAreSortedHigh() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#nullsAreSortedLow()
   */

  public boolean nullsAreSortedLow() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#othersDeletesAreVisible(int)
   */

  public boolean othersDeletesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#othersInsertsAreVisible(int)
   */

  public boolean othersInsertsAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#othersUpdatesAreVisible(int)
   */

  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#ownDeletesAreVisible(int)
   */

  public boolean ownDeletesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#ownInsertsAreVisible(int)
   */

  public boolean ownInsertsAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#ownUpdatesAreVisible(int)
   */

  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#storesLowerCaseIdentifiers()
   */

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#storesLowerCaseQuotedIdentifiers()
   */

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#storesMixedCaseIdentifiers()
   */

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#storesMixedCaseQuotedIdentifiers()
   */

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#storesUpperCaseIdentifiers()
   */

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#storesUpperCaseQuotedIdentifiers()
   */

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsANSI92EntryLevelSQL()
   */

  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsANSI92FullSQL()
   */

  public boolean supportsANSI92FullSQL() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsANSI92IntermediateSQL()
   */

  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsAlterTableWithAddColumn()
   */

  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsAlterTableWithDropColumn()
   */

  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsBatchUpdates()
   */

  public boolean supportsBatchUpdates() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsCatalogsInDataManipulation()
   */

  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsCatalogsInIndexDefinitions()
   */

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsCatalogsInPrivilegeDefinitions()
   */

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsCatalogsInProcedureCalls()
   */

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsCatalogsInTableDefinitions()
   */

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsColumnAliasing()
   */

  public boolean supportsColumnAliasing() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsConvert()
   */

  public boolean supportsConvert() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsConvert(int, int)
   */

  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsCoreSQLGrammar()
   */

  public boolean supportsCoreSQLGrammar() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsCorrelatedSubqueries()
   */

  public boolean supportsCorrelatedSubqueries() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * java.sql.DatabaseMetaData#supportsDataDefinitionAndDataManipulationTransactions
   * ()
   */

  public boolean supportsDataDefinitionAndDataManipulationTransactions()
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsDataManipulationTransactionsOnly()
   */

  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsDifferentTableCorrelationNames()
   */

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsExpressionsInOrderBy()
   */

  public boolean supportsExpressionsInOrderBy() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsExtendedSQLGrammar()
   */

  public boolean supportsExtendedSQLGrammar() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsFullOuterJoins()
   */

  public boolean supportsFullOuterJoins() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsGetGeneratedKeys()
   */

  public boolean supportsGetGeneratedKeys() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsGroupBy()
   */

  public boolean supportsGroupBy() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsGroupByBeyondSelect()
   */

  public boolean supportsGroupByBeyondSelect() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsGroupByUnrelated()
   */

  public boolean supportsGroupByUnrelated() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsIntegrityEnhancementFacility()
   */

  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsLikeEscapeClause()
   */

  public boolean supportsLikeEscapeClause() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsLimitedOuterJoins()
   */

  public boolean supportsLimitedOuterJoins() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsMinimumSQLGrammar()
   */

  public boolean supportsMinimumSQLGrammar() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsMixedCaseIdentifiers()
   */

  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsMixedCaseQuotedIdentifiers()
   */

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsMultipleOpenResults()
   */

  public boolean supportsMultipleOpenResults() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsMultipleResultSets()
   */

  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsMultipleTransactions()
   */

  public boolean supportsMultipleTransactions() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsNamedParameters()
   */

  public boolean supportsNamedParameters() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsNonNullableColumns()
   */

  public boolean supportsNonNullableColumns() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsOpenCursorsAcrossCommit()
   */

  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsOpenCursorsAcrossRollback()
   */

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsOpenStatementsAcrossCommit()
   */

  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsOpenStatementsAcrossRollback()
   */

  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsOrderByUnrelated()
   */

  public boolean supportsOrderByUnrelated() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsOuterJoins()
   */

  public boolean supportsOuterJoins() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsPositionedDelete()
   */

  public boolean supportsPositionedDelete() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsPositionedUpdate()
   */

  public boolean supportsPositionedUpdate() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsResultSetConcurrency(int, int)
   */

  public boolean supportsResultSetConcurrency(int type, int concurrency)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsResultSetHoldability(int)
   */

  public boolean supportsResultSetHoldability(int holdability)
      throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsResultSetType(int)
   */

  public boolean supportsResultSetType(int type) throws SQLException {
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsSavepoints()
   */

  public boolean supportsSavepoints() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsSchemasInDataManipulation()
   */

  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsSchemasInIndexDefinitions()
   */

  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsSchemasInPrivilegeDefinitions()
   */

  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsSchemasInProcedureCalls()
   */

  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsSchemasInTableDefinitions()
   */

  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsSelectForUpdate()
   */

  public boolean supportsSelectForUpdate() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsStatementPooling()
   */

  public boolean supportsStatementPooling() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsStoredFunctionsUsingCallSyntax()
   */

  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsStoredProcedures()
   */

  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsSubqueriesInComparisons()
   */

  public boolean supportsSubqueriesInComparisons() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsSubqueriesInExists()
   */

  public boolean supportsSubqueriesInExists() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsSubqueriesInIns()
   */

  public boolean supportsSubqueriesInIns() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsSubqueriesInQuantifieds()
   */

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsTableCorrelationNames()
   */

  public boolean supportsTableCorrelationNames() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsTransactionIsolationLevel(int)
   */

  public boolean supportsTransactionIsolationLevel(int level)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsTransactions()
   */

  public boolean supportsTransactions() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsUnion()
   */

  public boolean supportsUnion() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#supportsUnionAll()
   */

  public boolean supportsUnionAll() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#updatesAreDetected(int)
   */

  public boolean updatesAreDetected(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#usesLocalFilePerTable()
   */

  public boolean usesLocalFilePerTable() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.DatabaseMetaData#usesLocalFiles()
   */

  public boolean usesLocalFiles() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */

  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /**
   * Lazy-load manifest attributes as needed.
   */
  private static Attributes manifestAttributes = null;

  /**
   * Loads the manifest attributes from the jar.
   * 
   * @throws java.net.MalformedURLException
   * @throws IOException
   */
  private synchronized void loadManifestAttributes() throws IOException {
    if (manifestAttributes != null) {
      return;
    }
    Class clazz = this.getClass();
    String classContainer = clazz.getProtectionDomain().getCodeSource()
        .getLocation().toString();
    URL manifestUrl = new URL("jar:" + classContainer
        + "!/META-INF/MANIFEST.MF");
    Manifest manifest = new Manifest(manifestUrl.openStream());
    manifestAttributes = manifest.getMainAttributes();
  }

  /**
   * Helper to initialize attributes and return one.
   * 
   * @param attributeName
   * @return
   * @throws SQLException
   */
  private String fetchManifestAttribute(Attributes.Name attributeName)
      throws SQLException {
    try {
      loadManifestAttributes();
    } catch (IOException e) {
      throw new SQLException("Couldn't load manifest attributes.", e);
    }
    return manifestAttributes.getValue(attributeName);
  }

  public static void main(String[] args) throws SQLException {
    HiveDatabaseMetaData meta = new HiveDatabaseMetaData();
    System.out.println("DriverName: " + meta.getDriverName());
    System.out.println("DriverVersion: " + meta.getDriverVersion());
  }
}
