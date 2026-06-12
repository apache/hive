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

import com.google.common.collect.ImmutableSet;

import java.sql.SQLException;
import java.util.Set;

/**
 * Per-database ignorable DDL error codes for idempotent schema upgrades.
 * This is used by {@link IdempotentDDLExecutor} to determine whether a given {@link SQLException}
 * can be safely ignored (e.g. because it indicates an object already exists or is already gone).
 * <p> 
 * Use {@link #forDbType(String)} to get the right instance.
 */
public interface DbErrorCodes {

  /** @return true if the exception can be safely ignored (object already exists or already gone). */
  boolean isIgnorable(SQLException e);

  /**
   * Checks both {@link SQLException#getSQLState()} and {@code String.valueOf(getErrorCode())}
   * against {@code duplicateCodes} and {@code missingCodes}.
   */
  class Codes implements DbErrorCodes {
    private final Set<String> duplicateCodes;
    private final Set<String> missingCodes;

    Codes(Set<String> duplicateCodes, Set<String> missingCodes) {
      this.duplicateCodes = duplicateCodes;
      this.missingCodes = missingCodes;
    }

    @Override
    public boolean isIgnorable(SQLException e) {
      String state = e.getSQLState();
      String code = String.valueOf(e.getErrorCode());
      return duplicateCodes.contains(state) || duplicateCodes.contains(code)
          || missingCodes.contains(state)   || missingCodes.contains(code);
    }
  }

  DbErrorCodes POSTGRES = new Codes(
      ImmutableSet.of(
          "42P07",  // duplicate table
          "42701",  // duplicate column
          "42710"   // duplicate object (e.g. constraint, index)
      ),
      ImmutableSet.of(
          "42P01",  // undefined table
          "42703",  // undefined column
          "42704"   // undefined object
      )
  );

  DbErrorCodes DERBY = new Codes(
      ImmutableSet.of(
          "X0Y32",  // table/view already exists
          "X0Y68",  // index already exists
          "42Z93"   // duplicate constraint (same column set already constrained)
      ),
      ImmutableSet.of(
          "42Y55",  // table/view does not exist
          "42X14",  // column does not exist
          "42X65",  // index does not exist
          "42X86"   // constraint does not exist on table (ALTER TABLE DROP CONSTRAINT)
      )
  );

  DbErrorCodes MYSQL = new Codes(
      ImmutableSet.of(
          "1050",   // ER_TABLE_EXISTS_ERROR: table already exists
          "1060",   // ER_DUP_FIELDNAME: duplicate column name
          "1061"    // ER_DUP_KEYNAME: duplicate key name
      ),
      ImmutableSet.of(
          "1051",   // ER_BAD_TABLE_ERROR: unknown table (DROP TABLE)
          "1054",   // ER_BAD_FIELD_ERROR: unknown column (CHANGE COLUMN on already-renamed column)
          "1091"    // ER_CANT_DROP_FIELD_OR_KEY: column or index does not exist (DROP COLUMN/INDEX)
      )
  );

  DbErrorCodes ORACLE = new Codes(
      ImmutableSet.of(
          "955",    // ORA-00955: name already used by an existing object
          "957",    // ORA-00957: duplicate column name (RENAME COLUMN target already exists)
          "1430",   // ORA-01430: column being added already exists in table
          "2261"    // ORA-02261: unique or primary key already exists in the table
      ),
      ImmutableSet.of(
          "942",    // ORA-00942: table or view does not exist
          "904",    // ORA-00904: invalid identifier (column does not exist)
          "1418",   // ORA-01418: specified index does not exist
          "2443"    // ORA-02443: cannot drop constraint - nonexistent constraint
      )
  );

  DbErrorCodes MSSQL = new Codes(
      ImmutableSet.of(
          "2714",   // There is already an object named '...' in the database
          "2705",   // Column names in each table must be unique (duplicate column)
          "1913"    // There is already an index named '...' on table '...'
      ),
      ImmutableSet.of(
          "3701",   // Cannot drop object because it does not exist or you do not have permission
          "3728",   // DROP CONSTRAINT: not a constraint
          "4924",   // ALTER TABLE DROP COLUMN: column does not exist
          "15248"   // sp_rename: parameter @objname is ambiguous or @objtype is wrong (column already renamed)
      )
  );

  /** No-op instance for unrecognized database types; never ignores any error. */
  DbErrorCodes NOOP = e -> false;

  /** Returns the {@link DbErrorCodes} for the given db-type string, or {@link #NOOP} if unrecognized. */
  static DbErrorCodes forDbType(String dbType) {
    if (dbType == null) {
      return NOOP;
    }
    return switch (dbType.toLowerCase()) {
    case "postgres" -> POSTGRES;
    case "derby", "derby.clean" -> DERBY;
    case "mysql", "mariadb" -> MYSQL;
    case "oracle" -> ORACLE;
    case "mssql" -> MSSQL;
    default -> NOOP;
    };
  }
}
