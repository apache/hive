/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.hive.service.cli.operation.hplsql;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.hplsql.executor.ColumnMeta;
import org.apache.hive.hplsql.executor.Metadata;
import org.apache.hive.hplsql.executor.QueryException;
import org.apache.hive.hplsql.executor.QueryExecutor;
import org.apache.hive.hplsql.executor.QueryResult;
import org.apache.hive.hplsql.executor.RowResult;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

/**
 * Executing HiveQL from HPL/SQL directly, without JDBC or Thrift.
 */
public class HplSqlQueryExecutor implements QueryExecutor {
  public static final String QUERY_EXECUTOR = "QUERY_EXECUTOR";
  public static final String HPLSQL = "HPLSQL";
  private final HiveSession hiveSession;
  private final long fetchSize;

  public HplSqlQueryExecutor(HiveSession hiveSession) {
    this.fetchSize = hiveSession.getHiveConf().getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE);
    this.hiveSession = hiveSession;
  }

  @Override
  public QueryResult executeQuery(String sql, ParserRuleContext ctx) {
    try {
      Map<String, String> confOverlay = new HashMap<>();
      confOverlay.put(QUERY_EXECUTOR, HPLSQL);
      OperationHandle operationHandle = hiveSession.executeStatement(sql, confOverlay);
      return new QueryResult(new OperationRowResult(operationHandle), () -> metadata(operationHandle), null);
    } catch (HiveSQLException e) {
      return new QueryResult(null, () -> new Metadata(Collections.emptyList()), e);
    }
  }

  public Metadata metadata(OperationHandle operationHandle) {
    try {
      TableSchema meta = hiveSession.getResultSetMetadata(operationHandle);
      List<ColumnMeta> colMeta = new ArrayList<>();
      for (int i = 0; i < meta.getSize(); i++) {
        ColumnDescriptor col = meta.getColumnDescriptorAt(i);
        colMeta.add(new ColumnMeta(col.getName(), col.getTypeName(), col.getType().toJavaSQLType()));
      }
      return new Metadata(colMeta);
    } catch (HiveSQLException e) {
      throw new QueryException(e);
    }
  }

  private class OperationRowResult implements RowResult {
    private final OperationHandle handle;
    private RowSet rows;
    private Iterator<Object[]> iterator;
    private Object[] current;

    private OperationRowResult(OperationHandle operationHandle) {
      this.handle = operationHandle;
    }

    @Override
    public boolean next() {
      if (rows == null || !iterator.hasNext()) {
        this.rows = fetch();
        this.iterator = rows.iterator();
      }
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      } else {
        current = null;
        return false;
      }
    }

    private RowSet fetch() {
      try {
        return hiveSession.fetchResults(
                handle, FetchOrientation.FETCH_NEXT, fetchSize, FetchType.QUERY_OUTPUT);
      } catch (HiveSQLException e) {
        throw new QueryException(e);
      }
    }

    @Override
    public <T> T get(int columnIndex, Class<T> type) {
      if (current[columnIndex] == null) {
        return null;
      }
      if (type.isInstance(current[columnIndex])) {
        return (T) current[columnIndex];
      } else {
        if (current[columnIndex] instanceof Number) {
          if (type == Long.class)
              return type.cast(((Number) current[columnIndex]).longValue());
          if (type == Integer.class)
              return type.cast(((Number) current[columnIndex]).intValue());
          if (type == Short.class)
              return type.cast(((Number) current[columnIndex]).shortValue());
          if (type == Byte.class)
              return type.cast(((Number) current[columnIndex]).byteValue());
          if (type == String.class)
            return (T) String.valueOf(current[columnIndex]);
        }
        // RowSet can never return the HiveDecimal instances created on Hive side, nor its BigDecimal representation.
        // Instead, it gets converted into String object in ColumnBasedSet.addRow()...
        if (type == BigDecimal.class &&
            serdeConstants.DECIMAL_TYPE_NAME.equalsIgnoreCase(metadata(handle).columnTypeName(columnIndex))) {
          return (T) new BigDecimal((String) current[columnIndex]);
        }
        throw new ClassCastException(current[columnIndex].getClass() + " cannot be casted to " + type);
      }
    }

    @Override
    public void close() {
      try {
        hiveSession.closeOperation(handle);
      } catch (HiveSQLException e) {
        throw new QueryException(e);
      }
    }
  }
}
