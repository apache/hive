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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;

import java.io.IOException;
import java.util.List;

public interface DatabaseAccessor extends AutoCloseable {

  List<String> getColumnNames(Configuration conf) throws HiveJdbcDatabaseAccessException;

  /**
   * Returns a list of types for the columns in the specified configuration.
   *
   * The type must represent as close as possible the respective type of the column stored in the
   * database. Since it does not exist an exact mapping between database types and Hive types the
   * result is approximate. When it is not possible to derive a type for a given column the 
   * {@link org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory#unknownTypeInfo} is used.
   *
   * There is a one-to-one correspondence between the types returned in this method and the column
   * names obtained with {@link #getColumnNames(Configuration)}.
   *
   * Implementors of the method can derive the types by querying the database, exploit the state
   * of the accessor, or use the configuration.
   *
   * @throws HiveJdbcDatabaseAccessException if some error occurs while accessing the database
   */
  List<TypeInfo> getColumnTypes(Configuration conf) throws HiveJdbcDatabaseAccessException;

  int getTotalNumberOfRecords(Configuration conf) throws HiveJdbcDatabaseAccessException;

  JdbcRecordIterator
    getRecordIterator(Configuration conf, String partitionColumn, String lowerBound, String upperBound, int limit, int
          offset) throws
          HiveJdbcDatabaseAccessException;

  RecordWriter getRecordWriter(TaskAttemptContext context)
      throws IOException;

  Pair<String, String> getBounds(Configuration conf, String partitionColumn, boolean lower, boolean upper) throws
          HiveJdbcDatabaseAccessException;

  boolean needColumnQuote();
}
