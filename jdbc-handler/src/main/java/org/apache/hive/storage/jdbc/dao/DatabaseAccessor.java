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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;

import java.io.IOException;
import java.util.List;

public interface DatabaseAccessor {

  List<String> getColumnNames(Configuration conf) throws HiveJdbcDatabaseAccessException;

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
