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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Interface used to create Writable objects from vector expression primitives.
 *
 */
public interface VectorExpressionWriter {
  ObjectInspector getObjectInspector();
  Object writeValue(ColumnVector column, int row) throws HiveException;
  Object writeValue(long value) throws HiveException;
  Object writeValue(double value) throws HiveException;
  Object writeValue(byte[] value, int start, int length) throws HiveException;
  Object writeValue(HiveDecimalWritable value) throws HiveException;
  Object writeValue(HiveDecimal value) throws HiveException;
  Object writeValue(TimestampWritableV2 value) throws HiveException;
  Object writeValue(Timestamp value) throws HiveException;
  Object writeValue(HiveIntervalDayTimeWritable value) throws HiveException;
  Object writeValue(HiveIntervalDayTime value) throws HiveException;
  Object setValue(Object row, ColumnVector column, int columnRow) throws HiveException;
  Object initValue(Object ost) throws HiveException;
}
