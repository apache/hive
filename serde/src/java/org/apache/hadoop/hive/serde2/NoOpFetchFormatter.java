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

package org.apache.hadoop.hive.serde2;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * A No-op fetch formatter.
 * ListSinkOperator uses this when reading from the destination table which has data serialized by
 * ThriftJDBCBinarySerDe to a SequenceFile.
 */
public class NoOpFetchFormatter<T> implements FetchFormatter<Object> {

  @Override
  public void initialize(Configuration hconf, Properties props) throws SerDeException {
  }

  // this returns the row as is because this formatter is only called when
  // the ThriftJDBCBinarySerDe was used to serialize the rows to thrift-able objects.
  @Override
  public Object convert(Object row, ObjectInspector rowOI) throws Exception {
    return new Object[] { row };
  }

  @Override
  public void close() throws IOException {
  }
}
