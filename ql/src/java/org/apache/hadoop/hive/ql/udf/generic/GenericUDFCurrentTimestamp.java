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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

// If there is a new UDFType to describe a function that is deterministic within a query
// but changes value between queries, this function would fall into that category.
@UDFType(deterministic = true)
@Description(name = "current_timestamp",
    value = "_FUNC_() - Returns the current timestamp at the start of query evaluation."
    + " All calls of current_timestamp within the same query return the same value.")
public class GenericUDFCurrentTimestamp extends GenericUDF {

  protected TimestampWritable currentTimestamp;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException(
          "The function CURRENT_TIMESTAMP does not take any arguments, but found "
          + arguments.length);
    }

    if (currentTimestamp == null) {
      currentTimestamp = new TimestampWritable(SessionState.get().getQueryCurrentTimestamp());
    }

    return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return currentTimestamp;
  }

  public TimestampWritable getCurrentTimestamp() {
    return currentTimestamp;
  }

  public void setCurrentTimestamp(TimestampWritable currentTimestamp) {
    this.currentTimestamp = currentTimestamp;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "CURRENT_TIMESTAMP()";
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    // Need to preserve currentTimestamp
    GenericUDFCurrentTimestamp other = (GenericUDFCurrentTimestamp) newInstance;
    if (this.currentTimestamp != null) {
      other.currentTimestamp = new TimestampWritable(this.currentTimestamp);
    }
  }
}
