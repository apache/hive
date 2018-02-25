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
package org.apache.hadoop.hive.ql.udf.generic;

import java.sql.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

// This function is not a deterministic function, but a runtime constant.
// The return value is constant within a query but can be different between queries.
@UDFType(deterministic = false, runtimeConstant = true)
@Description(name = "current_date",
    value = "_FUNC_() - Returns the current date at the start of query evaluation."
    + " All calls of current_date within the same query return the same value.")
@NDV(maxNdv = 1)
public class GenericUDFCurrentDate extends GenericUDF {

  protected DateWritable currentDate;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException(
          "The function CURRENT_DATE does not take any arguments, but found "
          + arguments.length);
    }

    if (currentDate == null) {
      Date dateVal =
          Date.valueOf(SessionState.get().getQueryCurrentTimestamp().toString().substring(0, 10));
      currentDate = new DateWritable(dateVal);
    }

    return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return currentDate;
  }

  public DateWritable getCurrentDate() {
    return currentDate;
  }

  public void setCurrentDate(DateWritable currentDate) {
    this.currentDate = currentDate;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "CURRENT_DATE()";
  }


  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    // Need to preserve currentDate
    GenericUDFCurrentDate other = (GenericUDFCurrentDate) newInstance;
    if (this.currentDate != null) {
      other.currentDate = new DateWritable(this.currentDate);
    }
  }
}
