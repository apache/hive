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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.LongWritable;


@UDFType(deterministic = true)
@Description(name = "unix_timestamp",
    value = "_FUNC_(date[, pattern]) - Converts the time to a number",
    extended = "Converts the specified time to number of seconds "
        + "since 1970-01-01. The _FUNC_(void) overload is deprecated, use current_timestamp.")
public class GenericUDFUnixTimeStamp extends GenericUDFToUnixTimeStamp {

  private LongWritable currentInstant; // retValue is transient so store this separately.

  @Override
  protected void initializeInput(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length > 0) {
      super.initializeInput(arguments);
    } else {
      if (currentInstant == null) {
        currentInstant = new LongWritable(0);
        currentInstant.set(SessionState.get().getQueryCurrentTimestamp().toEpochMilli());
        String msg = "unix_timestamp(void) is deprecated. Use current_timestamp instead.";
        SessionState.getConsole().printInfo(msg, false);
      }
    }
  }

  @Override
  protected String getName() {
    return "unix_timestamp";
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return (arguments.length == 0) ? currentInstant : super.evaluate(arguments);
  }
}
