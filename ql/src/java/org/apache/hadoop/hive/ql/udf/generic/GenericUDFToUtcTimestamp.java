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

@Description(name = "to_utc_timestamp",
             value = "to_utc_timestamp(timestamp, string timezone) - "
                     + "Assumes given timestamp is in given timezone and converts to UTC (as of Hive 0.8.0)")
public class GenericUDFToUtcTimestamp extends
    GenericUDFFromUtcTimestamp {

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("Convert ");
    sb.append(children[0]);
    sb.append(" from timezone ");
    if (children.length > 1) {
      sb.append(children[1]);
    }
    sb.append(" to UTC");
    return sb.toString();
  }

  @Override
  public String getName() {
    return "to_utc_timestamp";
  }

  @Override
  protected boolean invert() {
    return true;
  }
}
