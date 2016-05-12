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

import java.io.PrintStream;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.LongWritable;


@UDFType(deterministic = true)
@Description(name = "unix_timestamp",
    value = "_FUNC_(date[, pattern]) - Converts the time to a number",
    extended = "Converts the specified time to number of seconds "
        + "since 1970-01-01. The _FUNC_(void) overload is deprecated, use current_timestamp.")
public class GenericUDFUnixTimeStamp extends GenericUDFToUnixTimeStamp {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFUnixTimeStamp.class);
  private LongWritable currentTimestamp; // retValue is transient so store this separately.
  private Configuration conf;

  @Override
  public void configure(MapredContext context) {
    super.configure(context);
    conf = context.getJobConf();
  }

  @Override
  protected void initializeInput(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length > 0) {
      super.initializeInput(arguments);
    } else {
      if (currentTimestamp == null) {
        currentTimestamp = new LongWritable(0);
        SessionState ss = SessionState.get();
        Timestamp queryTimestamp;
        if (ss == null) {
          if (conf == null) {
            queryTimestamp = new Timestamp(System.currentTimeMillis());
          } else {
            queryTimestamp = new Timestamp(
                    HiveConf.getLongVar(conf, HiveConf.ConfVars.HIVE_QUERY_TIMESTAMP));
          }
        } else {
          queryTimestamp = ss.getQueryCurrentTimestamp();
        }
        setValueFromTs(currentTimestamp, queryTimestamp);
        String msg = "unix_timestamp(void) is deprecated. Use current_timestamp instead.";
        LOG.warn(msg);
        PrintStream stream = LogHelper.getInfoStream();
        if (stream != null) {
          stream.println(msg);
        }
      }
    }
  }

  @Override
  protected String getName() {
    return "unix_timestamp";
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return (arguments.length == 0) ? currentTimestamp : super.evaluate(arguments);
  }
}
