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

import java.time.ZoneId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Return Unix Timestamp.
 * Extends {@link VectorUDFTimestampFieldDate}
 */
public final class VectorUDFUnixTimeStampDate extends VectorUDFTimestampFieldDate {

  private static final long serialVersionUID = 1L;

  private Date date;
  private ZoneId timeZone;

  @Override
  public void transientInit(Configuration conf) throws HiveException {
    super.transientInit(conf);
    if (timeZone == null) {
      String timeZoneStr = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE);
      timeZone = TimestampTZUtil.parseTimeZone(timeZoneStr);
    }
  }

  @Override
  protected long getDateField(long days) {
    date.setTimeInDays((int) days);
    return TimestampTZUtil.convert(date, timeZone).getEpochSecond();
  }

  public VectorUDFUnixTimeStampDate(int colNum, int outputColumnNum) {
    /* not a real field */
    super(-1, colNum, outputColumnNum);
    date = new Date();
  }

  public VectorUDFUnixTimeStampDate() {
    super();
  }

}
