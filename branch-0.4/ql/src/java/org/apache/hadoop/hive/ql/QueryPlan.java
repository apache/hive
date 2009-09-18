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

package org.apache.hadoop.hive.ql;

import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;

public class QueryPlan {
  private String queryString;
  private BaseSemanticAnalyzer plan;
  private String queryId;

  public QueryPlan(String queryString, BaseSemanticAnalyzer plan) {
    this.queryString = queryString;
    this.plan = plan;
    this.queryId = makeQueryId();
  }

  public String getQueryStr() {
    return queryString;
  }

  public BaseSemanticAnalyzer getPlan() {
    return plan;
  }

  public String getQueryId() {
    return queryId;
  }

  private String makeQueryId() {
    GregorianCalendar gc = new GregorianCalendar();
    String userid = System.getProperty("user.name");

    return userid + "_" +
      String.format("%1$4d%2$02d%3$02d%4$02d%5$02d%5$02d", gc.get(Calendar.YEAR),
                    gc.get(Calendar.MONTH) + 1,
                    gc.get(Calendar.DAY_OF_MONTH),
                    gc.get(Calendar.HOUR_OF_DAY),
                    gc.get(Calendar.MINUTE), gc.get(Calendar.SECOND));
  }
}
