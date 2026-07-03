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

package org.apache.hadoop.hive.ql.anon.builders;

import org.apache.hadoop.hive.ql.anon.TestBodyType;

public class InsertStatementBuilder {

  private int numRows;
  private String tableName;
  private int mColValue;
  private int oColValue;
  private String bodyColValue;
  private TestBodyType bodyColType;

  public InsertStatementBuilder() {
  }

  public InsertStatementBuilder(final int numRows, final String tableName,
                                final int mColValue,
                                final int oColValue,
                                final String bodyColValue, final TestBodyType bodyColType) {
    this.numRows = numRows;
    this.tableName = tableName;
    this.mColValue = mColValue;
    this.oColValue = oColValue;
    this.bodyColValue = bodyColValue;
    this.bodyColType = bodyColType;
  }

  public String build() {
    final StringBuilder sb = new StringBuilder();

    sb.append("INSERT INTO TABLE ");
    sb.append(tableName);
    sb.append(" VALUES ");

    for (int i = 0; i < numRows; i++) {
      sb.append("(").append(mColValue).append(",").append(oColValue + i).append(",");
      if (bodyColType == TestBodyType.STRING) {
        sb.append("'").append(bodyColValue).append("'");
      } else {
        sb.append("UNHEX('").append(bodyColValue).append("')");
      }
      sb.append(")");
      if (i < numRows - 1) {
        sb.append(",");
      }
    }

    return sb.toString();
  }
}