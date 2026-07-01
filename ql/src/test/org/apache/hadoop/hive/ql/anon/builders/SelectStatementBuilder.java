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

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.TestBodyType;
import org.apache.hadoop.hive.ql.anon.TestUtils;

public class SelectStatementBuilder {

  private String tableName;
  private String mColName;
  private String oColName;
  private String bodyColName;
  private ColumnInternalFormat internalFormat;

  public SelectStatementBuilder() {
  }

  public SelectStatementBuilder(final String tableName,
                                final String mColName,
                                final String oColName,
                                final String bodyColName, final ColumnInternalFormat internalFormat) {
    this.tableName = tableName;
    this.mColName = mColName;
    this.oColName = oColName;
    this.bodyColName = bodyColName;
    this.internalFormat = internalFormat;
  }

  public String build() {
    final StringBuilder sb = new StringBuilder();
    final TestBodyType bodyColType = TestUtils.getBodyType(internalFormat);

    sb.append("SELECT ");
    sb.append(mColName).append(", ").append(oColName).append(", ");
    if (bodyColType == TestBodyType.STRING) {
      sb.append(bodyColName);
    } else {
      sb.append("HEX(").append(bodyColName).append(")");
    }
    sb.append(" FROM ");
    sb.append(tableName);
    return sb.toString();
  }

}
