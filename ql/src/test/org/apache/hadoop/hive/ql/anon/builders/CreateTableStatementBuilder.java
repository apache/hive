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
import org.apache.hadoop.hive.ql.anon.FileType;
import org.apache.hadoop.hive.ql.anon.TestBodyType;
import org.apache.hadoop.hive.ql.anon.TestUtils;

public class CreateTableStatementBuilder {

  private String tableName;
  private boolean ifNotExists;
  private String mColName;
  private String mColType = "int";
  private String oColName;
  private String oColType = "bigint";
  private String bColName;
  private ColumnInternalFormat internalFormat;
  private FileType fileType;

  public CreateTableStatementBuilder() {
  }

  public CreateTableStatementBuilder(final String tableName, final String mColName, final String oColName, final String bColName) {
    this.tableName = tableName;
    this.mColName = mColName;
    this.oColName = oColName;
    this.bColName = bColName;
  }

  public CreateTableStatementBuilder withIfNotExists() {
    this.ifNotExists = true;
    return this;
  }

  public CreateTableStatementBuilder withInternalFormat(final ColumnInternalFormat internalFormat) {
    this.internalFormat = internalFormat;
    return this;
  }

  public CreateTableStatementBuilder withFileType(final FileType fileType){
    this.fileType = fileType;
    return this;
  }

  public String build() {
    final String ifne = ifNotExists ? "IF NOT EXISTS " : "";
    final TestBodyType bColType = TestUtils.getBodyType(internalFormat);
    return "CREATE TABLE " + ifne + tableName + " (" +
      mColName + " " + mColType + ", " +
      oColName + " " + oColType + ", " +
      bColName + " " + bColType.name() +
      ")" + (fileType == null ? "" : " STORED AS " + fileType.name());
  }

}
