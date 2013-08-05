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

package org.apache.hadoop.hive.ql.exec.vector.util;

import java.sql.Timestamp;

/**
 *
 * AllTypesRecord.
 *
 */
public class AllVectorTypesRecord {
  private final Byte ctinyInt;
  private final Short csmallInt;
  private final Integer cint;
  private final Long cbigInt;

  private final Float cfloat;
  private final Double cdouble;

  private final String cstring1;
  private final String cstring2;

  private final Timestamp ctimestamp1;
  private final Timestamp ctimestamp2;

  private final Boolean cboolean1;
  private final Boolean cboolean2;

  /**
   *
   * @param ctinyInt
   * @param csmallInt
   * @param cint
   * @param cbigInt
   * @param cfloat
   * @param cdouble
   * @param cstring1
   * @param cstring2
   * @param ctimestamp1
   * @param ctimestamp2
   * @param cboolean1
   * @param cboolean2
   */
  public AllVectorTypesRecord(Byte ctinyInt, Short csmallInt, Integer cint, Long cbigInt,
      Float cfloat, Double cdouble, String cstring1, String cstring2, Timestamp ctimestamp1,
      Timestamp ctimestamp2, Boolean cboolean1, Boolean cboolean2) {

    this.ctinyInt = ctinyInt;
    this.csmallInt = csmallInt;
    this.cint = cint;
    this.cbigInt = cbigInt;

    this.cfloat = cfloat;
    this.cdouble = cdouble;

    this.cstring1 = cstring1;
    this.cstring2 = cstring2;

    this.ctimestamp1 = ctimestamp1;
    this.ctimestamp2 = ctimestamp2;

    this.cboolean1 = cboolean1;
    this.cboolean2 = cboolean2;
  }

  public static final String TABLE_NAME = "alltypesorc";

  public static final String TABLE_CREATE_COMMAND =
      "CREATE TABLE " + TABLE_NAME + "(" +
          "ctinyint tinyint, " +
          "csmallint smallint, " +
          "cint int, " +
          "cbigint bigint, " +
          "cfloat float, " +
          "cdouble double, " +
          "cstring1 string, " +
          "cstring2 string, " +
          "ctimestamp1 timestamp, " +
          "ctimestamp2 timestamp, " +
          "cboolean1 boolean, " +
          "cboolean2 boolean) " +
          "STORED AS ORC";
}
