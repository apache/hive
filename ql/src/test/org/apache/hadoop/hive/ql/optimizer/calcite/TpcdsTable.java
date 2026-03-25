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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;

public enum TpcdsTable {
  CUSTOMER(
      80000000,
      f -> f.builder()
          .add("c_customer_sk", SqlTypeName.BIGINT)
          .nullable(false)
          .add("c_customer_id", SqlTypeName.CHAR, 16)
          .nullable(false)
          .add("c_current_cdemo_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("c_current_hdemo_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("c_current_addr_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("c_first_shipto_date_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("c_first_sales_date_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("c_salutation", SqlTypeName.CHAR, 10)
          .nullable(true)
          .add("c_first_name", SqlTypeName.CHAR, 20)
          .nullable(true)
          .add("c_last_name", SqlTypeName.CHAR, 30)
          .nullable(true)
          .add("c_preferred_cust_flag", SqlTypeName.CHAR, 1)
          .nullable(true)
          .add("c_birth_day", SqlTypeName.INTEGER)
          .nullable(true)
          .add("c_birth_month", SqlTypeName.INTEGER)
          .nullable(true)
          .add("c_birth_year", SqlTypeName.INTEGER)
          .nullable(true)
          .add("c_birth_country", SqlTypeName.VARCHAR, 20)
          .nullable(true)
          .add("c_login", SqlTypeName.CHAR, 13)
          .nullable(true)
          .add("c_email_address", SqlTypeName.CHAR, 50)
          .nullable(true)
          .add("c_last_review_date_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .addAll(systemColumns(f).getFieldList())
          .build()), STORE_RETURNS(
      8332595709d,
      f -> f.builder()
          .add("sr_return_time_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("sr_item_sk", SqlTypeName.BIGINT)
          .nullable(false)
          .add("sr_customer_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("sr_cdemo_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("sr_hdemo_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("sr_addr_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("sr_store_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("sr_reason_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("sr_ticket_number", SqlTypeName.BIGINT)
          .nullable(false)
          .add("sr_return_quantity", SqlTypeName.INTEGER)
          .nullable(true)
          .add("sr_return_amt", SqlTypeName.DECIMAL, 7, 2)
          .nullable(true)
          .add("sr_return_tax", SqlTypeName.DECIMAL, 7, 2)
          .nullable(true)
          .add("sr_return_amt_inc_tax", SqlTypeName.DECIMAL, 7, 2)
          .nullable(true)
          .add("sr_fee", SqlTypeName.DECIMAL, 7, 2)
          .nullable(true)
          .add("sr_return_ship_cost", SqlTypeName.DECIMAL, 7, 2)
          .nullable(true)
          .add("sr_refunded_cash", SqlTypeName.DECIMAL, 7, 2)
          .nullable(true)
          .add("sr_reversed_charge", SqlTypeName.DECIMAL, 7, 2)
          .nullable(true)
          .add("sr_store_credit", SqlTypeName.DECIMAL, 7, 2)
          .nullable(true)
          .add("sr_net_loss", SqlTypeName.DECIMAL, 7, 2)
          .nullable(true)
          .add("sr_returned_date_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .addAll(systemColumns(f).getFieldList())
          .build()), DATE_DIM(
      73049,
      f -> f.builder()
          .add("d_date_sk", SqlTypeName.BIGINT)
          .nullable(false)
          .add("d_date_id", SqlTypeName.VARCHAR, Integer.MAX_VALUE)
          .nullable(false)
          .add("d_date", SqlTypeName.DATE)
          .nullable(true)
          .add("d_month_seq", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_week_seq", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_quarter_seq", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_year", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_dow", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_moy", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_dom", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_qoy", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_fy_year", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_fy_quarter_seq", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_fy_week_seq", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_day_name", SqlTypeName.CHAR, 9)
          .nullable(true)
          .add("d_quarter_name", SqlTypeName.CHAR, 6)
          .nullable(true)
          .add("d_holiday", SqlTypeName.CHAR, 1)
          .nullable(true)
          .add("d_weekend", SqlTypeName.CHAR, 1)
          .nullable(true)
          .add("d_following_holiday", SqlTypeName.CHAR, 1)
          .nullable(true)
          .add("d_first_dom", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_last_dom", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_same_day_ly", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_same_day_lq", SqlTypeName.INTEGER)
          .nullable(true)
          .add("d_current_day", SqlTypeName.CHAR, 1)
          .nullable(true)
          .add("d_current_week", SqlTypeName.CHAR, 1)
          .nullable(true)
          .add("d_current_month", SqlTypeName.CHAR, 1)
          .nullable(true)
          .add("d_current_quarter", SqlTypeName.CHAR, 1)
          .nullable(true)
          .add("d_current_year", SqlTypeName.CHAR, 1)
          .nullable(true)
          .addAll(systemColumns(f).getFieldList())
          .build()), STORE(
      1704,
      f -> f.builder()
          .add("s_store_sk", SqlTypeName.BIGINT)
          .nullable(false)
          .add("s_store_id", SqlTypeName.VARCHAR, Integer.MAX_VALUE)
          .nullable(false)
          .add("s_rec_start_date", SqlTypeName.DATE)
          .nullable(true)
          .add("s_rec_end_date", SqlTypeName.DATE)
          .nullable(true)
          .add("s_closed_date_sk", SqlTypeName.BIGINT)
          .nullable(true)
          .add("s_store_name", SqlTypeName.VARCHAR, 50)
          .nullable(true)
          .add("s_number_employees", SqlTypeName.INTEGER)
          .nullable(true)
          .add("s_floor_space", SqlTypeName.INTEGER)
          .nullable(true)
          .add("s_hours", SqlTypeName.CHAR, 20)
          .nullable(true)
          .add("s_manager", SqlTypeName.VARCHAR, 40)
          .nullable(true)
          .add("s_market_id", SqlTypeName.INTEGER)
          .nullable(true)
          .add("s_geography_class", SqlTypeName.VARCHAR, 100)
          .nullable(true)
          .add("s_market_desc", SqlTypeName.VARCHAR, 100)
          .nullable(true)
          .add("s_market_manager", SqlTypeName.VARCHAR, 40)
          .nullable(true)
          .add("s_division_id", SqlTypeName.INTEGER)
          .nullable(true)
          .add("s_division_name", SqlTypeName.VARCHAR, 50)
          .nullable(true)
          .add("s_company_id", SqlTypeName.INTEGER)
          .nullable(true)
          .add("s_company_name", SqlTypeName.VARCHAR, 50)
          .nullable(true)
          .add("s_street_number", SqlTypeName.VARCHAR, 10)
          .nullable(true)
          .add("s_street_name", SqlTypeName.VARCHAR, 60)
          .nullable(true)
          .add("s_street_type", SqlTypeName.CHAR, 15)
          .nullable(true)
          .add("s_suite_number", SqlTypeName.CHAR, 10)
          .nullable(true)
          .add("s_city", SqlTypeName.VARCHAR, 60)
          .nullable(true)
          .add("s_county", SqlTypeName.VARCHAR, 30)
          .nullable(true)
          .add("s_state", SqlTypeName.CHAR, 2)
          .nullable(true)
          .add("s_zip", SqlTypeName.CHAR, 10)
          .nullable(true)
          .add("s_country", SqlTypeName.VARCHAR, 20)
          .nullable(true)
          .add("s_gmt_offset", SqlTypeName.DECIMAL, 5, 2)
          .nullable(true)
          .add("s_tax_percentage", SqlTypeName.DECIMAL, 5, 2)
          .nullable(true)
          .addAll(systemColumns(f).getFieldList())
          .build());
  private final RelProtoDataType protoType;
  private final double rowCount;

  TpcdsTable(double rowCount, RelProtoDataType protoType) {
    this.protoType = protoType;
    this.rowCount = rowCount;
  }

  public RelDataType type(RelDataTypeFactory typeFactory) {
    return protoType.apply(typeFactory);
  }

  public double rowCount() {
    return rowCount;
  }

  private static RelDataType systemColumns(RelDataTypeFactory factory) {
    RelDataType rowId = factory.builder()
        .add("writeid", SqlTypeName.BIGINT)
        .nullable(true)
        .add("bucketid", SqlTypeName.INTEGER)
        .nullable(true)
        .add("rowid", SqlTypeName.BIGINT)
        .nullable(true)
        .build();
    return factory.builder()
        .add("BLOCK__OFFSET__INSIDE__FILE", SqlTypeName.BIGINT)
        .nullable(true)
        .add("INPUT__FILE__NAME", SqlTypeName.VARCHAR, Integer.MAX_VALUE)
        .nullable(true)
        .add("ROW__ID", rowId)
        .nullable(true)
        .add("ROW__IS__DELETED", SqlTypeName.BOOLEAN)
        .nullable(true)
        .build();
  }
}
