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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.hbase;

import org.apache.hcatalog.common.HCatConstants;

/**
 * Constants class for constants used in HBase storage handler.
 */
class HBaseConstants {

  /** key used to store write transaction object */
  public static final String PROPERTY_WRITE_TXN_KEY = HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX + ".hbase.mapreduce.writeTxn";

  /** key used to define the name of the table to write to */
  public static final String PROPERTY_OUTPUT_TABLE_NAME_KEY = HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX + ".hbase.mapreduce.outputTableName";

  /** key used to define whether bulk storage output format will be used or not  */
  public static final String PROPERTY_BULK_OUTPUT_MODE_KEY = HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX + ".hbase.output.bulkMode";

  /** key used to define the hbase table snapshot. */
  public static final String PROPERTY_TABLE_SNAPSHOT_KEY = HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX + "hbase.table.snapshot";

}
