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
package org.apache.hadoop.hive.metastore.txn;

public class TxnHandlingFeatures {

  // Whether to use min_history_level table or not.
  // At startup we read it from the config, but set it to false if min_history_level does nto exists.
  private static boolean useMinHistoryLevel;
  private static boolean useMinHistoryWriteId;

  public static boolean useMinHistoryLevel() {
    return useMinHistoryLevel;
  }

  public static void setUseMinHistoryLevel(boolean useMinHistoryLevel) {
    TxnHandlingFeatures.useMinHistoryLevel = useMinHistoryLevel;
  }

  public static boolean useMinHistoryWriteId() {
    return useMinHistoryWriteId;
  }

  public static void setUseMinHistoryWriteId(boolean useMinHistoryWriteId) {
    TxnHandlingFeatures.useMinHistoryWriteId = useMinHistoryWriteId;
  }
}
