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
package org.apache.hadoop.hive.metastore.txn.jdbc;

import org.apache.hadoop.hive.metastore.DatabaseProduct;

/**
 * {@link ParameterizedCommand}. {@link ParameterizedBatchCommand}, and {@link InClauseBatchCommand} implementations can also
 * implement this interface, marking that the execution is conditial, and the command wants to get notified about 
 * execution errors. Can be used to implement commands depending on some feature flag(s).  
 */
public interface ConditionalCommand {

  /**
   * Indicates if the command should be executed or not
   * @param databaseProduct
   * @return Returns true if the command can be executed, false otherwise.
   */
  boolean shouldBeUsed(DatabaseProduct databaseProduct);

  /**
   * Called in case of execution error in order to notify this command about the failure
   * @param databaseProduct
   * @param e The caught Exception
   */
  void onError(DatabaseProduct databaseProduct, Exception e);
  
}
