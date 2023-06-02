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
package org.apache.hadoop.hive.metastore.txn.entities;

import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;

public class CompactionCandidate extends CompactionInfoBase {

  public boolean tooManyAborts;
  public boolean hasOldAbort;
  
  public CompactionInfo toFailedCompacion(String errorMessage) {
    CompactionInfo ci = new CompactionInfo();
    ci.dbname = dbname;
    ci.tableName = tableName;
    ci.partName = partName;
    ci.hasOldAbort = hasOldAbort;
    ci.tooManyAborts = tooManyAborts;
    ci.errorMessage = errorMessage;
    ci.state = TxnStore.DID_NOT_INITIATE;
    return ci;
  }
  
}
