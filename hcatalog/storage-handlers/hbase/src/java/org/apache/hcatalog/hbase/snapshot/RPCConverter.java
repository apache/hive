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
package org.apache.hcatalog.hbase.snapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RPCConverter {

  List<FamilyRevision> convertFamilyRevisions(List<RevisionManagerEndpointProtos.FamilyRevision> revisions) {
    List<FamilyRevision> result = new ArrayList<FamilyRevision>();
    for(RevisionManagerEndpointProtos.FamilyRevision revision : revisions) {
      result.add(new FamilyRevision(revision.getRevision(), revision.getTimestamp()));
    }
    return result;
  }
  RevisionManagerEndpointProtos.TableSnapshot convertTableSnapshot(TableSnapshot tableSnapshot) {
    RevisionManagerEndpointProtos.TableSnapshot.Builder builder =
        RevisionManagerEndpointProtos.TableSnapshot.newBuilder()
        .setTableName(tableSnapshot.getTableName())
        .setLatestRevision(tableSnapshot.getLatestRevision());
    Map<String, Long> cfRevisionMap = tableSnapshot.getColumnFamilyRevisionMap();
    for(Map.Entry<String, Long> entry : cfRevisionMap.entrySet()) {
      builder.addColumnFamilyRevision(RevisionManagerEndpointProtos.TableSnapshot.
          ColumnFamilyRevision.newBuilder()
          .setKey(entry.getKey())
          .setValue(entry.getValue()).build());
    }
    return builder.build();
  }

  TableSnapshot convertTableSnapshot(RevisionManagerEndpointProtos.TableSnapshot tableSnapshot) {
    Map<String, Long> columnFamilyRevisions = new HashMap<String, Long>();
    for(RevisionManagerEndpointProtos.TableSnapshot.ColumnFamilyRevision rev : tableSnapshot.getColumnFamilyRevisionList()) {
      columnFamilyRevisions.put(rev.getKey(), rev.getValue());
    }
    return new TableSnapshot(tableSnapshot.getTableName(), columnFamilyRevisions, tableSnapshot.getLatestRevision());
  }

  RevisionManagerEndpointProtos.Transaction convertTransaction(Transaction transaction) {
    return RevisionManagerEndpointProtos.Transaction.newBuilder()
      .setTableName(transaction.getTableName())
      .addAllColumnFamilies(transaction.getColumnFamilies())
      .setRevision(transaction.getRevisionNumber())
      .setTimeStamp(transaction.getTimeStamp())
      .setKeepAlive(transaction.getKeepAliveValue())
      .build();
  }
  Transaction convertTransaction(RevisionManagerEndpointProtos.Transaction transaction) {
    Transaction result = new Transaction(transaction.getTableName(), transaction.getColumnFamiliesList(), transaction.getRevision(), transaction.getTimeStamp());
    result.setKeepAlive(transaction.getKeepAlive());
    return result;
  }
}
