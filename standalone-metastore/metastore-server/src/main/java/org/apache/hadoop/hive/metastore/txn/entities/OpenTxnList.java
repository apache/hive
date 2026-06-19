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

import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.TxnType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.txn.entities.TxnStatus.ABORTED;
import static org.apache.hadoop.hive.metastore.txn.entities.TxnStatus.OPEN;

/**
 * Class for the getOpenTxnList calculation.
 */
public class OpenTxnList {
  
  private final long hwm;
  private final List<OpenTxn> openTxnList;

  public OpenTxnList(long hwm, List<OpenTxn> openTxnList) {
    this.hwm = hwm;
    this.openTxnList = openTxnList;
  }

  public GetOpenTxnsInfoResponse toOpenTxnsInfoResponse() {
    return new GetOpenTxnsInfoResponse(getHwm(), openTxnList.stream().map(OpenTxn::toTxnInfo).collect(toList()));
  }

  public long getHwm() {
    return hwm;
  }

  public List<OpenTxn> getOpenTxnList() {
    return openTxnList;
  }

  public GetOpenTxnsResponse toOpenTxnsResponse(List<TxnType> excludeTxnTypes) {
    List<Long> openList = new ArrayList<>();
    long minOpenTxn = Long.MAX_VALUE;
    BitSet abortedBits = new BitSet();
    for (OpenTxn openTxn : getOpenTxnList()) {
      if (openTxn.getStatus() == OPEN) {
        minOpenTxn = Math.min(minOpenTxn, openTxn.getTxnId());
      }
      if (excludeTxnTypes.contains(openTxn.getType())) {
        continue;
      }
      openList.add(openTxn.getTxnId());
      if (openTxn.getStatus() == ABORTED) {
        abortedBits.set(openList.size() - 1);
      }
    }
    ByteBuffer byteBuffer = ByteBuffer.wrap(abortedBits.toByteArray());
    GetOpenTxnsResponse otr = new GetOpenTxnsResponse(getHwm(), openList, byteBuffer);
    if (minOpenTxn < Long.MAX_VALUE) {
      otr.setMin_open_txn(minOpenTxn);
    }
    return otr;
  }
}
