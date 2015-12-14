/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.worker;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that the sequence of {@link RecordIdentifier RecordIdentifiers} are in a valid order for insertion into an
 * ACID delta file in a given partition and bucket.
 */
class SequenceValidator {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceValidator.class);

  private Long lastTxId;
  private Long lastRowId;

  SequenceValidator() {
  }

  boolean isInSequence(RecordIdentifier recordIdentifier) {
    if (lastTxId != null && recordIdentifier.getTransactionId() < lastTxId) {
      LOG.debug("Non-sequential transaction ID. Expected >{}, recordIdentifier={}", lastTxId, recordIdentifier);
      return false;
    } else if (lastTxId != null && recordIdentifier.getTransactionId() == lastTxId && lastRowId != null
        && recordIdentifier.getRowId() <= lastRowId) {
      LOG.debug("Non-sequential row ID. Expected >{}, recordIdentifier={}", lastRowId, recordIdentifier);
      return false;
    }
    lastTxId = recordIdentifier.getTransactionId();
    lastRowId = recordIdentifier.getRowId();
    return true;
  }

  /**
   * Validator must be reset for each new partition and or bucket.
   */
  void reset() {
    lastTxId = null;
    lastRowId = null;
    LOG.debug("reset");
  }

  @Override
  public String toString() {
    return "SequenceValidator [lastTxId=" + lastTxId + ", lastRowId=" + lastRowId + "]";
  }

}
