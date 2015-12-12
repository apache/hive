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
package org.apache.hive.hcatalog.streaming.mutate.client.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides a means to handle the situation when a held lock fails. */
public interface LockFailureListener {

  static final Logger LOG = LoggerFactory.getLogger(LockFailureListener.class);

  static final LockFailureListener NULL_LISTENER = new LockFailureListener() {
    @Override
    public void lockFailed(long lockId, Long transactionId, Iterable<String> tableNames, Throwable t) {
      LOG.warn(
          "Ignored lock failure: lockId=" + lockId + ", transactionId=" + transactionId + ", tables=" + tableNames, t);
    }
    
    public String toString() {
      return LockFailureListener.class.getName() + ".NULL_LISTENER";
    }
  };

  /** Called when the specified lock has failed. You should probably abort your job in this case. */
  void lockFailed(long lockId, Long transactionId, Iterable<String> tableNames, Throwable t);

}
