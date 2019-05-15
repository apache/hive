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
package org.apache.hive.jdbc.logs;

import org.apache.hive.service.rpc.thrift.TProgressUpdateResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface InPlaceUpdateStream {
  void update(TProgressUpdateResp response);

  InPlaceUpdateStream NO_OP = new InPlaceUpdateStream() {
    private final EventNotifier eventNotifier = new EventNotifier();
    @Override
    public void update(TProgressUpdateResp response) {

    }

    @Override
    public EventNotifier getEventNotifier() {
      return eventNotifier;
    }

  };

  EventNotifier getEventNotifier();

  class EventNotifier {
    public static final Logger LOG = LoggerFactory.getLogger(EventNotifier.class.getName());
    boolean isComplete = false;
    boolean isOperationLogUpdatedOnceAtLeast = false;

    public synchronized void progressBarCompleted() {
      LOG.debug("progress bar is complete");
      this.isComplete = true;
    }

    private synchronized boolean isProgressBarComplete() {
      return isComplete;

    }

    public synchronized void operationLogShowedToUser() {
      LOG.debug("operations log is shown to the user");
      isOperationLogUpdatedOnceAtLeast = true;
    }

    public synchronized boolean isOperationLogUpdatedAtLeastOnce() {
      return isOperationLogUpdatedOnceAtLeast;
    }

    public boolean canOutputOperationLogs() {
      return !isOperationLogUpdatedAtLeastOnce() || isProgressBarComplete();
    }
  }
}
