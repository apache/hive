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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.api.repl;

import org.apache.hive.hcatalog.api.HCatNotificationEvent;

/**
 * ErroredReplicationTask is a special kind of NoopReplicationTask in that it
 * is not actionable, and wraps an error that might have occurred during Task
 * instantiation time. This is used to protect "future" events that we know
 * nothing about from breaking the system by throwing IllegalStateExceptions.
 *
 * Whether or not the user intends to do something with these tasks and act
 * upon the exceptions is left to the user to determine how they can best use them.
 *
 */
public class ErroredReplicationTask extends NoopReplicationTask {

  RuntimeException errorCause = null;

  public ErroredReplicationTask(HCatNotificationEvent event, RuntimeException e) {
    super(event);
    this.errorCause = e;
  }

  public RuntimeException getCause(){
    return this.errorCause;
  }

  @Override
  public boolean isActionable(){
    return false;
  }

}
