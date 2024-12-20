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

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.DataConnector;

public class PreAlterDataConnectorEvent extends PreEventContext {

  private final DataConnector oldDC, newDC;

  public PreAlterDataConnectorEvent(DataConnector oldDC, DataConnector newDC, IHMSHandler handler) {
    super(PreEventType.ALTER_DATACONNECTOR, handler);
    this.oldDC = oldDC;
    this.newDC = newDC;
  }

  /**
   * @return the old connector
   */
  public DataConnector getOldDataConnector() {
    return oldDC;
  }

  /**
   * @return the new connector
   */
  public DataConnector getNewDataConnector() {
    return newDC;
  }
}
