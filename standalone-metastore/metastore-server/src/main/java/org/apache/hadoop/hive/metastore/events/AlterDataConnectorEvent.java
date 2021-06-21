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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.DataConnector;

/**
 * AlterDataConnectorEvent.
 * Event which is captured during connector alters for owner info or properties or URI
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AlterDataConnectorEvent extends ListenerEvent {
  private final DataConnector oldDc;
  private final DataConnector newDc;

  public AlterDataConnectorEvent(DataConnector oldDc, DataConnector newDc, boolean status, IHMSHandler handler,
      boolean isReplicated) {
    super(status, handler);
    this.oldDc = oldDc;
    this.newDc = newDc;
  }

  /**
   * @return the old db
   */
  public DataConnector getOldDataConnector() {
    return oldDc;
  }

  /**
   * @return the new db
   */
  public DataConnector getNewDataConnector() {
    return newDc;
  }

}
