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

import java.util.TimerTask;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.RawStore;

public class EventCleanerTask extends TimerTask{

  public static final Logger LOG = LoggerFactory.getLogger(EventCleanerTask.class);
  private final IHMSHandler handler;

  public EventCleanerTask(IHMSHandler handler) {
    super();
    this.handler = handler;
  }

  @Override
  public void run() {

    try {
      RawStore ms = handler.getMS();
      long deleteCnt = ms.cleanupEvents();

      if (deleteCnt > 0L){
        LOG.info("Number of events deleted from event Table: "+deleteCnt);
      }
    } catch (Exception e) {
      LOG.error("Exception while trying to delete events ", e);
    }
  }
}
