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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class IDGenClient extends Thread {

  String connectionStr;
  String base_dir;
  ZKUtil zkutil;
  Random sleepTime = new Random();
  int runtime;
  HashMap<Long, Long> idMap;
  String tableName;

  IDGenClient(String connectionStr, String base_dir, int time, String tableName) {
    super();
    this.connectionStr = connectionStr;
    this.base_dir = base_dir;
    this.zkutil = new ZKUtil(connectionStr, base_dir);
    this.runtime = time;
    idMap = new HashMap<Long, Long>();
    this.tableName = tableName;
  }

  /*
   * @see java.lang.Runnable#run()
   */
  @Override
  public void run() {
    long startTime = System.currentTimeMillis();
    int timeElapsed = 0;
    while( timeElapsed <= runtime){
      try {
        long id = zkutil.nextId(tableName);
        idMap.put(System.currentTimeMillis(), id);

        int sTime = sleepTime.nextInt(2);
        Thread.sleep(sTime * 100);
      } catch (Exception e) {
        e.printStackTrace();
      }

      timeElapsed = (int) Math.ceil((System.currentTimeMillis() - startTime)/(double)1000);
    }

  }

  Map<Long, Long> getIdMap(){
    return idMap;
  }

}
