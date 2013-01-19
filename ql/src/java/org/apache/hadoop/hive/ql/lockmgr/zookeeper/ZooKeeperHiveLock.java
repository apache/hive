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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.lockmgr.zookeeper;

import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;

public class ZooKeeperHiveLock extends HiveLock {
  private String         path;
  private HiveLockObject obj;
  private HiveLockMode   mode;

  public ZooKeeperHiveLock(String path, HiveLockObject obj, HiveLockMode mode) {
    this.path = path;
    this.obj  = obj;
    this.mode = mode;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public HiveLockObject getHiveLockObject() {
    return obj;
  }

  public void setHiveLockObject(HiveLockObject obj) {
    this.obj = obj;
  }

  @Override
  public HiveLockMode getHiveLockMode() {
    return mode;
  }

  public void setHiveLockMode(HiveLockMode mode) {
    this.mode = mode;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ZooKeeperHiveLock)) {
      return false;
    }

    ZooKeeperHiveLock zLock = (ZooKeeperHiveLock)o;

    return path.equals(zLock.getPath()) &&
      obj.equals(zLock.getHiveLockObject()) &&
      mode == zLock.getHiveLockMode();
  }
}
