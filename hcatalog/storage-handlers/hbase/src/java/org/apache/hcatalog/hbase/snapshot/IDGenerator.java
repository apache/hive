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

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hcatalog.hbase.snapshot.lock.LockListener;
import org.apache.hcatalog.hbase.snapshot.lock.WriteLock;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class generates revision id's for transactions.
 */
class IDGenerator implements LockListener {

  private ZooKeeper zookeeper;
  private String zNodeDataLoc;
  private String zNodeLockBasePath;
  private long id;
  private static final Logger LOG = LoggerFactory.getLogger(IDGenerator.class);

  IDGenerator(ZooKeeper zookeeper, String tableName, String idGenNode)
    throws IOException {
    this.zookeeper = zookeeper;
    this.zNodeDataLoc = idGenNode;
    this.zNodeLockBasePath = PathUtil.getLockManagementNode(idGenNode);
  }

  /**
   * This method obtains a revision id for a transaction.
   *
   * @return revision ID
   * @throws IOException
   */
  public long obtainID() throws IOException {
    WriteLock wLock = new WriteLock(zookeeper, zNodeLockBasePath, Ids.OPEN_ACL_UNSAFE);
    wLock.setLockListener(this);
    try {
      boolean lockGrabbed = wLock.lock();
      if (lockGrabbed == false) {
        //TO DO : Let this request queue up and try obtaining lock.
        throw new IOException("Unable to obtain lock to obtain id.");
      } else {
        id = incrementAndReadCounter();
      }
    } catch (KeeperException e) {
      LOG.warn("Exception while obtaining lock for ID.", e);
      throw new IOException("Exception while obtaining lock for ID.", e);
    } catch (InterruptedException e) {
      LOG.warn("Exception while obtaining lock for ID.", e);
      throw new IOException("Exception while obtaining lock for ID.", e);
    } finally {
      wLock.unlock();
    }
    return id;
  }

  /**
   * This method reads the latest revision ID that has been used. The ID
   * returned by this method cannot be used for transaction.
   * @return revision ID
   * @throws IOException
   */
  public long readID() throws IOException {
    long curId;
    try {
      Stat stat = new Stat();
      byte[] data = zookeeper.getData(this.zNodeDataLoc, false, stat);
      curId = Long.parseLong(new String(data, Charset.forName("UTF-8")));
    } catch (KeeperException e) {
      LOG.warn("Exception while reading current revision id.", e);
      throw new IOException("Exception while reading current revision id.", e);
    } catch (InterruptedException e) {
      LOG.warn("Exception while reading current revision id.", e);
      throw new IOException("Exception while reading current revision id.", e);
    }

    return curId;
  }


  private long incrementAndReadCounter() throws IOException {

    long curId, usedId;
    try {
      Stat stat = new Stat();
      byte[] data = zookeeper.getData(this.zNodeDataLoc, false, stat);
      usedId = Long.parseLong((new String(data, Charset.forName("UTF-8"))));
      curId = usedId + 1;
      String lastUsedID = String.valueOf(curId);
      zookeeper.setData(this.zNodeDataLoc, lastUsedID.getBytes(Charset.forName("UTF-8")), -1);

    } catch (KeeperException e) {
      LOG.warn("Exception while incrementing revision id.", e);
      throw new IOException("Exception while incrementing revision id. ", e);
    } catch (InterruptedException e) {
      LOG.warn("Exception while incrementing revision id.", e);
      throw new IOException("Exception while incrementing revision id. ", e);
    }

    return curId;
  }

  /*
   * @see org.apache.hcatalog.hbase.snapshot.lock.LockListener#lockAcquired()
   */
  @Override
  public void lockAcquired() {


  }

  /*
   * @see org.apache.hcatalog.hbase.snapshot.lock.LockListener#lockReleased()
   */
  @Override
  public void lockReleased() {

  }


}
