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
package org.apache.hadoop.hive.llap.shufflehandler;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;

class IndexCache {

  private final Configuration conf;
  private final int totalMemoryAllowed;
  private AtomicInteger totalMemoryUsed = new AtomicInteger();
  private static final Logger LOG = LoggerFactory.getLogger(IndexCache.class);

  private final ConcurrentHashMap<String,IndexInformation> cache =
      new ConcurrentHashMap<String,IndexInformation>();

  private final LinkedBlockingQueue<String> queue =
      new LinkedBlockingQueue<String>();

  public IndexCache(Configuration conf) {
    this.conf = conf;
    totalMemoryAllowed = 10 * 1024 * 1024;
    LOG.info("IndexCache created with max memory = " + totalMemoryAllowed);
  }

  /**
   * This method gets the index information for the given mapId and reduce.
   * It reads the index file into cache if it is not already present.
   * @param mapId
   * @param reduce
   * @param fileName The file to read the index information from if it is not
   *                 already present in the cache
   * @param expectedIndexOwner The expected owner of the index file
   * @return The Index Information
   * @throws IOException
   */
  public TezIndexRecord getIndexInformation(String mapId, int reduce,
                                         Path fileName, String expectedIndexOwner)
      throws IOException {

    IndexInformation info = cache.get(mapId);

    if (info == null) {
      info = readIndexFileToCache(fileName, mapId, expectedIndexOwner);
    } else {
      synchronized(info) {
        while (isUnderConstruction(info)) {
          try {
            info.wait();
          } catch (InterruptedException e) {
            throw new IOException("Interrupted waiting for construction", e);
          }
        }
      }
      LOG.debug("IndexCache HIT: MapId " + mapId + " found");
    }

    if (info.mapSpillRecord.size() == 0 ||
        info.mapSpillRecord.size() <= reduce) {
      throw new IOException("Invalid request " +
          " Map Id = " + mapId + " Reducer = " + reduce +
          " Index Info Length = " + info.mapSpillRecord.size());
    }
    return info.mapSpillRecord.getIndex(reduce);
  }

  private boolean isUnderConstruction(IndexInformation info) {
    synchronized(info) {
      return (null == info.mapSpillRecord);
    }
  }

  private IndexInformation readIndexFileToCache(Path indexFileName,
                                                String mapId,
                                                String expectedIndexOwner)
      throws IOException {
    IndexInformation info;
    IndexInformation newInd = new IndexInformation();
    if ((info = cache.putIfAbsent(mapId, newInd)) != null) {
      synchronized(info) {
        while (isUnderConstruction(info)) {
          try {
            info.wait();
          } catch (InterruptedException e) {
            throw new IOException("Interrupted waiting for construction", e);
          }
        }
      }
      LOG.debug("IndexCache HIT: MapId " + mapId + " found");
      return info;
    }
    LOG.debug("IndexCache MISS: MapId " + mapId + " not found") ;
    TezSpillRecord tmp = null;
    try {
      tmp = new TezSpillRecord(indexFileName, conf, expectedIndexOwner);
    } catch (Throwable e) {
      tmp = new TezSpillRecord(0);
      cache.remove(mapId);
      throw new IOException("Error Reading IndexFile", e);
    } finally {
      synchronized (newInd) {
        newInd.mapSpillRecord = tmp;
        newInd.notifyAll();
      }
    }
    queue.add(mapId);

    if (totalMemoryUsed.addAndGet(newInd.getSize()) > totalMemoryAllowed) {
      freeIndexInformation();
    }
    return newInd;
  }

  /**
   * This method removes the map from the cache if index information for this
   * map is loaded(size>0), index information entry in cache will not be 
   * removed if it is in the loading phrase(size=0), this prevents corruption  
   * of totalMemoryUsed. It should be called when a map output on this tracker 
   * is discarded.
   * @param mapId The taskID of this map.
   */
  public void removeMap(String mapId) {
    IndexInformation info = cache.get(mapId);
    if (info == null || ((info != null) && isUnderConstruction(info))) {
      return;
    }
    info = cache.remove(mapId);
    if (info != null) {
      totalMemoryUsed.addAndGet(-info.getSize());
      if (!queue.remove(mapId)) {
        LOG.warn("Map ID" + mapId + " not found in queue!!");
      }
    } else {
      LOG.info("Map ID " + mapId + " not found in cache");
    }
  }

  /**
   * This method checks if cache and totolMemoryUsed is consistent.
   * It is only used for unit test.
   * @return True if cache and totolMemoryUsed is consistent
   */
  boolean checkTotalMemoryUsed() {
    int totalSize = 0;
    for (IndexInformation info : cache.values()) {
      totalSize += info.getSize();
    }
    return totalSize == totalMemoryUsed.get();
  }

  /**
   * Bring memory usage below totalMemoryAllowed.
   */
  private synchronized void freeIndexInformation() {
    while (totalMemoryUsed.get() > totalMemoryAllowed) {
      String s = queue.remove();
      IndexInformation info = cache.remove(s);
      if (info != null) {
        totalMemoryUsed.addAndGet(-info.getSize());
      }
    }
  }

  private static class IndexInformation {
    TezSpillRecord mapSpillRecord;

    int getSize() {
      return mapSpillRecord == null
          ? 0
          : mapSpillRecord.size() * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH;
    }
  }
}
