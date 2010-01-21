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

/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: TransactionManager.java,v 1.7 2005/06/25 23:12:32 doomdark Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.recman;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * This class manages the transaction log that belongs to every
 * {@link RecordFile}. The transaction log is either clean, or in progress. In
 * the latter case, the transaction manager takes care of a roll forward.
 *<p>
 * Implementation note: this is a proof-of-concept implementation which hasn't
 * been optimized for speed. For instance, all sorts of streams are created for
 * every transaction.
 */
// TODO: Handle the case where we are recovering lg9 and lg0, were we
// should start with lg9 instead of lg0!

public final class TransactionManager {
  private final RecordFile owner;

  // streams for transaction log.
  private FileOutputStream fos;
  private ObjectOutputStream oos;

  /**
   * By default, we keep 10 transactions in the log file before synchronizing it
   * with the main database file.
   */
  static final int DEFAULT_TXNS_IN_LOG = 10;

  /**
   * Maximum number of transactions before the log file is synchronized with the
   * main database file.
   */
  private int _maxTxns = DEFAULT_TXNS_IN_LOG;

  /**
   * In-core copy of transactions. We could read everything back from the log
   * file, but the RecordFile needs to keep the dirty blocks in core anyway, so
   * we might as well point to them and spare us a lot of hassle.
   */
  private ArrayList[] txns = new ArrayList[DEFAULT_TXNS_IN_LOG];
  private int curTxn = -1;

  /** Extension of a log file. */
  static final String extension = ".lg";

  /** log file name */
  private String logFileName;

  /**
   * Instantiates a transaction manager instance. If recovery needs to be
   * performed, it is done.
   * 
   * @param owner
   *          the RecordFile instance that owns this transaction mgr.
   */
  TransactionManager(RecordFile owner) throws IOException {
    this.owner = owner;
    logFileName = null;
    recover();
    open();
  }

  /**
   * Synchronize log file data with the main database file.
   * <p>
   * After this call, the main database file is guaranteed to be consistent and
   * guaranteed to be the only file needed for backup purposes.
   */
  public void synchronizeLog() throws IOException {
    synchronizeLogFromMemory();
  }

  /**
   * Set the maximum number of transactions to record in the log (and keep in
   * memory) before the log is synchronized with the main database file.
   * <p>
   * This method must be called while there are no pending transactions in the
   * log.
   */
  public void setMaximumTransactionsInLog(int maxTxns) throws IOException {
    if (maxTxns <= 0) {
      throw new IllegalArgumentException(
          "Argument 'maxTxns' must be greater than 0.");
    }
    if (curTxn != -1) {
      throw new IllegalStateException(
          "Cannot change setting while transactions are pending in the log");
    }
    _maxTxns = maxTxns;
    txns = new ArrayList[maxTxns];
  }

  /** Builds logfile name */
  private String makeLogName() {
    return owner.getFileName() + extension;
  }

  /** Synchs in-core transactions to data file and opens a fresh log */
  private void synchronizeLogFromMemory() throws IOException {
    close();

    TreeSet blockList = new TreeSet(new BlockIoComparator());

    int numBlocks = 0;
    int writtenBlocks = 0;
    for (int i = 0; i < _maxTxns; i++) {
      if (txns[i] == null) {
        continue;
      }
      // Add each block to the blockList, replacing the old copy of this
      // block if necessary, thus avoiding writing the same block twice
      for (Iterator k = txns[i].iterator(); k.hasNext();) {
        BlockIo block = (BlockIo) k.next();
        if (blockList.contains(block)) {
          block.decrementTransactionCount();
        } else {
          writtenBlocks++;
          blockList.add(block);
        }
        numBlocks++;
      }

      txns[i] = null;
    }
    // Write the blocks from the blockList to disk
    synchronizeBlocks(blockList.iterator(), true);

    owner.sync();
    open();
  }

  /** Opens the log file */
  private void open() throws IOException {
    logFileName = makeLogName();
    fos = new FileOutputStream(logFileName);
    oos = new ObjectOutputStream(fos);
    oos.writeShort(Magic.LOGFILE_HEADER);
    oos.flush();
    curTxn = -1;
  }

  /** Startup recovery on all files */
  private void recover() throws IOException {
    String logName = makeLogName();
    File logFile = new File(logName);
    if (!logFile.exists()) {
      return;
    }
    if (logFile.length() == 0) {
      logFile.delete();
      return;
    }

    FileInputStream fis = new FileInputStream(logFile);
    ObjectInputStream ois = new ObjectInputStream(fis);

    try {
      if (ois.readShort() != Magic.LOGFILE_HEADER) {
        throw new Error("Bad magic on log file");
      }
    } catch (IOException e) {
      // corrupted/empty logfile
      logFile.delete();
      return;
    }

    while (true) {
      ArrayList blocks = null;
      try {
        blocks = (ArrayList) ois.readObject();
      } catch (ClassNotFoundException e) {
        throw new Error("Unexcepted exception: " + e);
      } catch (IOException e) {
        // corrupted logfile, ignore rest of transactions
        break;
      }
      synchronizeBlocks(blocks.iterator(), false);

      // ObjectInputStream must match exactly each
      // ObjectOutputStream created during writes
      try {
        ois = new ObjectInputStream(fis);
      } catch (IOException e) {
        // corrupted logfile, ignore rest of transactions
        break;
      }
    }
    owner.sync();
    logFile.delete();
  }

  /** Synchronizes the indicated blocks with the owner. */
  private void synchronizeBlocks(Iterator blockIterator, boolean fromCore)
      throws IOException {
    // write block vector elements to the data file.
    while (blockIterator.hasNext()) {
      BlockIo cur = (BlockIo) blockIterator.next();
      owner.synch(cur);
      if (fromCore) {
        cur.decrementTransactionCount();
        if (!cur.isInTransaction()) {
          owner.releaseFromTransaction(cur, true);
        }
      }
    }
  }

  /** Set clean flag on the blocks. */
  private void setClean(ArrayList blocks) throws IOException {
    for (Iterator k = blocks.iterator(); k.hasNext();) {
      BlockIo cur = (BlockIo) k.next();
      cur.setClean();
    }
  }

  /** Discards the indicated blocks and notify the owner. */
  private void discardBlocks(ArrayList blocks) throws IOException {
    for (Iterator k = blocks.iterator(); k.hasNext();) {
      BlockIo cur = (BlockIo) k.next();
      cur.decrementTransactionCount();
      if (!cur.isInTransaction()) {
        owner.releaseFromTransaction(cur, false);
      }
    }
  }

  /**
   * Starts a transaction. This can block if all slots have been filled with
   * full transactions, waiting for the synchronization thread to clean out
   * slots.
   */
  void start() throws IOException {
    curTxn++;
    if (curTxn == _maxTxns) {
      synchronizeLogFromMemory();
      curTxn = 0;
    }
    txns[curTxn] = new ArrayList();
  }

  /**
   * Indicates the block is part of the transaction.
   */
  void add(BlockIo block) throws IOException {
    block.incrementTransactionCount();
    txns[curTxn].add(block);
  }

  /**
   * Commits the transaction to the log file.
   */
  void commit() throws IOException {
    oos.writeObject(txns[curTxn]);
    sync();

    // set clean flag to indicate blocks have been written to log
    setClean(txns[curTxn]);

    // open a new ObjectOutputStream in order to store
    // newer states of BlockIo
    oos = new ObjectOutputStream(fos);
  }

  /** Flushes and syncs */
  private void sync() throws IOException {
    oos.flush();
    fos.flush();
    fos.getFD().sync();
  }

  /**
   * Shutdowns the transaction manager. Resynchronizes outstanding logs.
   */
  void shutdown() throws IOException {
    synchronizeLogFromMemory();
    close();
  }

  /**
   * Closes open files.
   */
  private void close() throws IOException {
    sync();
    oos.close();
    fos.close();
    oos = null;
    fos = null;
  }

  public void removeLogFile() {
    // if file is not closed yet, just return
    if (oos != null) {
      return;
    }
    if (logFileName != null) {
      File file = new File(logFileName);
      file.delete();
      logFileName = null;
    }
  }

  /**
   * Force closing the file without synchronizing pending transaction data. Used
   * for testing purposes only.
   */
  void forceClose() throws IOException {
    oos.close();
    fos.close();
    oos = null;
    fos = null;
  }

  /**
   * Use the disk-based transaction log to synchronize the data file.
   * Outstanding memory logs are discarded because they are believed to be
   * inconsistent.
   */
  void synchronizeLogFromDisk() throws IOException {
    close();

    for (int i = 0; i < _maxTxns; i++) {
      if (txns[i] == null) {
        continue;
      }
      discardBlocks(txns[i]);
      txns[i] = null;
    }

    recover();
    open();
  }

  /**
   * INNER CLASS. Comparator class for use by the tree set used to store the
   * blocks to write for this transaction. The BlockIo objects are ordered by
   * their blockIds.
   */
  public static class BlockIoComparator implements Comparator {

    public int compare(Object o1, Object o2) {
      BlockIo block1 = (BlockIo) o1;
      BlockIo block2 = (BlockIo) o2;
      int result = 0;
      if (block1.getBlockId() == block2.getBlockId()) {
        result = 0;
      } else if (block1.getBlockId() < block2.getBlockId()) {
        result = -1;
      } else {
        result = 1;
      }
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }
  } // class BlockIOComparator

}
