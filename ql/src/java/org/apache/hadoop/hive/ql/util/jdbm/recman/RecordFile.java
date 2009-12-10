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
 * $Id: RecordFile.java,v 1.6 2005/06/25 23:12:32 doomdark Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.recman;

import java.io.*;
import java.util.*;

/**
 *  This class represents a random access file as a set of fixed size
 *  records. Each record has a physical record number, and records are
 *  cached in order to improve access.
 *<p>
 *  The set of dirty records on the in-use list constitutes a transaction.
 *  Later on, we will send these records to some recovery thingy.
 */
public final class RecordFile {
    final TransactionManager txnMgr;

    // Todo: reorganize in hashes and fifos as necessary.
    // free -> inUse -> dirty -> inTxn -> free
    // free is a cache, thus a FIFO. The rest are hashes.
    private final LinkedList free = new LinkedList();
    private final HashMap inUse = new HashMap();
    private final HashMap dirty = new HashMap();
    private final HashMap inTxn = new HashMap();

    // transactions disabled?
    private boolean transactionsDisabled = false;

    /** The length of a single block. */
    public final static int BLOCK_SIZE = 8192;//4096;

    /** The extension of a record file */
    final static String extension = ".db";

    /** A block of clean data to wipe clean pages. */
    final static byte[] cleanData = new byte[BLOCK_SIZE];

    private RandomAccessFile file;
    private final String fileName;

    /**
     *  Creates a new object on the indicated filename. The file is
     *  opened in read/write mode.
     *
     *  @param fileName the name of the file to open or create, without
     *         an extension.
     *  @throws IOException whenever the creation of the underlying
     *          RandomAccessFile throws it.
     */
    RecordFile(String fileName) throws IOException {
        this.fileName = fileName;
        file = new RandomAccessFile(fileName + extension, "rw");
        txnMgr = new TransactionManager(this);
    }
    
    /**
     *  Creates a new object on the indicated filename. The file is
     *  opened in read/write mode.
     *
     *  @param fileName the name of the file to open or create, without
     *         an extension.
     *  @throws IOException whenever the creation of the underlying
     *          RandomAccessFile throws it.
     */
    RecordFile(File file) throws IOException {
        this.fileName = file.getName();
        this.file = new RandomAccessFile(file, "rw");
        txnMgr = new TransactionManager(this);
    }

    /**
     *  Returns the file name.
     */
    String getFileName() {
        return fileName;
    }

    /**
     *  Disables transactions: doesn't sync and doesn't use the
     *  transaction manager.
     */
    void disableTransactions() {
        transactionsDisabled = true;
    }

    /**
     *  Gets a block from the file. The returned byte array is
     *  the in-memory copy of the record, and thus can be written
     *  (and subsequently released with a dirty flag in order to
     *  write the block back).
     *
     *  @param blockid The record number to retrieve.
     */
     BlockIo get(long blockid) throws IOException {
         Long key = new Long(blockid);

         // try in transaction list, dirty list, free list
         BlockIo node = (BlockIo) inTxn.get(key);
         if (node != null) {
             inTxn.remove(key);
             inUse.put(key, node);
             return node;
         }
         node = (BlockIo) dirty.get(key);
         if (node != null) {
             dirty.remove(key);
             inUse.put(key, node);
             return node;
         }
         for (Iterator i = free.iterator(); i.hasNext(); ) {
             BlockIo cur = (BlockIo) i.next();
             if (cur.getBlockId() == blockid) {
                 node = cur;
                 i.remove();
                 inUse.put(key, node);
                 return node;
             }
         }

         // sanity check: can't be on in use list
         if (inUse.get(key) != null) {
             throw new Error("double get for block " + blockid);
         }

         // get a new node and read it from the file
         node = getNewNode(blockid);
         long offset = blockid * BLOCK_SIZE;
         if (file.length() > 0 && offset <= file.length()) {
             read(file, offset, node.getData(), BLOCK_SIZE);
         } else {
             System.arraycopy(cleanData, 0, node.getData(), 0, BLOCK_SIZE);
         }
         inUse.put(key, node);
         node.setClean();
         return node;
     }


    /**
     *  Releases a block.
     *
     *  @param blockid The record number to release.
     *  @param isDirty If true, the block was modified since the get().
     */
    void release(long blockid, boolean isDirty)
    throws IOException {
        BlockIo node = (BlockIo) inUse.get(new Long(blockid));
        if (node == null)
            throw new IOException("bad blockid " + blockid + " on release");
        if (!node.isDirty() && isDirty)
            node.setDirty();
        release(node);
    }

    /**
     *  Releases a block.
     *
     *  @param block The block to release.
     */
    void release(BlockIo block) {
        Long key = new Long(block.getBlockId());
        inUse.remove(key);
        if (block.isDirty()) {
            // System.out.println( "Dirty: " + key + block );
            dirty.put(key, block);
        } else {
            if (!transactionsDisabled && block.isInTransaction()) {
                inTxn.put(key, block);
            } else {
                free.add(block);
            }
        }
    }

    /**
     *  Discards a block (will not write the block even if it's dirty)
     *
     *  @param block The block to discard.
     */
    void discard(BlockIo block) {
        Long key = new Long(block.getBlockId());
        inUse.remove(key);

        // note: block not added to free list on purpose, because
        //       it's considered invalid
    }

    /**
     *  Commits the current transaction by flushing all dirty buffers
     *  to disk.
     */
    void commit() throws IOException {
        // debugging...
        if (!inUse.isEmpty() && inUse.size() > 1) {
            showList(inUse.values().iterator());
            throw new Error("in use list not empty at commit time ("
                            + inUse.size() + ")");
        }

        //  System.out.println("committing...");

        if ( dirty.size() == 0 ) {
            // if no dirty blocks, skip commit process
            return;
        }

        if (!transactionsDisabled) {
            txnMgr.start();
        }

        for (Iterator i = dirty.values().iterator(); i.hasNext(); ) {
            BlockIo node = (BlockIo) i.next();
            i.remove();
            // System.out.println("node " + node + " map size now " + dirty.size());
            if (transactionsDisabled) {
                long offset = node.getBlockId() * BLOCK_SIZE;
                file.seek(offset);
                file.write(node.getData());
                node.setClean();
                free.add(node);
            }
            else {
                txnMgr.add(node);
                inTxn.put(new Long(node.getBlockId()), node);
            }
        }
        if (!transactionsDisabled) {
            txnMgr.commit();
        }
    }

    /**
     *  Rollback the current transaction by discarding all dirty buffers
     */
    void rollback() throws IOException {
        // debugging...
        if (!inUse.isEmpty()) {
            showList(inUse.values().iterator());
            throw new Error("in use list not empty at rollback time ("
                            + inUse.size() + ")");
        }
        //  System.out.println("rollback...");
        dirty.clear();

        txnMgr.synchronizeLogFromDisk();

        if (!inTxn.isEmpty()) {
            showList(inTxn.values().iterator());
            throw new Error("in txn list not empty at rollback time ("
                            + inTxn.size() + ")");
        };
    }

    /**
     *  Commits and closes file.
     */
    void close() throws IOException {
        if (!dirty.isEmpty()) {
            commit();
        }
        txnMgr.shutdown();
        if ( transactionsDisabled ) {
          txnMgr.removeLogFile();
        }

        if (!inTxn.isEmpty()) {
            showList(inTxn.values().iterator());
            throw new Error("In transaction not empty");
        }

        // these actually ain't that bad in a production release
        if (!dirty.isEmpty()) {
            System.out.println("ERROR: dirty blocks at close time");
            showList(dirty.values().iterator());
            throw new Error("Dirty blocks at close time");
        }
        if (!inUse.isEmpty()) {
            System.out.println("ERROR: inUse blocks at close time");
            showList(inUse.values().iterator());
            throw new Error("inUse blocks at close time");
        }

        // debugging stuff to keep an eye on the free list
        // System.out.println("Free list size:" + free.size());
        file.close();
        file = null;
    }


    /**
     * Force closing the file and underlying transaction manager.
     * Used for testing purposed only.
     */
    void forceClose() throws IOException {
      txnMgr.forceClose();
      file.close();
    }

    /**
     *  Prints contents of a list
     */
    private void showList(Iterator i) {
        int cnt = 0;
        while (i.hasNext()) {
            System.out.println("elem " + cnt + ": " + i.next());
            cnt++;
        }
    }


    /**
     *  Returns a new node. The node is retrieved (and removed)
     *  from the released list or created new.
     */
    private BlockIo getNewNode(long blockid)
    throws IOException {

        BlockIo retval = null;
        if (!free.isEmpty()) {
            retval = (BlockIo) free.removeFirst();
        }
        if (retval == null)
            retval = new BlockIo(0, new byte[BLOCK_SIZE]);

        retval.setBlockId(blockid);
        retval.setView(null);
        return retval;
    }

    /**
     *  Synchs a node to disk. This is called by the transaction manager's
     *  synchronization code.
     */
    void synch(BlockIo node) throws IOException {
        byte[] data = node.getData();
        if (data != null) {
            long offset = node.getBlockId() * BLOCK_SIZE;
            file.seek(offset);
            file.write(data);
        }
    }

    /**
     *  Releases a node from the transaction list, if it was sitting
     *  there.
     *
     *  @param recycle true if block data can be reused
     */
    void releaseFromTransaction(BlockIo node, boolean recycle)
    throws IOException {
        Long key = new Long(node.getBlockId());
        if ((inTxn.remove(key) != null) && recycle) {
            free.add(node);
        }
    }

    /**
     *  Synchronizes the file.
     */
    void sync() throws IOException {
        file.getFD().sync();
    }


    /**
     * Utility method: Read a block from a RandomAccessFile
     */
    private static void read(RandomAccessFile file, long offset,
                             byte[] buffer, int nBytes) throws IOException {
        file.seek(offset);
        int remaining = nBytes;
        int pos = 0;
        while (remaining > 0) {
            int read = file.read(buffer, pos, remaining);
            if (read == -1) {
                System.arraycopy(cleanData, 0, buffer, pos, remaining);
                break;
            }
            remaining -= read;
            pos += read;
        }
    }

}
