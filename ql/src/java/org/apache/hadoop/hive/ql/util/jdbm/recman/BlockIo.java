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
 * $Id: BlockIo.java,v 1.2 2002/08/06 05:18:36 boisvert Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.recman;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This class wraps a page-sized byte array and provides methods to read and
 * write data to and from it. The readers and writers are just the ones that the
 * rest of the toolkit needs, nothing else. Values written are compatible with
 * java.io routines.
 * 
 * @see java.io.DataInput
 * @see java.io.DataOutput
 */
public final class BlockIo implements java.io.Externalizable {

  public final static long serialVersionUID = 2L;

  private long blockId;

  private transient byte[] data; // work area
  private transient BlockView view = null;
  private transient boolean dirty = false;
  private transient int transactionCount = 0;

  /**
   * Default constructor for serialization
   */
  public BlockIo() {
    // empty
  }

  /**
   * Constructs a new BlockIo instance working on the indicated buffer.
   */
  BlockIo(long blockId, byte[] data) {
    // removeme for production version
    if (blockId > 10000000000L) {
      throw new Error("bogus block id " + blockId);
    }
    this.blockId = blockId;
    this.data = data;
  }

  /**
   * Returns the underlying array
   */
  byte[] getData() {
    return data;
  }

  /**
   * Sets the block number. Should only be called by RecordFile.
   */
  void setBlockId(long id) {
    if (isInTransaction()) {
      throw new Error("BlockId assigned for transaction block");
    }
    // removeme for production version
    if (id > 10000000000L) {
      throw new Error("bogus block id " + id);
    }
    blockId = id;
  }

  /**
   * Returns the block number.
   */
  long getBlockId() {
    return blockId;
  }

  /**
   * Returns the current view of the block.
   */
  public BlockView getView() {
    return view;
  }

  /**
   * Sets the current view of the block.
   */
  public void setView(BlockView view) {
    this.view = view;
  }

  /**
   * Sets the dirty flag
   */
  void setDirty() {
    dirty = true;
  }

  /**
   * Clears the dirty flag
   */
  void setClean() {
    dirty = false;
  }

  /**
   * Returns true if the dirty flag is set.
   */
  boolean isDirty() {
    return dirty;
  }

  /**
   * Returns true if the block is still dirty with respect to the transaction
   * log.
   */
  boolean isInTransaction() {
    return transactionCount != 0;
  }

  /**
   * Increments transaction count for this block, to signal that this block is
   * in the log but not yet in the data file. The method also takes a snapshot
   * so that the data may be modified in new transactions.
   */
  synchronized void incrementTransactionCount() {
    transactionCount++;
    // @fixme(alex)
    setClean();
  }

  /**
   * Decrements transaction count for this block, to signal that this block has
   * been written from the log to the data file.
   */
  synchronized void decrementTransactionCount() {
    transactionCount--;
    if (transactionCount < 0) {
      throw new Error("transaction count on block " + getBlockId()
          + " below zero!");
    }

  }

  /**
   * Reads a byte from the indicated position
   */
  public byte readByte(int pos) {
    return data[pos];
  }

  /**
   * Writes a byte to the indicated position
   */
  public void writeByte(int pos, byte value) {
    data[pos] = value;
    setDirty();
  }

  /**
   * Reads a short from the indicated position
   */
  public short readShort(int pos) {
    return (short) (((short) (data[pos + 0] & 0xff) << 8) | ((short) (data[pos + 1] & 0xff) << 0));
  }

  /**
   * Writes a short to the indicated position
   */
  public void writeShort(int pos, short value) {
    data[pos + 0] = (byte) (0xff & (value >> 8));
    data[pos + 1] = (byte) (0xff & (value >> 0));
    setDirty();
  }

  /**
   * Reads an int from the indicated position
   */
  public int readInt(int pos) {
    return (((data[pos + 0] & 0xff) << 24) | ((data[pos + 1] & 0xff) << 16)
        | ((data[pos + 2] & 0xff) << 8) | ((data[pos + 3] & 0xff) << 0));
  }

  /**
   * Writes an int to the indicated position
   */
  public void writeInt(int pos, int value) {
    data[pos + 0] = (byte) (0xff & (value >> 24));
    data[pos + 1] = (byte) (0xff & (value >> 16));
    data[pos + 2] = (byte) (0xff & (value >> 8));
    data[pos + 3] = (byte) (0xff & (value >> 0));
    setDirty();
  }

  /**
   * Reads a long from the indicated position
   */
  public long readLong(int pos) {
    // Contributed by Erwin Bolwidt <ejb@klomp.org>
    // Gives about 15% performance improvement
    return ((long) (((data[pos + 0] & 0xff) << 24)
        | ((data[pos + 1] & 0xff) << 16) | ((data[pos + 2] & 0xff) << 8) | ((data[pos + 3] & 0xff))) << 32)
        | ((long) (((data[pos + 4] & 0xff) << 24)
            | ((data[pos + 5] & 0xff) << 16) | ((data[pos + 6] & 0xff) << 8) | ((data[pos + 7] & 0xff))) & 0xffffffff);
    /*
     * Original version by Alex Boisvert. Might be faster on 64-bit JVMs. return
     * (((long)(data[pos+0] & 0xff) << 56) | ((long)(data[pos+1] & 0xff) << 48)
     * | ((long)(data[pos+2] & 0xff) << 40) | ((long)(data[pos+3] & 0xff) << 32)
     * | ((long)(data[pos+4] & 0xff) << 24) | ((long)(data[pos+5] & 0xff) << 16)
     * | ((long)(data[pos+6] & 0xff) << 8) | ((long)(data[pos+7] & 0xff) << 0));
     */
  }

  /**
   * Writes a long to the indicated position
   */
  public void writeLong(int pos, long value) {
    data[pos + 0] = (byte) (0xff & (value >> 56));
    data[pos + 1] = (byte) (0xff & (value >> 48));
    data[pos + 2] = (byte) (0xff & (value >> 40));
    data[pos + 3] = (byte) (0xff & (value >> 32));
    data[pos + 4] = (byte) (0xff & (value >> 24));
    data[pos + 5] = (byte) (0xff & (value >> 16));
    data[pos + 6] = (byte) (0xff & (value >> 8));
    data[pos + 7] = (byte) (0xff & (value >> 0));
    setDirty();
  }

  // overrides java.lang.Object

  @Override
  public String toString() {
    return "BlockIO(" + blockId + "," + dirty + "," + view + ")";
  }

  // implement externalizable interface
  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    blockId = in.readLong();
    int length = in.readInt();
    data = new byte[length];
    in.readFully(data);
  }

  // implement externalizable interface
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeInt(data.length);
    out.write(data);
  }

}
