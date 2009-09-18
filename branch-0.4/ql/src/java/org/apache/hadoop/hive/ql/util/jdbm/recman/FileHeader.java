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
 * $Id: FileHeader.java,v 1.3 2005/06/25 23:12:32 doomdark Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.recman;

/**
 *  This class represents a file header. It is a 1:1 representation of
 *  the data that appears in block 0 of a file.
 */
class FileHeader implements BlockView {
    // offsets
    private static final short O_MAGIC = 0; // short magic
    private static final short O_LISTS = Magic.SZ_SHORT; // long[2*NLISTS]
    private static final int O_ROOTS = 
        O_LISTS + (Magic.NLISTS * 2 * Magic.SZ_LONG);

    // my block
    private BlockIo block;

    /** The number of "root" rowids available in the file. */
    static final int NROOTS = 
        (RecordFile.BLOCK_SIZE - O_ROOTS) / Magic.SZ_LONG;

    /**
     *  Constructs a FileHeader object from a block.
     *
     *  @param block The block that contains the file header
     *  @param isNew If true, the file header is for a new file.
     *  @throws IOException if the block is too short to keep the file
     *          header.
     */
    FileHeader(BlockIo block, boolean isNew) {
        this.block = block;
        if (isNew)
            block.writeShort(O_MAGIC, Magic.FILE_HEADER);
        else if (!magicOk())
            throw new Error("CRITICAL: file header magic not OK " 
                            + block.readShort(O_MAGIC));
    }

    /** Returns true if the magic corresponds with the fileHeader magic.  */
    private boolean magicOk() {
        return block.readShort(O_MAGIC) == Magic.FILE_HEADER;
    }


    /** Returns the offset of the "first" block of the indicated list */
    private short offsetOfFirst(int list) {
        return (short) (O_LISTS + (2 * Magic.SZ_LONG * list));
    }

    /** Returns the offset of the "last" block of the indicated list */
    private short offsetOfLast(int list) {
        return (short) (offsetOfFirst(list) + Magic.SZ_LONG);
    }

    /** Returns the offset of the indicated root */
    private short offsetOfRoot(int root) {
        return (short) (O_ROOTS + (root * Magic.SZ_LONG));
    }

    /**
     *  Returns the first block of the indicated list
     */
    long getFirstOf(int list) {
        return block.readLong(offsetOfFirst(list));
    }
    
    /**
     *  Sets the first block of the indicated list
     */
    void setFirstOf(int list, long value) {
        block.writeLong(offsetOfFirst(list), value);
    }
    
    /**
     *  Returns the last block of the indicated list
     */
    long getLastOf(int list) {
        return block.readLong(offsetOfLast(list));
    }
    
    /**
     *  Sets the last block of the indicated list
     */
    void setLastOf(int list, long value) {
        block.writeLong(offsetOfLast(list), value);
    }

    /**
     *  Returns the indicated root rowid. A root rowid is a special rowid
     *  that needs to be kept between sessions. It could conceivably be
     *  stored in a special file, but as a large amount of space in the
     *  block header is wasted anyway, it's more useful to store it where
     *  it belongs.
     *
     *  @see #NROOTS
     */
    long getRoot(int root) {
        return block.readLong(offsetOfRoot(root));
    }

    /**
     *  Sets the indicated root rowid.
     *
     *  @see #getRoot
     *  @see #NROOTS
     */
    void setRoot(int root, long rowid) {
        block.writeLong(offsetOfRoot(root), rowid);
    }
}
