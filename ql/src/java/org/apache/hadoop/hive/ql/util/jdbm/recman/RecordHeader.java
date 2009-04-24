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
 * $Id: RecordHeader.java,v 1.1 2000/05/06 00:00:31 boisvert Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.recman;

/**
 *  The data that comes at the start of a record of data. It stores 
 *  both the current size and the avaliable size for the record - the latter
 *  can be bigger than the former, which allows the record to grow without
 *  needing to be moved and which allows the system to put small records
 *  in larger free spots.
 */
class RecordHeader {
    // offsets
    private static final short O_CURRENTSIZE = 0; // int currentSize
    private static final short O_AVAILABLESIZE = Magic.SZ_INT; // int availableSize
    static final int SIZE = O_AVAILABLESIZE + Magic.SZ_INT;
    
    // my block and the position within the block
    private BlockIo block;
    private short pos;

    /**
     *  Constructs a record header from the indicated data starting at
     *  the indicated position.
     */
    RecordHeader(BlockIo block, short pos) {
        this.block = block;
        this.pos = pos;
        if (pos > (RecordFile.BLOCK_SIZE - SIZE))
            throw new Error("Offset too large for record header (" 
                            + block.getBlockId() + ":" 
                            + pos + ")");
    }

    /** Returns the current size */
    int getCurrentSize() {
        return block.readInt(pos + O_CURRENTSIZE);
    }
    
    /** Sets the current size */
    void setCurrentSize(int value) {
        block.writeInt(pos + O_CURRENTSIZE, value);
    }
    
    /** Returns the available size */
    int getAvailableSize() {
        return block.readInt(pos + O_AVAILABLESIZE);
    }
    
    /** Sets the available size */
    void setAvailableSize(int value) {
        block.writeInt(pos + O_AVAILABLESIZE, value);
    }

    // overrides java.lang.Object
    public String toString() {
        return "RH(" + block.getBlockId() + ":" + pos 
            + ", avl=" + getAvailableSize()
            + ", cur=" + getCurrentSize() 
            + ")";
    }
}
