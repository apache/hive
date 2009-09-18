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
 * $Id: PageManager.java,v 1.3 2005/06/25 23:12:32 doomdark Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.recman;

import java.io.*;

/**
 *  This class manages the linked lists of pages that make up a file.
 */
final class PageManager {
    // our record file
    private RecordFile file;
    // header data
    private FileHeader header;
    private BlockIo headerBuf;
    
    /**
     *  Creates a new page manager using the indicated record file.
     */
    PageManager(RecordFile file) throws IOException {
        this.file = file;
        
        // check the file header. If the magic is 0, we assume a new
        // file. Note that we hold on to the file header node.
        headerBuf = file.get(0);
        if (headerBuf.readShort(0) == 0)
            header = new FileHeader(headerBuf, true);
        else
            header = new FileHeader(headerBuf, false);
    }
    
    /**
     *  Allocates a page of the indicated type. Returns recid of the
     *  page.
     */
    long allocate(short type) throws IOException {
        
        if (type == Magic.FREE_PAGE)
            throw new Error("allocate of free page?");
        
        // do we have something on the free list?
        long retval = header.getFirstOf(Magic.FREE_PAGE);
        boolean isNew = false;
        if (retval != 0) {
            // yes. Point to it and make the next of that page the
            // new first free page.
            header.setFirstOf(Magic.FREE_PAGE, getNext(retval));
        }
        else {
            // nope. make a new record
            retval = header.getLastOf(Magic.FREE_PAGE);
            if (retval == 0)
                // very new file - allocate record #1
                retval = 1;
            header.setLastOf(Magic.FREE_PAGE, retval + 1);
            isNew = true;
        }
        
        // Cool. We have a record, add it to the correct list
        BlockIo buf = file.get(retval);
        PageHeader pageHdr = isNew ? new PageHeader(buf, type) 
            : PageHeader.getView(buf);
        long oldLast = header.getLastOf(type);
        
        // Clean data.
        System.arraycopy(RecordFile.cleanData, 0, 
                         buf.getData(), 0, 
                         RecordFile.BLOCK_SIZE);
        pageHdr.setType(type);
        pageHdr.setPrev(oldLast);
        pageHdr.setNext(0);
        
        
        if (oldLast == 0)
            // This was the first one of this type
            header.setFirstOf(type, retval);
        header.setLastOf(type, retval);
        file.release(retval, true);
        
        // If there's a previous, fix up its pointer
        if (oldLast != 0) {
            buf = file.get(oldLast);
            pageHdr = PageHeader.getView(buf);
            pageHdr.setNext(retval);
            file.release(oldLast, true);
        }
        
        // remove the view, we have modified the type.
        buf.setView(null);
        
        return retval;
    }
    
    /**
     *  Frees a page of the indicated type.
     */
    void free(short type, long recid) throws IOException {
        if (type == Magic.FREE_PAGE)
            throw new Error("free free page?");
        if (recid == 0)
            throw new Error("free header page?");
        
        // get the page and read next and previous pointers
        BlockIo buf = file.get(recid);
        PageHeader pageHdr = PageHeader.getView(buf);
        long prev = pageHdr.getPrev();
        long next = pageHdr.getNext();
        
        // put the page at the front of the free list.
        pageHdr.setType(Magic.FREE_PAGE);
        pageHdr.setNext(header.getFirstOf(Magic.FREE_PAGE));
        pageHdr.setPrev(0);
        
        header.setFirstOf(Magic.FREE_PAGE, recid);
        file.release(recid, true);
        
        // remove the page from its old list
        if (prev != 0) {
            buf = file.get(prev);
            pageHdr = PageHeader.getView(buf);
            pageHdr.setNext(next);
            file.release(prev, true);
        }
        else {
            header.setFirstOf(type, next);
        }
        if (next != 0) {
            buf = file.get(next);
            pageHdr = PageHeader.getView(buf);
            pageHdr.setPrev(prev);
            file.release(next, true);
        }
        else {
            header.setLastOf(type, prev);
        }
        
    }
    
    
    /**
     *  Returns the page following the indicated block
     */
    long getNext(long block) throws IOException {
        try {
            return PageHeader.getView(file.get(block)).getNext();
        } finally {
            file.release(block, false);
        }
    }
    
    /**
     *  Returns the page before the indicated block
     */
    long getPrev(long block) throws IOException {
        try {
            return PageHeader.getView(file.get(block)).getPrev();
        } finally {
            file.release(block, false);
        }
    }
    
    /**
     *  Returns the first page on the indicated list.
     */
    long getFirst(short type) throws IOException {
        return header.getFirstOf(type);
    }

    /**
     *  Returns the last page on the indicated list.
     */
    long getLast(short type) throws IOException {
        return header.getLastOf(type);
    }
    
    
    /**
     *  Commit all pending (in-memory) data by flushing the page manager.
     *  This forces a flush of all outstanding blocks (this it's an implicit
     *  {@link RecordFile#commit} as well).
     */
    void commit() throws IOException {
        // write the header out
        file.release(headerBuf);
        file.commit();

        // and obtain it again
        headerBuf = file.get(0);
        header = new FileHeader(headerBuf, false);
    }

    /**
     *  Flushes the page manager. This forces a flush of all outstanding
     *  blocks (this it's an implicit {@link RecordFile#commit} as well).
     */
    void rollback() throws IOException {
        // release header
        file.discard(headerBuf);
        file.rollback();
        // and obtain it again
        headerBuf = file.get(0);
        if (headerBuf.readShort(0) == 0)
            header = new FileHeader(headerBuf, true);
        else
            header = new FileHeader(headerBuf, false);
    }
    
    /**
     *  Closes the page manager. This flushes the page manager and releases
     *  the lock on the header.
     */
    void close() throws IOException {   
        file.release(headerBuf);
        file.commit();
        headerBuf = null;
        header = null;
        file = null;
    }
    
    /**
     *  Returns the file header.
     */
    FileHeader getFileHeader() {
        return header;
    }
    
}
