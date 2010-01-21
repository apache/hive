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
 * $Id: PageHeader.java,v 1.2 2003/09/21 15:47:01 boisvert Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.recman;

import java.io.IOException;

/**
 * This class represents a page header. It is the common superclass for all
 * different page views.
 */
public class PageHeader implements BlockView {
  // offsets
  private static final short O_MAGIC = 0; // short magic
  private static final short O_NEXT = Magic.SZ_SHORT; // long next
  private static final short O_PREV = O_NEXT + Magic.SZ_LONG; // long prev
  protected static final short SIZE = O_PREV + Magic.SZ_LONG;

  // my block
  protected BlockIo block;

  /**
   * Constructs a PageHeader object from a block
   * 
   * @param block
   *          The block that contains the file header
   * @throws IOException
   *           if the block is too short to keep the file header.
   */
  protected PageHeader(BlockIo block) {
    initialize(block);
    if (!magicOk()) {
      throw new Error("CRITICAL: page header magic for block "
          + block.getBlockId() + " not OK " + getMagic());
    }
  }

  /**
   * Constructs a new PageHeader of the indicated type. Used for newly created
   * pages.
   */
  PageHeader(BlockIo block, short type) {
    initialize(block);
    setType(type);
  }

  /**
   * Factory method to create or return a page header for the indicated block.
   */
  static PageHeader getView(BlockIo block) {
    BlockView view = block.getView();
    if (view != null && view instanceof PageHeader) {
      return (PageHeader) view;
    } else {
      return new PageHeader(block);
    }
  }

  private void initialize(BlockIo block) {
    this.block = block;
    block.setView(this);
  }

  /**
   * Returns true if the magic corresponds with the fileHeader magic.
   */
  private boolean magicOk() {
    int magic = getMagic();
    return magic >= Magic.BLOCK
        && magic <= (Magic.BLOCK + Magic.FREEPHYSIDS_PAGE);
  }

  /**
   * For paranoia mode
   */
  protected void paranoiaMagicOk() {
    if (!magicOk()) {
      throw new Error("CRITICAL: page header magic not OK " + getMagic());
    }
  }

  /** Returns the magic code */
  short getMagic() {
    return block.readShort(O_MAGIC);
  }

  /** Returns the next block. */
  long getNext() {
    paranoiaMagicOk();
    return block.readLong(O_NEXT);
  }

  /** Sets the next block. */
  void setNext(long next) {
    paranoiaMagicOk();
    block.writeLong(O_NEXT, next);
  }

  /** Returns the previous block. */
  long getPrev() {
    paranoiaMagicOk();
    return block.readLong(O_PREV);
  }

  /** Sets the previous block. */
  void setPrev(long prev) {
    paranoiaMagicOk();
    block.writeLong(O_PREV, prev);
  }

  /** Sets the type of the page header */
  void setType(short type) {
    block.writeShort(O_MAGIC, (short) (Magic.BLOCK + type));
  }
}
