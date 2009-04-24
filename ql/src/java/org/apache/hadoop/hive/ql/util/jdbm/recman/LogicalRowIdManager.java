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
 * $Id: LogicalRowIdManager.java,v 1.3 2005/06/25 23:12:32 doomdark Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.recman;

import java.io.IOException;

/**
 *  This class manages the linked lists of logical rowid pages.
 */
final class LogicalRowIdManager {
    // our record file and associated page manager
    private RecordFile file;
    private PageManager pageman;
    private FreeLogicalRowIdPageManager freeman;

    /**
     *  Creates a log rowid manager using the indicated record file and
     *  page manager
     */
    LogicalRowIdManager(RecordFile file, PageManager pageman)
  throws IOException {
  this.file = file;
  this.pageman = pageman;
  this.freeman = new FreeLogicalRowIdPageManager(file, pageman);

    }

    /**
     *  Creates a new logical rowid pointing to the indicated physical
     *  id
     */
    Location insert(Location loc)
    throws IOException {
  // check whether there's a free rowid to reuse
  Location retval = freeman.get();
  if (retval == null) {
      // no. This means that we bootstrap things by allocating
      // a new translation page and freeing all the rowids on it.
      long firstPage = pageman.allocate(Magic.TRANSLATION_PAGE);
      short curOffset = TranslationPage.O_TRANS;
      for (int i = 0; i < TranslationPage.ELEMS_PER_PAGE; i++) {
    freeman.put(new Location(firstPage, curOffset));
    curOffset += PhysicalRowId.SIZE;
      }
      retval = freeman.get();
      if (retval == null) {
    throw new Error("couldn't obtain free translation");
      }
  }
  // write the translation.
  update(retval, loc);
  return retval;
    }

    /**
     *  Releases the indicated logical rowid.
     */
    void delete(Location rowid)
  throws IOException {

  freeman.put(rowid);
    }

    /**
     *  Updates the mapping
     *
     *  @param rowid The logical rowid
     *  @param loc The physical rowid
     */
    void update(Location rowid, Location loc)
    throws IOException {

        TranslationPage xlatPage = TranslationPage.getTranslationPageView(
                                       file.get(rowid.getBlock()));
        PhysicalRowId physid = xlatPage.get(rowid.getOffset());
        physid.setBlock(loc.getBlock());
        physid.setOffset(loc.getOffset());
        file.release(rowid.getBlock(), true);
    }

    /**
     *  Returns a mapping
     *
     *  @param rowid The logical rowid
     *  @return The physical rowid
     */
    Location fetch(Location rowid)
    throws IOException {

        TranslationPage xlatPage = TranslationPage.getTranslationPageView(
                                       file.get(rowid.getBlock()));
        try {
            Location retval = new Location(xlatPage.get(rowid.getOffset()));
            return retval;
        } finally {
            file.release(rowid.getBlock(), false);
        }
    }

}
