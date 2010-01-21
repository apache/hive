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
 * Contributions are (C) Copyright 2000 by their associated contributors.
 *
 */

package org.apache.hadoop.hive.ql.util.jdbm.htree;

import java.io.IOException;

import org.apache.hadoop.hive.ql.util.jdbm.RecordManager;
import org.apache.hadoop.hive.ql.util.jdbm.helper.FastIterator;

/**
 * Persistent hashtable implementation for PageManager. Implemented as an H*Tree
 * structure.
 * 
 * WARNING! If this instance is used in a transactional context, it *must* be
 * discarded after a rollback.
 * 
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: HTree.java,v 1.3 2005/06/25 23:12:32 doomdark Exp $
 */
public class HTree {

  /**
   * Root hash directory.
   */
  private final HashDirectory _root;

  /**
   * Private constructor
   * 
   * @param root
   *          Root hash directory.
   */
  private HTree(HashDirectory root) {
    _root = root;
  }

  /**
   * Create a persistent hashtable.
   * 
   * @param recman
   *          Record manager used for persistence.
   */
  public static HTree createInstance(RecordManager recman) throws IOException {
    HashDirectory root;
    long recid;

    root = new HashDirectory((byte) 0);
    recid = recman.insert(root);
    root.setPersistenceContext(recman, recid);

    return new HTree(root);
  }

  /**
   * Load a persistent hashtable
   * 
   * @param recman
   *          RecordManager used to store the persistent hashtable
   * @param root_recid
   *          Record id of the root directory of the HTree
   */
  public static HTree load(RecordManager recman, long root_recid)
      throws IOException {
    HTree tree;
    HashDirectory root;

    root = (HashDirectory) recman.fetch(root_recid);
    root.setPersistenceContext(recman, root_recid);
    tree = new HTree(root);
    return tree;
  }

  /**
   * Associates the specified value with the specified key.
   * 
   * @param key
   *          key with which the specified value is to be assocated.
   * @param value
   *          value to be associated with the specified key.
   */
  public synchronized void put(Object key, Object value) throws IOException {
    _root.put(key, value);
  }

  /**
   * Returns the value which is associated with the given key. Returns
   * <code>null</code> if there is not association for this key.
   * 
   * @param key
   *          key whose associated value is to be returned
   */
  public synchronized Object get(Object key) throws IOException {
    return _root.get(key);
  }

  /**
   * Remove the value which is associated with the given key. If the key does
   * not exist, this method simply ignores the operation.
   * 
   * @param key
   *          key whose associated value is to be removed
   */
  public synchronized void remove(Object key) throws IOException {
    _root.remove(key);
  }

  /**
   * Returns an enumeration of the keys contained in this
   */
  public synchronized FastIterator keys() throws IOException {
    return _root.keys();
  }

  /**
   * Returns an enumeration of the values contained in this
   */
  public synchronized FastIterator values() throws IOException {
    return _root.values();
  }

  /**
   * Get the record identifier used to load this hashtable.
   */
  public long getRecid() {
    return _root.getRecid();
  }

}
