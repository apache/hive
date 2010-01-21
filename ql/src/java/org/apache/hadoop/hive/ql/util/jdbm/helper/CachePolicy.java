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
 * $Id: CachePolicy.java,v 1.5 2003/11/01 13:25:02 dranatunga Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.helper;

import java.util.Enumeration;

/**
 * CachePolicity is an abstraction for different cache policies. (ie. MRU,
 * time-based, soft-refs, ...)
 * 
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="mailto:dranatunga@users.sourceforge.net">Dilum Ranatunga</a>
 * @version $Id: CachePolicy.java,v 1.5 2003/11/01 13:25:02 dranatunga Exp $
 */
public interface CachePolicy {

  /**
   * Place an object in the cache. If the cache does not currently contain an
   * object for the key specified, this mapping is added. If an object currently
   * exists under the specified key, the current object is replaced with the new
   * object.
   * <p>
   * If the changes to the cache cause the eviction of any objects
   * <strong>stored under other key(s)</strong>, events corresponding to the
   * evictions are fired for each object. If an event listener is unable to
   * handle the eviction, and throws a cache eviction exception, that exception
   * is propagated to the caller. If such an exception is thrown, the cache
   * itself should be left as it was before the <code>put()</code> operation was
   * invoked: the the object whose eviction failed is still in the cache, and
   * the new insertion or modification is reverted.
   * 
   * @param key
   *          key for the cached object
   * @param value
   *          the cached object
   * @throws CacheEvictionException
   *           propagated if, while evicting objects to make room for new
   *           object, an eviction listener encountered this problem.
   */
  public void put(Object key, Object value) throws CacheEvictionException;

  /**
   * Obtain the object stored under the key specified.
   * 
   * @param key
   *          key the object was cached under
   * @return the object if it is still in the cache, null otherwise.
   */
  public Object get(Object key);

  /**
   * Remove the object stored under the key specified. Note that since eviction
   * notices are only fired when objects under <strong>different keys</strong>
   * are evicted, no event is fired for any object stored under this key (see
   * {@link #put(Object, Object) put( )}).
   * 
   * @param key
   *          key the object was stored in the cache under.
   */
  public void remove(Object key);

  /**
   * Remove all objects from the cache. Consistent with {@link #remove(Object)
   * remove( )}, no eviction notices are fired.
   */
  public void removeAll();

  /**
   * Enumerate through the objects currently in the cache.
   */
  public Enumeration elements();

  /**
   * Add a listener to this cache policy.
   * <p>
   * If this cache policy already contains a listener that is equal to the one
   * being added, this call has no effect.
   * 
   * @param listener
   *          the (non-null) listener to add to this policy
   * @throws IllegalArgumentException
   *           if listener is null.
   */
  public void addListener(CachePolicyListener listener)
      throws IllegalArgumentException;

  /**
   * Remove a listener from this cache policy. The listener is found using
   * object equality, not identity.
   * 
   * @param listener
   *          the listener to remove from this policy
   */
  public void removeListener(CachePolicyListener listener);

}
