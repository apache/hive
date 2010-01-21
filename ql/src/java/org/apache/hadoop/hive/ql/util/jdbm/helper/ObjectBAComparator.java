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
 * Copyright 2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2001 by their associated contributors.
 *
 */

package org.apache.hadoop.hive.ql.util.jdbm.helper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator for objects which have been serialized into byte arrays. In
 * effect, it wraps another Comparator which compares object and provides
 * transparent deserialization from byte array to object.
 * 
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: ObjectBAComparator.java,v 1.1 2002/05/31 06:33:20 boisvert Exp
 *          $
 */
public final class ObjectBAComparator implements Comparator, Serializable {

  /**
   * Version id for serialization.
   */
  final static long serialVersionUID = 1L;

  /**
   * Wrapped comparator.
   */
  private final Comparator _comparator;

  /**
   * Construct an ObjectByteArrayComparator which wraps an Object Comparator.
   * 
   * @param comparator
   *          Object comparator.
   */
  public ObjectBAComparator(Comparator comparator) {
    if (comparator == null) {
      throw new IllegalArgumentException("Argument 'comparator' is null");
    }

    _comparator = comparator;
  }

  /**
   * Compare two objects.
   * 
   * @param obj1
   *          First object
   * @param obj2
   *          Second object
   * @return 1 if obj1 > obj2, 0 if obj1 == obj2, -1 if obj1 < obj2
   */
  public int compare(Object obj1, Object obj2) {
    if (obj1 == null) {
      throw new IllegalArgumentException("Argument 'obj1' is null");
    }

    if (obj2 == null) {
      throw new IllegalArgumentException("Argument 'obj2' is null");
    }

    try {
      obj1 = Serialization.deserialize((byte[]) obj1);
      obj2 = Serialization.deserialize((byte[]) obj2);

      return _comparator.compare(obj1, obj2);
    } catch (IOException except) {
      throw new WrappedRuntimeException(except);
    } catch (ClassNotFoundException except) {
      throw new WrappedRuntimeException(except);
    }
  }

  /**
   * Compare two byte arrays.
   */
  public static int compareByteArray(byte[] thisKey, byte[] otherKey) {
    int len = Math.min(thisKey.length, otherKey.length);

    // compare the byte arrays
    for (int i = 0; i < len; i++) {
      if (thisKey[i] >= 0) {
        if (otherKey[i] >= 0) {
          // both positive
          if (thisKey[i] < otherKey[i]) {
            return -1;
          } else if (thisKey[i] > otherKey[i]) {
            return 1;
          }
        } else {
          // otherKey is negative => greater (because MSB is 1)
          return -1;
        }
      } else {
        if (otherKey[i] >= 0) {
          // thisKey is negative => greater (because MSB is 1)
          return 1;
        } else {
          // both negative
          if (thisKey[i] < otherKey[i]) {
            return -1;
          } else if (thisKey[i] > otherKey[i]) {
            return 1;
          }
        }
      }
    }
    if (thisKey.length == otherKey.length) {
      return 0;
    }
    if (thisKey.length < otherKey.length) {
      return -1;
    }
    return 1;
  }

}
