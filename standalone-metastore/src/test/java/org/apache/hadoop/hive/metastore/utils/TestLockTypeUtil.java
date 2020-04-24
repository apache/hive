/*
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
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(MetastoreUnitTest.class)
public class TestLockTypeUtil {

    @Test
    public void testGetEncoding() {
        assertEquals('r', LockTypeUtil.getEncoding(LockType.SHARED_READ));
        assertEquals('w', LockTypeUtil.getEncoding(LockType.SHARED_WRITE));
        assertEquals('x', LockTypeUtil.getEncoding(LockType.EXCL_WRITE));
        assertEquals('e', LockTypeUtil.getEncoding(LockType.EXCLUSIVE));
    }

    @Test
    public void testGetLockType() {
        assertEquals(LockType.SHARED_READ, LockTypeUtil.getLockTypeFromEncoding('r').get());
        assertEquals(LockType.SHARED_WRITE, LockTypeUtil.getLockTypeFromEncoding('w').get());
        assertEquals(LockType.EXCL_WRITE, LockTypeUtil.getLockTypeFromEncoding('x').get());
        assertEquals(LockType.EXCLUSIVE, LockTypeUtil.getLockTypeFromEncoding('e').get());
        assertFalse(LockTypeUtil.getLockTypeFromEncoding('y').isPresent());
    }
}
