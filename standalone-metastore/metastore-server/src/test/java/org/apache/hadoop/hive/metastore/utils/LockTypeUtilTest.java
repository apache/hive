package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(MetastoreUnitTest.class)
public class LockTypeUtilTest {

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
