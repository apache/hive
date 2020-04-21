package org.apache.hadoop.hive.metastore.utils;

import com.google.common.collect.BiMap;
import com.google.common.collect.EnumHashBiMap;
import org.apache.hadoop.hive.metastore.api.LockType;

import java.util.Optional;

/**
 * Provides utility methods for {@link org.apache.hadoop.hive.metastore.api.LockType}.
 * One use case is to encapsulate each lock type's persistence encoding, since
 * Thrift-generated enums cannot be extended with character values.
 */
public class LockTypeUtil {

    private static final char UNKNOWN_LOCK_TYPE_ENCODING = 'z';
    private static final BiMap<LockType, Character> persistenceEncodings = EnumHashBiMap.create(LockType.class);
    static {
        persistenceEncodings.put(LockType.SHARED_READ, 'r');
        persistenceEncodings.put(LockType.SHARED_WRITE, 'w');
        persistenceEncodings.put(LockType.EXCL_WRITE, 'x');
        persistenceEncodings.put(LockType.EXCLUSIVE, 'e');
    }

    public static char getEncoding(LockType lockType) {
        return persistenceEncodings.getOrDefault(lockType, UNKNOWN_LOCK_TYPE_ENCODING);
    }

    public static String getEncodingAsStr(LockType lockType) {
        return Character.toString(getEncoding(lockType));
    }

    public static Optional<LockType> getLockTypeFromEncoding(char encoding) {
        return Optional.ofNullable(persistenceEncodings.inverse().get(encoding));
    }
}
