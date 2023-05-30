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
package org.apache.hadoop.hive.metastore.properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.UUID;

/**
 * Helper class that creates a type 5 uuid.
 * <p>This is computed from a set of updates using an SHA-1 message digest massaged into a UUID.
 * see <a href="https://en.wikipedia.org/wiki/Universally_unique_identifier">...</a>
 */
public class Digester {
    /** The Namespace uuid. */
    private final UUID nsuid;
    /** The digest used to compute the UUID. */
    private final MessageDigest md;
    /** A default namespace based on the class loading time. */
    private static final UUID TEMP_NS;
    static {
        MessageDigest md = createDigest();
        digest(md, System.currentTimeMillis());
        TEMP_NS = computeUUID(md);
    }

    /**
     * Allows to update the message digest from an object.
     */
    private static class TagOutputStream extends OutputStream {
        /** The digest to update. */
        private final MessageDigest md;

        /**
         * Sole ctor.
         * @param md the message digester
         */
        TagOutputStream(MessageDigest md) {
            this.md = md;
        }

        @Override
        public void write(int b) throws IOException {
            md.update((byte) b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            md.update(b, off, len);
        }
    }

    /**
     * @return a SHA-1 message digest
     */
    private static MessageDigest createDigest() {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException nsae) {
            throw new InternalError("SHA not supported");
        }
        return md;
    }

    /**
     * Updates a digest with a uuid.
     * @param md the digest to update
     * @param uid the uuid
     */
    private static MessageDigest digest(MessageDigest md, UUID uid) {
        if (uid != null) {
            long msb = uid.getMostSignificantBits();
            digest(md, msb);
            long lsb = uid.getLeastSignificantBits();
            digest(md, lsb);
        }
        return md;
    }

    /**
     * Updates a digest with an input stream.
     * @param md the digest to update
     * @param input the input to consume
     * @throws IllegalStateException if an io exception occurs
     */
    private static void digest(MessageDigest md, InputStream input) {
        try (OutputStream out = new TagOutputStream(md)) {
            byte[] buffer = new byte[1024];
            int read;
            while ((read = input.read(buffer, 0, 1024)) >= 0) {
                out.write(buffer, 0, read);
            }
        } catch(IOException xio) {
            throw new IllegalStateException(xio);
        }
    }


    /**
     * Updates a digest with a long.
     * @param md the digest to update
     * @param l8 the long
     */
    private static void digest(MessageDigest md, long l8) {
        md.update((byte) (l8 & 0xff));
        md.update((byte) (l8 >> 8));
        md.update((byte) (l8 >> 16));
        md.update((byte) (l8 >> 24));
        md.update((byte) (l8 >> 32));
        md.update((byte) (l8 >> 40));
        md.update((byte) (l8 >> 48));
        md.update((byte) (l8 >> 56));
    }

    /**
     * Updates a digest with an object.
     * @param md the digest to update
     * @param obj the object
     */
    private static void digest(MessageDigest md, Object obj) {
        if (obj == null) {
            return;
        }
        try (ObjectOutput out = new ObjectOutputStream(new TagOutputStream(md))) {
            out.writeObject(obj);
        } catch (IOException ex) {
            // ignore close exception
        }
        // ignore close exception
    }

    /**
     * Computes the uuid.
     * @param md the message digest used to compute the hash
     * @return the eTag as a type 5 uuid
     */
    private static UUID computeUUID(MessageDigest md) {
        byte[] sha1Bytes = md.digest();
        sha1Bytes[6] &= 0x0f;  /* clear version        */
        sha1Bytes[6] |= 0x50;  /* set to version 5     */
        sha1Bytes[8] &= 0x3f;  /* clear variant        */
        sha1Bytes[8] |= 0x80;  /* set to IETF variant  */

        // SHA generates 160 bytes; truncate to 128
        long msb = 0;
        //assert data.length == 16 || data.length == 20;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (sha1Bytes[i] & 0xff);
        }
        long lsb = 0;
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (sha1Bytes[i] & 0xff);
        }
        return new UUID(msb, lsb);
    }

    /**
     * A marker interface for objects that can be digested.
     */
    public interface Digestible {
        /**
         * Updates a digest with this variable.
         * @param digester the digester to update
         * @return true if this digestible actually contributed to the digest
         */
        boolean digest(Digester digester);
    }

    /**
     * A type 5 uuid is namespace + sha1; namespace in our case is an uuid.
     * Two instances of digesters built with the same namespace will produce the same UUIDs from the
     * same inputs.
     * @param namespace the uuid namespace
     */
    public Digester(UUID namespace) {
        nsuid = namespace == null? TEMP_NS : namespace;
        md = createDigest();
        // inject namespace
        digest(md, nsuid);
    }

    /**
     * A copy ctor base.
     * @param lnsuid the namespace uid
     * @param lmd the message digest
     */
    private Digester(UUID lnsuid, MessageDigest lmd)  {
        this.nsuid = lnsuid;
        this.md = lmd;
    }

    /**
     * Default ctor.
     * The created digester uses the class loading time as seed for its namespace; this means 2 instances of digester
     * built in different JVM instances will *NOT* produce the same UUIDs for the same input. Typical use is in
     * a non-persistent scenario, to verify an instance of an object has not been modified by checking
     * its digested UUID remained the same.
     * To get stable UUID computation across time and space in Digester usable in persistent scenario,
     * you *NEED* to use a namespace-based digester using {@link Digester(UUID)}, uuid that is easily created
     * using {@link UUID#nameUUIDFromBytes(byte[])} from any name/uri you might desire.
     */
    public Digester() {
        this(null);
    }

    /**
     * @return a clone of this instance
     */
    public Digester copy() {
        try {
            return new Digester(nsuid, (MessageDigest) md.clone());
        } catch (CloneNotSupportedException ex) {
            return null;
        }
    }

    /**
     * Computes the version tag from this digester.
     * <p>This uses the current message digest state and resets it.
     * @return the type 5 uuid
     */
    public UUID getUUID() {
        UUID uuid = computeUUID(md);
        md.reset();
        digest(nsuid);
        return uuid;
    }

    /**
     * Updates the digest with a boolean.
     * @param b the boolean
     * @return this digester
     */
    public Digester digest(boolean b) {
        md.update((byte) (b? 1 : 0));
        return this;
    }

    /**
     * Updates the digest with a char.
     * @param c the char
     * @return this digester
     */
    public Digester digest(char c) {
        md.update((byte) (c & 0xff));
        md.update((byte) (c >> 8));
        return this;
    }

    /**
     * Updates the digest with a bytes array.
     * @param bytes the bytes
     * @return this digester
     */
    public Digester digest(byte[] bytes) {
        if (bytes != null) {
            md.update(bytes);
        }
        return this;
    }

    /**
     * Updates the digest with an integer.
     * @param i4 the int
     * @return this digester
     */
    public Digester digest(int i4) {
        md.update((byte) (i4 & 0xff));
        md.update((byte) (i4 >> 8));
        md.update((byte) (i4 >> 16));
        md.update((byte) (i4 >> 24));
        return this;
    }

    /**
     * Updates the digest with a long.
     * @param l8 the long
     * @return this digester
     */
    public Digester digest(long l8) {
        digest(md, l8);
        return this;
    }

    /**
     * Updates the digest with a double.
     * @param f8 the double
     * @return this digester
     */
    public Digester digest(double f8) {
        digest(md, Double.doubleToRawLongBits(f8));
        return this;
    }

    /**
     * Updates the digest with a date.
     * @param date the date
     * @return this digester
     */
    public Digester digest(Date date) {
        if (date != null) {
            digest(md, date.getTime());
        }
        return this;
    }

    /**
     * Updates the digest with a string.
     * @param str the string
     * @return this digester
     */
    public Digester digest(String str) {
        if (str != null) {
            final int sz = str.length();
            for(int i = 0; i < sz; ++i) {
                digest(str.charAt(i));
            }
        }
        return this;
    }

    /**
     * Updates the digest with an uuid.
     * @param uid the uuid
     * @return this digester
     */
    public Digester digest(UUID uid) {
        digest(md, uid);
        return this;
    }

    /**
     * Updates the digest with an uuid.
     * @param uri the uri
     * @return this digester
     */
    public Digester digest(URI uri) {
        digest(md, uri.toString());
        return this;
    }

    /**
     * Updates the digest with an object that describes how it digests.
     * @param digestible the object
     * @return this digester
     */
    public Digester digest(Digestible digestible) {
        if (digestible != null) {
            digestible.digest(this);
        }
        return this;
    }

    /**
     * Updates the digest with a stream.
     * @param input the stream
     * @return this digester
     */
    public Digester digest(InputStream input) {
        if (input != null) {
            digest(md, input);
        }
        return this;
    }
    /**
     * Updates the digest with any (serializable) object.
     * @param obj the object
     * @return this digester
     */
    public Digester digest(Object obj) {
        if (obj instanceof Digestible) {
            return digest((Digestible) obj);
        }
        if (obj instanceof UUID) {
            return digest((UUID) obj);
        }
        if (obj instanceof URI) {
            return digest((URI) obj);
        }
        if (obj instanceof String) {
            return digest((String) obj);
        }
        if (obj instanceof Date) {
            return digest((Date) obj);
        }
        if (obj instanceof Integer) {
            return digest(((Integer) obj).intValue());
        }
        if (obj instanceof Long) {
            return digest(((Long) obj).longValue());
        }
        if (obj instanceof Double) {
            return digest(((Double) obj).doubleValue());
        }
        if (obj instanceof Boolean) {
            return digest(((Boolean) obj).booleanValue());
        }
        if (obj instanceof Character) {
            return digest(((Character) obj).charValue());
        }
        if (obj instanceof Short) {
            return digest(((Short) obj).intValue());
        }
        if (obj instanceof Float) {
            return digest(((Float) obj).doubleValue());
        }
        if (obj != null && obj.getClass().isArray()) {
           int sz  = java.lang.reflect.Array.getLength(obj);
           for(int i = 0; i < sz; ++i) {
               digest(java.lang.reflect.Array.get(obj, i));
           }
           return this;
        }
        // worst case
        digest(md, obj);
        return this;
    }
}
