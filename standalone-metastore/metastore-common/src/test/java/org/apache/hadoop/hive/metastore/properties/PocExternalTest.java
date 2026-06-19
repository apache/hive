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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;

/**
 *
 * @author henri
 */
public class PocExternalTest {
    static {
        SerializationProxy.registerType(253, PocExternal.class);
        //SerializationProxy.registerType(254, PocExternalDerive.class);
    }

    public PocExternalTest() {
    }


    // Another class that derives from PocExternal
    public static class PocExternalDerive extends PocExternal {
        private static final long serialVersionUID = 202212281720L;

        /** {@inheritDoc } */
        private Object writeReplace() throws ObjectStreamException {
            return new SerializationProxy<>(this);
        }

        /** {@inheritDoc } */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            throw new InvalidObjectException("proxy required");
        }

        public PocExternalDerive(String str, int i) {
            super(str, i);
        }

        public PocExternalDerive(ObjectInput in) throws IOException {
            this(serialVersionUID, in);
        }

        protected PocExternalDerive(long version, DataInput in) throws IOException {
            super(version, in);
        }

        public void write(ObjectOutput out) throws IOException {
            super.write(serialVersionUID, out);
        }

        public void write(ObjectOutput out, String more) throws IOException {
            super.write(serialVersionUID, out);
            out.writeUTF(more);
        }

        @Override
        public String getString() {
            return aString.replace('o', 'O');
        }

        @Override
        public int getInt() {
            return anInt + 1;
        }

    }

    // Another class that derives from PocExternal
    public static class Poc3 extends PocExternalDerive {
        private static final long serialVersionUID = 202301021116L;
        private String other;

        /**
         * {@inheritDoc }
         */
        private Object writeReplace() throws ObjectStreamException {
            return new SerializationProxy<>(this);
        }

        /**
         * {@inheritDoc }
         */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            throw new InvalidObjectException("proxy required");
        }

        public Poc3(String str, int i) {
            super(str, i);
            other = "other" + i;
        }

        public Poc3(ObjectInput in) throws IOException {
            super(serialVersionUID, in);
            other = in.readUTF();
        }

        public Poc3(ObjectInput in, boolean up) throws IOException {
            super(serialVersionUID, in);
            other = in.readUTF();
            if (up) {
                other = other.toUpperCase();
            }
        }

        public String getOther() {
            return other;
        }

        public void write(ObjectOutput out) throws IOException {
            super.write(serialVersionUID, out);
            out.writeUTF(other);
        }
        public void write(ObjectOutput out, String alien) throws IOException {
            super.write(serialVersionUID, out);
            out.writeUTF(alien);
        }
    }

    @Test
    public void testExternal() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        ObjectOutputStream out = new ObjectOutputStream(baos);

        PocExternal poc = new PocExternal("fourty-two", 42);
        out.writeObject(poc);
        byte[] data = baos.toByteArray();

        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data));
        PocExternal poci = (PocExternal) in.readObject();

        Assert.assertEquals(poc.getString(), poci.getString());
        Assert.assertEquals(poc.getInt(), poci.getInt());
    }

    @Test
    public void testExternalProxy() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        ObjectOutputStream out = new ObjectOutputStream(baos);

        PocExternal poc = new PocExternal("fourty-two", 42);
        byte[] data = SerializationProxy.toBytes(poc);

        PocExternal poci = SerializationProxy.fromBytes(data);

        Assert.assertEquals(poc.getString(), poci.getString());
        Assert.assertEquals(poc.getInt(), poci.getInt());
    }

    @Test
    public void testExternalDerive() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        ObjectOutputStream out = new ObjectOutputStream(baos);

        PocExternalDerive pocx = new PocExternalDerive("fourty-two", 42);
        out.writeObject(pocx);
        byte[] data = baos.toByteArray();

        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data));
        PocExternal poci = (PocExternal) in.readObject();

        Assert.assertEquals("fOurty-twO", poci.getString());
        Assert.assertEquals(43, poci.getInt());
        Assert.assertEquals(pocx.getString(), poci.getString());
        Assert.assertEquals(pocx.getInt(), poci.getInt());
    }

    @Test
    public void testPoc3() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        ObjectOutputStream out = new ObjectOutputStream(baos);

        Poc3 pocx = new Poc3("fourty-two", 42);
        out.writeObject(pocx);
        byte[] data = baos.toByteArray();
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data));
        Poc3 poc3 = (Poc3) in.readObject();
        Assert.assertEquals("other42", poc3.getOther());

        data = SerializationProxy.toBytes(poc3, "alien");
        poc3 = SerializationProxy.fromBytes(data, true);
        Assert.assertEquals("ALIEN", poc3.getOther());
    }
}
