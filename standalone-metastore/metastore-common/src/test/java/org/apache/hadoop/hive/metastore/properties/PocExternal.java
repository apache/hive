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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * A Proof Of Concept about externalizable.
 */
public class PocExternal implements Serializable {
    private static final long serialVersionUID = 202212281714L;

    /** {@inheritDoc } */
    private Object writeReplace() throws ObjectStreamException {
        return new SerializationProxy<>(this);
    }

    /** {@inheritDoc } */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        throw new InvalidObjectException("proxy required");
    }

    protected final String aString;
    protected final int anInt;

    public PocExternal(ObjectInput in) throws IOException {
        this(serialVersionUID, in);
    }

    public void write(ObjectOutput out) throws IOException {
        write(serialVersionUID, out);
    }
    protected PocExternal(long version, DataInput in) throws IOException {
        long serial = in.readLong();
        if (serial != version) {
            throw new IOException("invalid version");
        }
        aString = in.readUTF();
        anInt = in.readInt();
    }

    protected void write(long version, DataOutput out) throws IOException {
        out.writeLong(version);
        out.writeUTF(aString);
        out.writeInt(anInt);
    }

    public PocExternal(String str, int i) {
        aString = str;
        anInt = i;
    }

    public String getString() {
        return aString;
    }

    public int getInt() {
        return anInt;
    }

}
