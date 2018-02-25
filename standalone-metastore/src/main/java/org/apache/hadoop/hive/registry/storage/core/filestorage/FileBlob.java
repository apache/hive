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
package org.apache.hadoop.hive.registry.storage.core.filestorage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.hive.registry.common.Schema;
import org.apache.hadoop.hive.registry.storage.core.annotation.StorableEntity;
import org.apache.hadoop.hive.registry.storage.core.PrimaryKey;
import org.apache.hadoop.hive.registry.storage.core.StorableKey;
import org.apache.hadoop.hive.registry.storage.core.annotation.VersionField;
import org.apache.hadoop.hive.registry.storage.core.catalog.AbstractStorable;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;

/**
 * Storable for storing any kind of files as Blobs into the underlying Db.
 */
@StorableEntity
public class FileBlob extends AbstractStorable {
    public static final String NAMESPACE = "fileblob";

    private static final String NAME = "name";

    @VersionField
    private Long version;

    private String name;

    private InputStream data;

    private Long timestamp;

    public FileBlob() {
    }

    public FileBlob(FileBlob other) {
        version = other.version;
        name = other.name;
        timestamp = other.timestamp;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public InputStream getData() {
        return data;
    }

    public void setData(InputStream data) {
        this.data = data;
    }

    public void setData(byte[] data) {
        this.data = new ByteArrayInputStream(data);
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @JsonIgnore
    @Override
    public String getNameSpace() {
        return NAMESPACE;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        return new PrimaryKey(Collections.singletonMap(new Schema.Field(NAME, Schema.Type.STRING), this.name));
    }

    public static StorableKey getStorableKey(String name) {
        Schema.Field field = new Schema.Field(NAME, Schema.Type.STRING);
        return new StorableKey(NAMESPACE, new PrimaryKey(Collections.singletonMap(field, name)));
    }

    @Override
    public boolean isCacheable() {
        return false;
    }
}
