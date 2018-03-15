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

import org.apache.hadoop.hive.registry.common.util.FileStorage;
import org.apache.hadoop.hive.registry.storage.core.StorageManager;
import org.apache.hadoop.hive.registry.storage.core.StorageManagerAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

/**
 * DB based file storage that stores the file as a blob.
 */
public class DbFileStorage implements FileStorage, StorageManagerAware {
    private static final Logger LOG = LoggerFactory.getLogger(DbFileStorage.class);
    private StorageManager dao;

    @Override
    public void init(Map<String, String> config) {
    }

    @Override
    public String upload(InputStream inputStream, String name) throws IOException {
        LOG.debug("Uploading '{}'", name);
        long start = System.nanoTime();
        Optional<FileBlob> existing = get(name);
        if (existing.isPresent()) {
            LOG.debug("Updating existing file '{}'", name);
            FileBlob updated = new FileBlob(existing.get());
            updated.setData(inputStream);
            updated.setVersion(existing.get().getVersion() + 1);
            updated.setTimestamp(System.currentTimeMillis());
            dao.update(updated);
        } else {
            LOG.debug("Adding new file '{}'", name);
            FileBlob fileBlob = new FileBlob();
            fileBlob.setName(name);
            fileBlob.setTimestamp(System.currentTimeMillis());
            fileBlob.setVersion(0L);
            fileBlob.setData(inputStream);
            dao.add(fileBlob);
        }
        LOG.debug("Uploaded '{}' in '{}' milliseconds", name, (System.nanoTime() - start)/1000000);
        return name;
    }

    @Override
    public InputStream download(String name) throws IOException {
        LOG.debug("Downloading file '{}'", name);
        FileBlob res = get(name)
                .orElseThrow(() -> new IOException("Not able to get file blob with name : " + name));
        return res.getData();
    }

    @Override
    public boolean delete(String name) throws IOException {
        LOG.debug("Deleting file '{}'", name);
        return dao.remove(FileBlob.getStorableKey(name)) != null;
    }

    @Override
    public boolean exists(String name) {
        return dao.exists(FileBlob.getStorableKey(name));
    }

    private Optional<FileBlob> get(String name) {
        return Optional.ofNullable(dao.get(FileBlob.getStorableKey(name)));
    }

    @Override
    public void setStorageManager(StorageManager storageManager) {
        this.dao = storageManager;
    }
}
