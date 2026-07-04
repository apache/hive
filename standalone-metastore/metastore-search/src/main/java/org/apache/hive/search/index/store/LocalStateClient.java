/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.index.store;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.search.exception.IndexIOException;
import org.apache.hive.search.index.manifest.IndexManifest;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record LocalStateClient(Directory directory, String indexName)
    implements IndexStateClient {
  private static final Logger LOG = LoggerFactory.getLogger(LocalStateClient.class);

  public static boolean isStagingFile(String fileName) {
    return IndexManifest.STAGING_MANIFEST_FILE_NAME.equals(fileName);
  }

  @Override
  public Optional<IndexManifest> readManifest() throws IOException {
    if (hasStagingManifest()) {
      return Optional.empty();
    }
    if (!DirectoryReader.indexExists(directory)) {
      return Optional.empty();
    }
    SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
    Map<String, String> userData = segmentInfos.getUserData();
    String nid = userData.get("nid");
    if (StringUtils.isEmpty(nid)) {
      return Optional.empty();
    }
    String modelName = userData.get("model");
    if (StringUtils.isEmpty(modelName)) {
      return Optional.empty();
    }
    long eventId;
    try {
      eventId = Long.parseLong(nid);
    } catch (NumberFormatException e) {
      throw new IndexIOException("Invalid commit checkpoint in index metadata: nid=" + nid, e);
    }
    List<IndexManifest.IndexFile> files = new ArrayList<>();
    for (String file : segmentInfos.files(true)) {
      files.add(new IndexManifest.IndexFile(file, fileLength(file)));
    }
    return Optional.of(IndexManifest.create(indexName, files, modelName, eventId));
  }

  @Override
  public Optional<IndexManifest> readStagingManifest() throws IOException {
    if (!hasStagingManifest()) {
      return Optional.empty();
    }
    try (IndexInput in = directory.openInput(IndexManifest.STAGING_MANIFEST_FILE_NAME,
        IOContext.DEFAULT)) {
      byte[] bytes = new byte[(int) in.length()];
      in.readBytes(bytes, 0, bytes.length);
      return Optional.of(IndexManifest.fromJson(bytes));
    } catch (IOException e) {
      LOG.warn("Failed to read the staging manifest", e);
      return Optional.empty();
    }
  }

  @Override
  public void writeStagingManifest(IndexManifest manifest) throws IOException {
    write(IndexManifest.STAGING_MANIFEST_FILE_NAME,
        new ByteArrayInputStream(manifest.toJsonBytes()));
  }

  @Override
  public void clearStagingManifest() throws IOException {
    delete(IndexManifest.STAGING_MANIFEST_FILE_NAME);
  }

  public boolean hasStagingManifest() throws IOException {
    return Arrays.asList(directory.listAll())
        .contains(IndexManifest.STAGING_MANIFEST_FILE_NAME);
  }

  @Override
  public IndexManifest readLocalFileManifest() throws IOException {
    List<IndexManifest.IndexFile> files = new ArrayList<>();
    for (String file : directory.listAll()) {
      if (isStagingFile(file)) {
        continue;
      }
      files.add(new IndexManifest.IndexFile(file, fileLength(file)));
    }
    return IndexManifest.create(indexName, files, "", -1);
  }

  public boolean isIndexReadable() throws IOException {
    return DirectoryReader.indexExists(directory);
  }

  @Override
  public void validateRestoredIndex(IndexManifest expected) throws IndexIOException {
    try {
      if (!expected.sameFilesAs(readLocalFileManifest())) {
        throw new IndexIOException("Restored index files do not match staging manifest");
      }
      if (!isIndexReadable()) {
        throw new IndexIOException("Restored index is not readable by Lucene");
      }
      SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
      Map<String, String> userData = segmentInfos.getUserData();
      String nid = userData.get("nid");
      if (StringUtils.isEmpty(nid)) {
        throw new IndexIOException("Restored index is missing commit checkpoint");
      }
      String modelName = userData.get("model");
      if (StringUtils.isEmpty(modelName)) {
        throw new IndexIOException("Restored index is missing embedding model metadata");
      }
      long eventId;
      try {
        eventId = Long.parseLong(nid);
      } catch (NumberFormatException e) {
        throw new IndexIOException("Restored index has invalid commit checkpoint: nid=" + nid, e);
      }
      if (eventId != expected.lastEventId()) {
        throw new IndexIOException(
            "Restored index checkpoint mismatch: expected nid=" + expected.lastEventId()
                + " but found nid=" + eventId);
      }
      if (!expected.modelName().equals(modelName)) {
        throw new IndexIOException("Restored index embedding model mismatch");
      }
    } catch (IOException e) {
      throw IndexIOException.wrap(e);
    }
  }

  @Override
  public InputStream read(String fileName) throws IOException {
    return new DirectoryInputStream(directory, fileName);
  }

  @Override
  public void write(String fileName, InputStream stream) throws IOException {
    if (Arrays.asList(directory.listAll()).contains(fileName)) {
      directory.deleteFile(fileName);
    }
    try (IndexOutput out = directory.createOutput(fileName, IOContext.DEFAULT)) {
      stream.transferTo(new OutputStreamAdapter(out));
    }
  }

  @Override
  public void delete(String fileName) throws IOException {
    if (Arrays.asList(directory.listAll()).contains(fileName)) {
      directory.deleteFile(fileName);
    }
  }

  @Override
  public void clear() throws IOException {
    for (String file : directory.listAll()) {
      directory.deleteFile(file);
    }
  }

  private long fileLength(String fileName) throws IOException {
    try (IndexInput input = directory.openInput(fileName, IOContext.DEFAULT)) {
      return input.length();
    }
  }

  private static final class DirectoryInputStream extends InputStream {
    private final IndexInput input;

    DirectoryInputStream(Directory directory, String fileName) throws IOException {
      if (!Arrays.asList(directory.listAll()).contains(fileName)) {
        throw new NoSuchFileException(fileName);
      }
      this.input = directory.openInput(fileName, IOContext.DEFAULT);
    }

    @Override
    public int read() throws IOException {
      if (input.getFilePointer() >= input.length()) {
        return -1;
      }
      return Byte.toUnsignedInt(input.readByte());
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int remaining = (int) Math.min(len, input.length() - input.getFilePointer());
      if (remaining <= 0) {
        return -1;
      }
      input.readBytes(b, off, remaining);
      return remaining;
    }

    @Override
    public void close() throws IOException {
      input.close();
    }
  }

  private static final class OutputStreamAdapter extends OutputStream {
    private final IndexOutput output;

    OutputStreamAdapter(IndexOutput output) {
      this.output = output;
    }

    @Override
    public void write(int b) throws IOException {
      output.writeByte((byte) b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      output.writeBytes(b, off, len);
    }

    @Override
    public void close() throws IOException {
      output.close();
    }
  }
}
