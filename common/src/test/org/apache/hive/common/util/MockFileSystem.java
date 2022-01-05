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

package org.apache.hive.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.mockito.MockitoAnnotations;

public class MockFileSystem extends FileSystem {
  final List<MockFile> files = new ArrayList<MockFile>();
  final Map<MockFile, FileStatus> fileStatusMap = new HashMap<>();
  Path workingDir = new Path("/");
  // statics for when the mock fs is created via FileSystem.get
  private static String blockedUgi = null;
  private final static List<MockFile> globalFiles = new ArrayList<MockFile>();
  protected Statistics statistics;
  public boolean allowDelete = false;

  public MockFileSystem() {
    // empty
  }

  @Override
  public void initialize(URI uri, Configuration conf) {
    setConf(conf);
    statistics = getStatistics("mock", getClass());
  }

  public MockFileSystem(Configuration conf, MockFile... files) {
    setConf(conf);
    this.files.addAll(Arrays.asList(files));
    statistics = getStatistics("mock", getClass());
  }

  public static void setBlockedUgi(String s) {
    blockedUgi = s;
  }

  public void clear() {
    files.clear();
  }

  @Override
  public URI getUri() {
    try {
      return new URI("mock:///");
    } catch (URISyntaxException err) {
      throw new IllegalArgumentException("huh?", err);
    }
  }

  // increments file modification time
  public void touch(MockFile file) {
    if (fileStatusMap.containsKey(file)) {
      FileStatus fileStatus = fileStatusMap.get(file);
      FileStatus fileStatusNew = new FileStatus(fileStatus.getLen(), fileStatus.isDirectory(),
          fileStatus.getReplication(), fileStatus.getBlockSize(),
          fileStatus.getModificationTime() + 1, fileStatus.getAccessTime(),
          fileStatus.getPermission(), fileStatus.getOwner(), fileStatus.getGroup(),
          fileStatus.getPath());
      fileStatusMap.put(file, fileStatusNew);
    }
  }

  @SuppressWarnings("serial")
  public static class MockAccessDenied extends IOException {
  }

  @Override
  public FSDataInputStream open(Path path, int i) throws IOException {
    statistics.incrementReadOps(1);
    checkAccess();
    MockFile file = findFile(path);
    if (file != null) return new FSDataInputStream(new MockInputStream(file));
    throw new IOException("File not found: " + path);
  }

  public MockFile findFile(Path path) {
    for (MockFile file: files) {
      if (file.path.equals(path)) {
        return file;
      }
    }
    for (MockFile file: globalFiles) {
      if (file.path.equals(path)) {
        return file;
      }
    }
    return null;
  }

  private void checkAccess() throws IOException {
    if (blockedUgi == null) return;
    if (!blockedUgi.equals(UserGroupInformation.getCurrentUser().getShortUserName())) return;
    throw new MockAccessDenied();
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progressable
                                   ) throws IOException {
    statistics.incrementWriteOps(1);
    checkAccess();
    MockFile file = findFile(path);
    if (file == null) {
      file = new MockFile(path.toString(), (int) blockSize, new byte[0]);
      files.add(file);
    }
    return new MockOutputStream(file);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize,
                                   Progressable progressable
                                   ) throws IOException {
    statistics.incrementWriteOps(1);
    checkAccess();
    return create(path, FsPermission.getDefault(), true, bufferSize,
        (short) 3, 256 * 1024, progressable);
  }

  @Override
  public boolean rename(Path path, Path path2) throws IOException {
    statistics.incrementWriteOps(1);
    checkAccess();

    MockFile file = findFile(path);
    if (file == null || findFile(path2) != null) {
      return false;
    }

    files.add(new MockFile(path2.toString(), file.blockSize, file.content));
    files.remove(file);
    return true;
  }

  @Override
  public boolean delete(Path path) throws IOException {
    statistics.incrementWriteOps(1);
    checkAccess();
    return false;
  }

  @Override
  public boolean delete(Path path, boolean isRecursive) throws IOException {
    statistics.incrementWriteOps(1);
    checkAccess();
    return allowDelete && isRecursive && deleteMatchingFiles(files, path.toString());
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
      throws IOException {
    return new RemoteIterator<LocatedFileStatus>() {
      private Iterator<LocatedFileStatus> iterator = listLocatedFileStatuses(f).iterator();

      @Override
      public boolean hasNext() throws IOException {
        return iterator.hasNext();
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        return iterator.next();
      }
    };
  }

  private List<LocatedFileStatus> listLocatedFileStatuses(Path path) throws IOException {
    statistics.incrementReadOps(1);
    checkAccess();
    path = path.makeQualified(this);
    List<LocatedFileStatus> result = new ArrayList<>();
    String pathname = path.toString();
    String pathnameAsDir = pathname + "/";
    Set<String> dirs = new TreeSet<String>();
    MockFile file = findFile(path);
    if (file != null) {
      result.add(createLocatedStatus(file));
      return result;
    }
    findMatchingLocatedFiles(files, pathnameAsDir, dirs, result);
    findMatchingLocatedFiles(globalFiles, pathnameAsDir, dirs, result);
    // for each directory add it once
    for(String dir: dirs) {
      result.add(createLocatedDirectory(new MockPath(this, pathnameAsDir + dir)));
    }
    return result;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    statistics.incrementReadOps(1);
    checkAccess();
    path = path.makeQualified(this);
    List<FileStatus> result = new ArrayList<FileStatus>();
    String pathname = path.toString();
    String pathnameAsDir = pathname + "/";
    Set<String> dirs = new TreeSet<String>();
    MockFile file = findFile(path);
    if (file != null) {
      return new FileStatus[]{createStatus(file)};
    }
    findMatchingFiles(files, pathnameAsDir, dirs, result);
    findMatchingFiles(globalFiles, pathnameAsDir, dirs, result);
    // for each directory add it once
    for(String dir: dirs) {
      result.add(createDirectory(new MockPath(this, pathnameAsDir + dir)));
    }
    return result.toArray(new FileStatus[result.size()]);
  }

  private void findMatchingFiles(
      List<MockFile> files, String pathnameAsDir, Set<String> dirs, List<FileStatus> result) {
    for (MockFile file: files) {
      String filename = file.path.toString();
      if (filename.startsWith(pathnameAsDir)) {
        String tail = filename.substring(pathnameAsDir.length());
        int nextSlash = tail.indexOf('/');
        if (nextSlash > 0) {
          dirs.add(tail.substring(0, nextSlash));
        } else {
          result.add(createStatus(file));
        }
      }
    }
  }

  private boolean deleteMatchingFiles(List<MockFile> files, String path) {
    Iterator<MockFile> fileIter = files.iterator();
    boolean result = true;
    while (fileIter.hasNext()) {
      MockFile file = fileIter.next();
      String filename = file.path.toString();
      if (!filename.startsWith(path)) continue;
      if (filename.length() <= path.length() || filename.charAt(path.length()) != '/') continue;
      if (file.cannotDelete) {
        result = false;
        continue;
      }
      assert !file.isDeleted;
      file.isDeleted = true;
      fileIter.remove();
    }
    return result;
  }

  private void findMatchingLocatedFiles(
      List<MockFile> files, String pathnameAsDir, Set<String> dirs, List<LocatedFileStatus> result)
      throws IOException {
    for (MockFile file: files) {
      String filename = file.path.toString();
      if (filename.startsWith(pathnameAsDir)) {
        String tail = filename.substring(pathnameAsDir.length());
        int nextSlash = tail.indexOf('/');
        if (nextSlash > 0) {
          dirs.add(tail.substring(0, nextSlash));
        } else {
          result.add(createLocatedStatus(file));
        }
      }
    }
  }

  @Override
  public void setWorkingDirectory(Path path) {
    workingDir = path;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) {
    statistics.incrementWriteOps(1);
    return false;
  }

  private FileStatus createStatus(MockFile file) {
    if (fileStatusMap.containsKey(file)) {
      return fileStatusMap.get(file);
    }
    FileStatus fileStatus = new FileStatus(file.length, false, 1, file.blockSize, 0, 0,
        FsPermission.createImmutable((short) 644), "owen", "group",
        file.path);
    fileStatusMap.put(file, fileStatus);
    return fileStatus;
  }

  private FileStatus createDirectory(Path dir) {
    return new FileStatus(0, true, 0, 0, 0, 0,
        FsPermission.createImmutable((short) 755), "owen", "group", dir);
  }

  private LocatedFileStatus createLocatedStatus(MockFile file) throws IOException {
    FileStatus fileStatus = createStatus(file);
    return new LocatedFileStatus(fileStatus,
        getFileBlockLocationsImpl(fileStatus, 0, fileStatus.getLen(), false));
  }

  private LocatedFileStatus createLocatedDirectory(Path dir) throws IOException {
    FileStatus fileStatus = createDirectory(dir);
    return new LocatedFileStatus(fileStatus,
        getFileBlockLocationsImpl(fileStatus, 0, fileStatus.getLen(), false));
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    statistics.incrementReadOps(1);
    checkAccess();
    path = path.makeQualified(this);
    String pathnameAsDir = path.toString() + "/";
    MockFile file = findFile(path);
    if (file != null) return createStatus(file);
    for (MockFile dir : files) {
      if (dir.path.toString().startsWith(pathnameAsDir)) {
        return createDirectory(path);
      }
    }
    for (MockFile dir : globalFiles) {
      if (dir.path.toString().startsWith(pathnameAsDir)) {
        return createDirectory(path);
      }
    }
    throw new FileNotFoundException("File " + path + " does not exist");
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus stat,
                                               long start, long len) throws IOException {
    return getFileBlockLocationsImpl(stat, start, len, true);
  }

  private BlockLocation[] getFileBlockLocationsImpl(final FileStatus stat, final long start,
      final long len,
      final boolean updateStats) throws IOException {
    if (updateStats) {
      statistics.incrementReadOps(1);
    }
    checkAccess();
    List<BlockLocation> result = new ArrayList<BlockLocation>();
    MockFile file = findFile(stat.getPath());
    if (file != null) {
      for(MockBlock block: file.blocks) {
        if (getOverlap(block.offset, block.length, start, len) > 0) {
          String[] topology = new String[block.hosts.length];
          for(int i=0; i < topology.length; ++i) {
            topology[i] = "/rack/ " + block.hosts[i];
          }
          result.add(new BlockLocation(block.hosts, block.hosts,
              topology, block.offset, block.length));
        }
      }
      return result.toArray(new BlockLocation[result.size()]);
    }
    return new BlockLocation[0];
  }
  

  /**
   * Compute the number of bytes that overlap between the two ranges.
   * @param offset1 start of range1
   * @param length1 length of range1
   * @param offset2 start of range2
   * @param length2 length of range2
   * @return the number of bytes in the overlap range
   */
  private static long getOverlap(long offset1, long length1, long offset2, long length2) {
    // c/p from OrcInputFormat
    long end1 = offset1 + length1;
    long end2 = offset2 + length2;
    if (end2 <= offset1 || end1 <= offset2) {
      return 0;
    } else {
      return Math.min(end1, end2) - Math.max(offset1, offset2);
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("mockFs{files:[");
    for(int i=0; i < files.size(); ++i) {
      if (i != 0) {
        buffer.append(", ");
      }
      buffer.append(files.get(i));
    }
    buffer.append("]}");
    return buffer.toString();
  }

  public static void addGlobalFile(MockFile mockFile) {
    globalFiles.add(mockFile);
  }

  public static void clearGlobalFiles() {
    globalFiles.clear();
  }


  public static class MockBlock {
    int offset;
    int length;
    final String[] hosts;

    public MockBlock(String... hosts) {
      this.hosts = hosts;
    }

    public void setOffset(int offset) {
      this.offset = offset;
    }

    public void setLength(int length) {
      this.length = length;
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("block{offset: ");
      buffer.append(offset);
      buffer.append(", length: ");
      buffer.append(length);
      buffer.append(", hosts: [");
      for(int i=0; i < hosts.length; i++) {
        if (i != 0) {
          buffer.append(", ");
        }
        buffer.append(hosts[i]);
      }
      buffer.append("]}");
      return buffer.toString();
    }
  }

  public static class MockFile {
    public final Path path;
    public int blockSize;
    public int length;
    public MockBlock[] blocks;
    public byte[] content;
    public boolean cannotDelete = false;
    // This is purely for testing convenience; has no bearing on FS operations such as list.
    public boolean isDeleted = false;

    public MockFile(String path, int blockSize, byte[] content,
                    MockBlock... blocks) {
      this.path = new Path(path);
      this.blockSize = blockSize;
      this.blocks = blocks;
      this.content = content;
      this.length = content.length;
      int offset = 0;
      for(MockBlock block: blocks) {
        block.offset = offset;
        block.length = Math.min(length - offset, blockSize);
        offset += block.length;
      }
    }

    @Override
    public int hashCode() {
      return path.hashCode() + 31 * length;
    }

    @Override
    public boolean equals(final Object obj) {
      if (!(obj instanceof MockFile)) { return false; }
      return ((MockFile) obj).path.equals(this.path) && ((MockFile) obj).length == this.length;
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("mockFile{path: ");
      buffer.append(path.toString());
      buffer.append(", blkSize: ");
      buffer.append(blockSize);
      buffer.append(", len: ");
      buffer.append(length);
      buffer.append(", blocks: [");
      for(int i=0; i < blocks.length; i++) {
        if (i != 0) {
          buffer.append(", ");
        }
        buffer.append(blocks[i]);
      }
      buffer.append("]}");
      return buffer.toString();
    }
  }

  static class MockInputStream extends FSInputStream {
    final MockFile file;
    int offset = 0;

    public MockInputStream(MockFile file) throws IOException {
      this.file = file;
    }

    @Override
    public void seek(long offset) throws IOException {
      this.offset = (int) offset;
    }

    @Override
    public long getPos() throws IOException {
      return offset;
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
      return false;
    }

    @Override
    public int read() throws IOException {
      if (offset < file.length) {
        return file.content[offset++] & 0xff;
      }
      return -1;
    }
  }

  public static class MockPath extends Path {
    private final FileSystem fs;
    public MockPath(FileSystem fs, String path) {
      super(path);
      this.fs = fs;
    }
    @Override
    public FileSystem getFileSystem(Configuration conf) {
      return fs;
    }
  }

  public static class MockOutputStream extends FSDataOutputStream {
    public final MockFile file;

    public MockOutputStream(MockFile file) throws IOException {
      super(new DataOutputBuffer(), null);
      this.file = file;
    }

    /**
     * Set the blocks and their location for the file.
     * Must be called after the stream is closed or the block length will be
     * wrong.
     * @param blocks the list of blocks
     */
    public void setBlocks(MockBlock... blocks) {
      file.blocks = blocks;
      int offset = 0;
      int i = 0;
      while (offset < file.length && i < blocks.length) {
        blocks[i].offset = offset;
        blocks[i].length = Math.min(file.length - offset, file.blockSize);
        offset += blocks[i].length;
        i += 1;
      }
    }

    @Override
    public void close() throws IOException {
      super.close();
      DataOutputBuffer buf = (DataOutputBuffer) getWrappedStream();
      file.length = buf.getLength();
      file.content = new byte[file.length];
      MockBlock block = new MockBlock("host1");
      block.setLength(file.length);
      setBlocks(block);
      System.arraycopy(buf.getData(), 0, file.content, 0, file.length);
    }

    @Override
    public String toString() {
      return "Out stream to " + file.toString();
    }
  }

  public void addFile(MockFile file) {
    files.add(file);
  }
}