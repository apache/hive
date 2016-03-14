/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hive.ql.io.FileFormatException;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

public class TestReaderImpl {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private final Path path = new Path("test-file.orc");
  private FSDataInputStream in;
  private int psLen;
  private ByteBuffer buffer;

  @Before
  public void setup() {
    in = null;
  }

  @Test
  public void testEnsureOrcFooterSmallTextFile() throws IOException {
    prepareTestCase("1".getBytes());
    thrown.expect(FileFormatException.class);
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testEnsureOrcFooterLargeTextFile() throws IOException {
    prepareTestCase("This is Some Text File".getBytes());
    thrown.expect(FileFormatException.class);
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testEnsureOrcFooter011ORCFile() throws IOException {
    prepareTestCase(composeContent(OrcFile.MAGIC, "FOOTER"));
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testEnsureOrcFooterCorrectORCFooter() throws IOException {
    prepareTestCase(composeContent("",OrcFile.MAGIC));
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  private void prepareTestCase(byte[] bytes) {
    buffer = ByteBuffer.wrap(bytes);
    psLen = buffer.get(bytes.length - 1) & 0xff;
    in = new FSDataInputStream(new SeekableByteArrayInputStream(bytes));
  }

  private byte[] composeContent(String headerStr, String footerStr) throws CharacterCodingException {
    ByteBuffer header = Text.encode(headerStr);
    ByteBuffer footer = Text.encode(footerStr);
    int headerLen = header.remaining();
    int footerLen = footer.remaining() + 1;

    ByteBuffer buf = ByteBuffer.allocate(headerLen + footerLen);

    buf.put(header);
    buf.put(footer);
    buf.put((byte) footerLen);
    return buf.array();
  }

  private static final class SeekableByteArrayInputStream extends ByteArrayInputStream
          implements Seekable, PositionedReadable {

    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public void seek(long pos) throws IOException {
      this.reset();
      this.skip(pos);
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
      long oldPos = getPos();
      int nread = -1;
      try {
        seek(position);
        nread = read(buffer, offset, length);
      } finally {
        seek(oldPos);
      }
      return nread;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
      int nread = 0;
      while (nread < length) {
        int nbytes = read(position + nread, buffer, offset + nread, length - nread);
        if (nbytes < 0) {
          throw new EOFException("End of file reached before reading fully.");
        }
        nread += nbytes;
      }
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }
  }
}
