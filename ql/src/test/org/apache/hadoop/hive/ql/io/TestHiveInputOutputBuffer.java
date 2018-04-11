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
package org.apache.hadoop.hive.ql.io;

import static org.junit.Assert.assertArrayEquals;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.junit.Test;

import junit.framework.TestCase;

/**
 * TestHiveInputOutputBuffer.
 *
 */
public class TestHiveInputOutputBuffer extends TestCase {

  private static final int numCases = 14; 
  
  private static final String asciiLine1 = "Foo 12345 moo";
  private static final String asciiLine2 = "Line two";
  private static final String asciiString = asciiLine1 + "\n" + asciiLine2 + "\r\n";

  public void testReadAndWrite() throws IOException {
    String testString = "test_hive_input_output_number_0";
    byte[] string_bytes = testString.getBytes();
    NonSyncDataInputBuffer inBuffer = new NonSyncDataInputBuffer();
    NonSyncDataOutputBuffer outBuffer = new NonSyncDataOutputBuffer();
    try {
      outBuffer.write(string_bytes);
      inBuffer.reset(outBuffer.getData(), 0, outBuffer.getLength());
      byte[] readBytes = new byte[string_bytes.length];
      inBuffer.read(readBytes);
      String readString = new String(readBytes);
      assertEquals("Field testReadAndWrite()", readString, testString);
    } finally {
      inBuffer.close();
      outBuffer.close();
    }
  }

  @SuppressWarnings("deprecation")
  private static void readJunk(NonSyncDataInputBuffer in, Random r, long seed, int iter) 
      throws IOException {
    r.setSeed(seed);
    for (int i = 0; i < iter; ++i) {
      switch (r.nextInt(numCases)) {
        case 0:
          assertEquals((byte)(r.nextInt() & 0xFF), in.readByte()); break;
        case 1:
          assertEquals((short)(r.nextInt() & 0xFFFF), in.readShort()); break;
        case 2:
          assertEquals(r.nextInt(), in.readInt()); break;
        case 3:
          assertEquals(r.nextLong(), in.readLong()); break;
        case 4:
          assertEquals(Double.doubleToLongBits(r.nextDouble()),
                       Double.doubleToLongBits(in.readDouble())); break;
        case 5:
          assertEquals(Float.floatToIntBits(r.nextFloat()),
                       Float.floatToIntBits(in.readFloat())); break;
        case 6:
          int len = r.nextInt(1024);
          // 1 (test #readFully(3)):
          final byte[] vb = new byte[len];
          r.nextBytes(vb);
          final byte[] b = new byte[len];
          in.readFully(b, 0, len);
          assertArrayEquals(vb, b);
          // 2 (test #read(3)):
          r.nextBytes(vb);
          in.read(b, 0, len);
          assertArrayEquals(vb, b);
          // 3 (test #readFully(1)):
          r.nextBytes(vb);
          in.readFully(b);
          assertArrayEquals(vb, b);
          break;
        case 7:
          assertEquals(r.nextBoolean(), in.readBoolean());
          break;
        case 8:
          assertEquals((char)r.nextInt(), in.readChar());
          break;
        case 9:
          int actualUB = in.readUnsignedByte();
          assertTrue(actualUB >= 0);
          assertTrue(actualUB <= 255);
          assertEquals(r.nextInt() & 0xFF, actualUB);
          break;
        case 10:
          int actualUS = in.readUnsignedShort();
          assertTrue(actualUS >= 0);
          assertTrue(actualUS <= 0xFFFF);
          assertEquals(r.nextInt() & 0xFFFF, actualUS);
          break;
        case 11:
          String expectedString1 = composeString(1024, r);
          assertEquals(expectedString1, in.readUTF());
          String expectedString2 = composeString(1024, r);
          assertEquals(expectedString2, NonSyncDataInputBuffer.readUTF(in));
          break;
        case 12:
          assertEquals(asciiLine1, in.readLine());
          assertEquals(asciiLine2, in.readLine());
          break;
        case 13:
          in.skipBytes(8);
          r.nextLong(); // ignore
          assertEquals(r.nextLong(), in.readLong());
          break;
      }
    }
  }
  
  private static void writeJunk(DataOutput out, Random r, long seed, int iter)
      throws IOException  {
    r.setSeed(seed);
    for (int i = 0; i < iter; ++i) {
      switch (r.nextInt(numCases)) {
        case 0: out.writeByte(r.nextInt()); break;
        case 1: out.writeShort((short)(r.nextInt() & 0xFFFF)); break;
        case 2: out.writeInt(r.nextInt()); break;
        case 3: out.writeLong(r.nextLong()); break;
        case 4: out.writeDouble(r.nextDouble()); break;
        case 5: out.writeFloat(r.nextFloat()); break;
        case 6:
          byte[] b = new byte[r.nextInt(1024)];
          // 1:
          r.nextBytes(b);
          out.write(b);
          // 2:
          r.nextBytes(b);
          out.write(b);
          // 3:
          r.nextBytes(b);
          out.write(b);
          break;
        case 7:
          out.writeBoolean(r.nextBoolean());
          break;
        case 8:
          out.writeChar((char)r.nextInt());
          break;
        case 9:
          out.writeByte((byte)r.nextInt());
          break;
        case 10:
          out.writeShort((short)r.nextInt());
          break;
        case 11:
          String string = composeString(1024, r);
          out.writeUTF(string);
          String string2 = composeString(1024, r);
          out.writeUTF(string2);
          break;
        case 12:
          byte[] bb = asciiString.getBytes("UTF-8");
          out.write(bb);
          break;
        case 13:
          out.writeLong(r.nextLong());
          out.writeLong(r.nextLong());
          break;
      }
    }
  }

  private static String composeString(int len, Random r) {
    char[] cc = new char[len];
    char ch;
    for (int i = 0; i<len; i++) {
      do {
        ch = (char)r.nextInt();
      } while (!Character.isDefined(ch) 
          || Character.isHighSurrogate(ch)
          || Character.isLowSurrogate(ch));
      cc[i] = ch;
    }
    return new String(cc);
  }
  
  /**
   * Tests methods of {@link NonSyncDataInputBuffer}.
   * @throws IOException
   */
  @Test
  public void testBaseBuffers() throws IOException {
    NonSyncDataOutputBuffer dob = new NonSyncDataOutputBuffer();
    final Random r = new Random();
    final long seed = 0x0123456789ABCDEFL; // hardcoded for reproducibility.
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    
    writeJunk(dob, r, seed, 1000);
    NonSyncDataInputBuffer dib = new NonSyncDataInputBuffer();
    dib.reset(dob.getData(), 0, dob.getLength());
    assertEquals(0, dib.getPosition());
    assertEquals(dob.getLength(), dib.getLength());
    readJunk(dib, r, seed, 1000);

    dob.reset();
    writeJunk(dob, r, seed, 1000);
    dib.reset(dob.getData(), dob.getLength());
    assertEquals(0, dib.getPosition());
    assertEquals(dob.getLength(), dib.getLength());
    readJunk(dib, r, seed, 1000);
  }
  
}
