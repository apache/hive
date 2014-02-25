/**
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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;

/**
 * A writer that performs light weight compression over sequence of integers.
 * <p>
 * There are four types of lightweight integer compression
 * <ul>
 * <li>SHORT_REPEAT</li>
 * <li>DIRECT</li>
 * <li>PATCHED_BASE</li>
 * <li>DELTA</li>
 * </ul>
 * </p>
 * The description and format for these types are as below:
 * <p>
 * <b>SHORT_REPEAT:</b> Used for short repeated integer sequences.
 * <ul>
 * <li>1 byte header
 * <ul>
 * <li>2 bits for encoding type</li>
 * <li>3 bits for bytes required for repeating value</li>
 * <li>3 bits for repeat count (MIN_REPEAT + run length)</li>
 * </ul>
 * </li>
 * <li>Blob - repeat value (fixed bytes)</li>
 * </ul>
 * </p>
 * <p>
 * <b>DIRECT:</b> Used for random integer sequences whose number of bit
 * requirement doesn't vary a lot.
 * <ul>
 * <li>2 bytes header
 * <ul>
 * 1st byte
 * <li>2 bits for encoding type</li>
 * <li>5 bits for fixed bit width of values in blob</li>
 * <li>1 bit for storing MSB of run length</li>
 * </ul>
 * <ul>
 * 2nd byte
 * <li>8 bits for lower run length bits</li>
 * </ul>
 * </li>
 * <li>Blob - stores the direct values using fixed bit width. The length of the
 * data blob is (fixed width * run length) bits long</li>
 * </ul>
 * </p>
 * <p>
 * <b>PATCHED_BASE:</b> Used for random integer sequences whose number of bit
 * requirement varies beyond a threshold.
 * <ul>
 * <li>4 bytes header
 * <ul>
 * 1st byte
 * <li>2 bits for encoding type</li>
 * <li>5 bits for fixed bit width of values in blob</li>
 * <li>1 bit for storing MSB of run length</li>
 * </ul>
 * <ul>
 * 2nd byte
 * <li>8 bits for lower run length bits</li>
 * </ul>
 * <ul>
 * 3rd byte
 * <li>3 bits for bytes required to encode base value</li>
 * <li>5 bits for patch width</li>
 * </ul>
 * <ul>
 * 4th byte
 * <li>3 bits for patch gap width</li>
 * <li>5 bits for patch length</li>
 * </ul>
 * </li>
 * <li>Base value - Stored using fixed number of bytes. If MSB is set, base
 * value is negative else positive. Length of base value is (base width * 8)
 * bits.</li>
 * <li>Data blob - Base reduced values as stored using fixed bit width. Length
 * of data blob is (fixed width * run length) bits.</li>
 * <li>Patch blob - Patch blob is a list of gap and patch value. Each entry in
 * the patch list is (patch width + patch gap width) bits long. Gap between the
 * subsequent elements to be patched are stored in upper part of entry whereas
 * patch values are stored in lower part of entry. Length of patch blob is
 * ((patch width + patch gap width) * patch length) bits.</li>
 * </ul>
 * </p>
 * <p>
 * <b>DELTA</b> Used for monotonically increasing or decreasing sequences,
 * sequences with fixed delta values or long repeated sequences.
 * <ul>
 * <li>2 bytes header
 * <ul>
 * 1st byte
 * <li>2 bits for encoding type</li>
 * <li>5 bits for fixed bit width of values in blob</li>
 * <li>1 bit for storing MSB of run length</li>
 * </ul>
 * <ul>
 * 2nd byte
 * <li>8 bits for lower run length bits</li>
 * </ul>
 * </li>
 * <li>Base value - encoded as varint</li>
 * <li>Delta base - encoded as varint</li>
 * <li>Delta blob - only positive values. monotonicity and orderness are decided
 * based on the sign of the base value and delta base</li>
 * </ul>
 * </p>
 */
class RunLengthIntegerWriterV2 implements IntegerWriter {

  public enum EncodingType {
    SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA
  }

  static final int MAX_SCOPE = 512;
  static final int MIN_REPEAT = 3;
  private static final int MAX_SHORT_REPEAT_LENGTH = 10;
  private long prevDelta = 0;
  private int fixedRunLength = 0;
  private int variableRunLength = 0;
  private final long[] literals = new long[MAX_SCOPE];
  private final PositionedOutputStream output;
  private final boolean signed;
  private EncodingType encoding;
  private int numLiterals;
  private long[] zigzagLiterals;
  private long[] baseRedLiterals;
  private long[] adjDeltas;
  private long fixedDelta;
  private int zzBits90p;
  private int zzBits100p;
  private int brBits95p;
  private int brBits100p;
  private int bitsDeltaMax;
  private int patchWidth;
  private int patchGapWidth;
  private int patchLength;
  private long[] gapVsPatchList;
  private long min;
  private boolean isFixedDelta;

  RunLengthIntegerWriterV2(PositionedOutputStream output, boolean signed) {
    this.output = output;
    this.signed = signed;
    clear();
  }

  private void writeValues() throws IOException {
    if (numLiterals != 0) {

      if (encoding.equals(EncodingType.SHORT_REPEAT)) {
        writeShortRepeatValues();
      } else if (encoding.equals(EncodingType.DIRECT)) {
        writeDirectValues();
      } else if (encoding.equals(EncodingType.PATCHED_BASE)) {
        writePatchedBaseValues();
      } else {
        writeDeltaValues();
      }

      // clear all the variables
      clear();
    }
  }

  private void writeDeltaValues() throws IOException {
    int len = 0;
    int fb = bitsDeltaMax;
    int efb = 0;

    if (isFixedDelta) {
      // if fixed run length is greater than threshold then it will be fixed
      // delta sequence with delta value 0 else fixed delta sequence with
      // non-zero delta value
      if (fixedRunLength > MIN_REPEAT) {
        // ex. sequence: 2 2 2 2 2 2 2 2
        len = fixedRunLength - 1;
        fixedRunLength = 0;
      } else {
        // ex. sequence: 4 6 8 10 12 14 16
        len = variableRunLength - 1;
        variableRunLength = 0;
      }
    } else {
      // fixed width 0 is used for long repeating values.
      // sequences that require only 1 bit to encode will have an additional bit
      if (fb == 1) {
        fb = 2;
      }
      efb = SerializationUtils.encodeBitWidth(fb);
      efb = efb << 1;
      len = variableRunLength - 1;
      variableRunLength = 0;
    }

    // extract the 9th bit of run length
    int tailBits = (len & 0x100) >>> 8;

    // create first byte of the header
    int headerFirstByte = getOpcode() | efb | tailBits;

    // second byte of the header stores the remaining 8 bits of runlength
    int headerSecondByte = len & 0xff;

    // write header
    output.write(headerFirstByte);
    output.write(headerSecondByte);

    // store the first value from zigzag literal array
    if (signed) {
      SerializationUtils.writeVslong(output, literals[0]);
    } else {
      SerializationUtils.writeVulong(output, literals[0]);
    }

    if (isFixedDelta) {
      // if delta is fixed then we don't need to store delta blob
      SerializationUtils.writeVslong(output, fixedDelta);
    } else {
      // store the first value as delta value using zigzag encoding
      SerializationUtils.writeVslong(output, adjDeltas[0]);
      // adjacent delta values are bit packed
      SerializationUtils.writeInts(adjDeltas, 1, adjDeltas.length - 1, fb,
          output);
    }
  }

  private void writePatchedBaseValues() throws IOException {

    // write the number of fixed bits required in next 5 bits
    int fb = brBits95p;
    int efb = SerializationUtils.encodeBitWidth(fb) << 1;

    // adjust variable run length, they are one off
    variableRunLength -= 1;

    // extract the 9th bit of run length
    int tailBits = (variableRunLength & 0x100) >>> 8;

    // create first byte of the header
    int headerFirstByte = getOpcode() | efb | tailBits;

    // second byte of the header stores the remaining 8 bits of runlength
    int headerSecondByte = variableRunLength & 0xff;

    // if the min value is negative toggle the sign
    boolean isNegative = min < 0 ? true : false;
    if (isNegative) {
      min = -min;
    }

    // find the number of bytes required for base and shift it by 5 bits
    // to accommodate patch width. The additional bit is used to store the sign
    // of the base value.
    int baseWidth = SerializationUtils.findClosestNumBits(min) + 1;
    int baseBytes = baseWidth % 8 == 0 ? baseWidth / 8 : (baseWidth / 8) + 1;
    int bb = (baseBytes - 1) << 5;

    // if the base value is negative then set MSB to 1
    if (isNegative) {
      min |= (1L << ((baseBytes * 8) - 1));
    }

    // third byte contains 3 bits for number of bytes occupied by base
    // and 5 bits for patchWidth
    int headerThirdByte = bb | SerializationUtils.encodeBitWidth(patchWidth);

    // fourth byte contains 3 bits for page gap width and 5 bits for
    // patch length
    int headerFourthByte = (patchGapWidth - 1) << 5 | patchLength;

    // write header
    output.write(headerFirstByte);
    output.write(headerSecondByte);
    output.write(headerThirdByte);
    output.write(headerFourthByte);

    // write the base value using fixed bytes in big endian order
    for(int i = baseBytes - 1; i >= 0; i--) {
      byte b = (byte) ((min >>> (i * 8)) & 0xff);
      output.write(b);
    }

    // base reduced literals are bit packed
    int closestFixedBits = SerializationUtils.getClosestFixedBits(brBits95p);
    SerializationUtils.writeInts(baseRedLiterals, 0, baseRedLiterals.length,
        closestFixedBits, output);

    // write patch list
    closestFixedBits = SerializationUtils.getClosestFixedBits(patchGapWidth
        + patchWidth);
    SerializationUtils.writeInts(gapVsPatchList, 0, gapVsPatchList.length,
        closestFixedBits, output);

    // reset run length
    variableRunLength = 0;
  }

  /**
   * Store the opcode in 2 MSB bits
   * @return opcode
   */
  private int getOpcode() {
    return encoding.ordinal() << 6;
  }

  private void writeDirectValues() throws IOException {

    // write the number of fixed bits required in next 5 bits
    int efb = SerializationUtils.encodeBitWidth(zzBits100p) << 1;

    // adjust variable run length
    variableRunLength -= 1;

    // extract the 9th bit of run length
    int tailBits = (variableRunLength & 0x100) >>> 8;

    // create first byte of the header
    int headerFirstByte = getOpcode() | efb | tailBits;

    // second byte of the header stores the remaining 8 bits of runlength
    int headerSecondByte = variableRunLength & 0xff;

    // write header
    output.write(headerFirstByte);
    output.write(headerSecondByte);

    // bit packing the zigzag encoded literals
    SerializationUtils.writeInts(zigzagLiterals, 0, zigzagLiterals.length,
        zzBits100p, output);

    // reset run length
    variableRunLength = 0;
  }

  private void writeShortRepeatValues() throws IOException {
    // get the value that is repeating, compute the bits and bytes required
    long repeatVal = 0;
    if (signed) {
      repeatVal = SerializationUtils.zigzagEncode(literals[0]);
    } else {
      repeatVal = literals[0];
    }

    int numBitsRepeatVal = SerializationUtils.findClosestNumBits(repeatVal);
    int numBytesRepeatVal = numBitsRepeatVal % 8 == 0 ? numBitsRepeatVal >>> 3
        : (numBitsRepeatVal >>> 3) + 1;

    // write encoding type in top 2 bits
    int header = getOpcode();

    // write the number of bytes required for the value
    header |= ((numBytesRepeatVal - 1) << 3);

    // write the run length
    fixedRunLength -= MIN_REPEAT;
    header |= fixedRunLength;

    // write the header
    output.write(header);

    // write the repeating value in big endian byte order
    for(int i = numBytesRepeatVal - 1; i >= 0; i--) {
      int b = (int) ((repeatVal >>> (i * 8)) & 0xff);
      output.write(b);
    }

    fixedRunLength = 0;
  }

  private void determineEncoding() {
    // used for direct encoding
    zigzagLiterals = new long[numLiterals];

    // used for patched base encoding
    baseRedLiterals = new long[numLiterals];

    // used for delta encoding
    adjDeltas = new long[numLiterals - 1];

    int idx = 0;

    // for identifying monotonic sequences
    boolean isIncreasing = false;
    int increasingCount = 1;
    boolean isDecreasing = false;
    int decreasingCount = 1;

    // for identifying type of delta encoding
    min = literals[0];
    long max = literals[0];
    isFixedDelta = true;
    long currDelta = 0;

    min = literals[0];
    long deltaMax = 0;

    // populate all variables to identify the encoding type
    if (numLiterals >= 1) {
      currDelta = literals[1] - literals[0];
      for(int i = 0; i < numLiterals; i++) {
        if (i > 0 && literals[i] >= max) {
          max = literals[i];
          increasingCount++;
        }

        if (i > 0 && literals[i] <= min) {
          min = literals[i];
          decreasingCount++;
        }

        // if delta doesn't changes then mark it as fixed delta
        if (i > 0 && isFixedDelta) {
          if (literals[i] - literals[i - 1] != currDelta) {
            isFixedDelta = false;
          }

          fixedDelta = currDelta;
        }

        // populate zigzag encoded literals
        long zzEncVal = 0;
        if (signed) {
          zzEncVal = SerializationUtils.zigzagEncode(literals[i]);
        } else {
          zzEncVal = literals[i];
        }
        zigzagLiterals[idx] = zzEncVal;
        idx++;

        // max delta value is required for computing the fixed bits
        // required for delta blob in delta encoding
        if (i > 0) {
          if (i == 1) {
            // first value preserve the sign
            adjDeltas[i - 1] = literals[i] - literals[i - 1];
          } else {
            adjDeltas[i - 1] = Math.abs(literals[i] - literals[i - 1]);
            if (adjDeltas[i - 1] > deltaMax) {
              deltaMax = adjDeltas[i - 1];
            }
          }
        }
      }

      // stores the number of bits required for packing delta blob in
      // delta encoding
      bitsDeltaMax = SerializationUtils.findClosestNumBits(deltaMax);

      // if decreasing count equals total number of literals then the
      // sequence is monotonically decreasing
      if (increasingCount == 1 && decreasingCount == numLiterals) {
        isDecreasing = true;
      }

      // if increasing count equals total number of literals then the
      // sequence is monotonically increasing
      if (decreasingCount == 1 && increasingCount == numLiterals) {
        isIncreasing = true;
      }
    }

    // if the sequence is both increasing and decreasing then it is not
    // monotonic
    if (isDecreasing && isIncreasing) {
      isDecreasing = false;
      isIncreasing = false;
    }

    // fixed delta condition
    if (isIncreasing == false && isDecreasing == false && isFixedDelta == true) {
      encoding = EncodingType.DELTA;
      return;
    }

    // monotonic condition
    if (isIncreasing || isDecreasing) {
      encoding = EncodingType.DELTA;
      return;
    }

    // percentile values are computed for the zigzag encoded values. if the
    // number of bit requirement between 90th and 100th percentile varies
    // beyond a threshold then we need to patch the values. if the variation
    // is not significant then we can use direct or delta encoding

    double p = 0.9;
    zzBits90p = SerializationUtils.percentileBits(zigzagLiterals, p);

    p = 1.0;
    zzBits100p = SerializationUtils.percentileBits(zigzagLiterals, p);

    int diffBitsLH = zzBits100p - zzBits90p;

    // if the difference between 90th percentile and 100th percentile fixed
    // bits is > 1 then we need patch the values
    if (isIncreasing == false && isDecreasing == false && diffBitsLH > 1
        && isFixedDelta == false) {
      // patching is done only on base reduced values.
      // remove base from literals
      for(int i = 0; i < zigzagLiterals.length; i++) {
        baseRedLiterals[i] = literals[i] - min;
      }

      // 95th percentile width is used to determine max allowed value
      // after which patching will be done
      p = 0.95;
      brBits95p = SerializationUtils.percentileBits(baseRedLiterals, p);

      // 100th percentile is used to compute the max patch width
      p = 1.0;
      brBits100p = SerializationUtils.percentileBits(baseRedLiterals, p);

      // after base reducing the values, if the difference in bits between
      // 95th percentile and 100th percentile value is zero then there
      // is no point in patching the values, in which case we will
      // fallback to DIRECT encoding.
      // The decision to use patched base was based on zigzag values, but the
      // actual patching is done on base reduced literals.
      if ((brBits100p - brBits95p) != 0) {
        encoding = EncodingType.PATCHED_BASE;
        preparePatchedBlob();
        return;
      } else {
        encoding = EncodingType.DIRECT;
        return;
      }
    }

    // if difference in bits between 95th percentile and 100th percentile is
    // 0, then patch length will become 0. Hence we will fallback to direct
    if (isIncreasing == false && isDecreasing == false && diffBitsLH <= 1
        && isFixedDelta == false) {
      encoding = EncodingType.DIRECT;
      return;
    }

    // this should not happen
    if (encoding == null) {
      throw new RuntimeException("Integer encoding cannot be determined.");
    }
  }

  private void preparePatchedBlob() {
    // mask will be max value beyond which patch will be generated
    long mask = (1L << brBits95p) - 1;

    // since we are considering only 95 percentile, the size of gap and
    // patch array can contain only be 5% values
    patchLength = (int) Math.ceil((baseRedLiterals.length * 0.05));

    int[] gapList = new int[patchLength];
    long[] patchList = new long[patchLength];

    // #bit for patch
    patchWidth = brBits100p - brBits95p;
    patchWidth = SerializationUtils.getClosestFixedBits(patchWidth);

    // if patch bit requirement is 64 then it will not possible to pack
    // gap and patch together in a long. To make sure gap and patch can be
    // packed together adjust the patch width
    if (patchWidth == 64) {
      patchWidth = 56;
      brBits95p = 8;
      mask = (1L << brBits95p) - 1;
    }

    int gapIdx = 0;
    int patchIdx = 0;
    int prev = 0;
    int gap = 0;
    int maxGap = 0;

    for(int i = 0; i < baseRedLiterals.length; i++) {
      // if value is above mask then create the patch and record the gap
      if (baseRedLiterals[i] > mask) {
        gap = i - prev;
        if (gap > maxGap) {
          maxGap = gap;
        }

        // gaps are relative, so store the previous patched value index
        prev = i;
        gapList[gapIdx++] = gap;

        // extract the most significant bits that are over mask bits
        long patch = baseRedLiterals[i] >>> brBits95p;
        patchList[patchIdx++] = patch;

        // strip off the MSB to enable safe bit packing
        baseRedLiterals[i] &= mask;
      }
    }

    // adjust the patch length to number of entries in gap list
    patchLength = gapIdx;

    // if the element to be patched is the first and only element then
    // max gap will be 0, but to store the gap as 0 we need atleast 1 bit
    if (maxGap == 0 && patchLength != 0) {
      patchGapWidth = 1;
    } else {
      patchGapWidth = SerializationUtils.findClosestNumBits(maxGap);
    }

    // special case: if the patch gap width is greater than 256, then
    // we need 9 bits to encode the gap width. But we only have 3 bits in
    // header to record the gap width. To deal with this case, we will save
    // two entries in patch list in the following way
    // 256 gap width => 0 for patch value
    // actual gap - 256 => actual patch value
    // We will do the same for gap width = 511. If the element to be patched is
    // the last element in the scope then gap width will be 511. In this case we
    // will have 3 entries in the patch list in the following way
    // 255 gap width => 0 for patch value
    // 255 gap width => 0 for patch value
    // 1 gap width => actual patch value
    if (patchGapWidth > 8) {
      patchGapWidth = 8;
      // for gap = 511, we need two additional entries in patch list
      if (maxGap == 511) {
        patchLength += 2;
      } else {
        patchLength += 1;
      }
    }

    // create gap vs patch list
    gapIdx = 0;
    patchIdx = 0;
    gapVsPatchList = new long[patchLength];
    for(int i = 0; i < patchLength; i++) {
      long g = gapList[gapIdx++];
      long p = patchList[patchIdx++];
      while (g > 255) {
        gapVsPatchList[i++] = (255L << patchWidth);
        g -= 255;
      }

      // store patch value in LSBs and gap in MSBs
      gapVsPatchList[i] = (g << patchWidth) | p;
    }
  }

  /**
   * clears all the variables
   */
  private void clear() {
    numLiterals = 0;
    encoding = null;
    prevDelta = 0;
    zigzagLiterals = null;
    baseRedLiterals = null;
    adjDeltas = null;
    fixedDelta = 0;
    zzBits90p = 0;
    zzBits100p = 0;
    brBits95p = 0;
    brBits100p = 0;
    bitsDeltaMax = 0;
    patchGapWidth = 0;
    patchLength = 0;
    patchWidth = 0;
    gapVsPatchList = null;
    min = 0;
    isFixedDelta = false;
  }

  @Override
  public void flush() throws IOException {
    if (numLiterals != 0) {
      if (variableRunLength != 0) {
        determineEncoding();
        writeValues();
      } else if (fixedRunLength != 0) {
        if (fixedRunLength < MIN_REPEAT) {
          variableRunLength = fixedRunLength;
          fixedRunLength = 0;
          determineEncoding();
          writeValues();
        } else if (fixedRunLength >= MIN_REPEAT
            && fixedRunLength <= MAX_SHORT_REPEAT_LENGTH) {
          encoding = EncodingType.SHORT_REPEAT;
          writeValues();
        } else {
          encoding = EncodingType.DELTA;
          isFixedDelta = true;
          writeValues();
        }
      }
    }
    output.flush();
  }

  @Override
  public void write(long val) throws IOException {
    if (numLiterals == 0) {
      initializeLiterals(val);
    } else {
      if (numLiterals == 1) {
        prevDelta = val - literals[0];
        literals[numLiterals++] = val;
        // if both values are same count as fixed run else variable run
        if (val == literals[0]) {
          fixedRunLength = 2;
          variableRunLength = 0;
        } else {
          fixedRunLength = 0;
          variableRunLength = 2;
        }
      } else {
        long currentDelta = val - literals[numLiterals - 1];
        if (prevDelta == 0 && currentDelta == 0) {
          // fixed delta run

          literals[numLiterals++] = val;

          // if variable run is non-zero then we are seeing repeating
          // values at the end of variable run in which case keep
          // updating variable and fixed runs
          if (variableRunLength > 0) {
            fixedRunLength = 2;
          }
          fixedRunLength += 1;

          // if fixed run met the minimum condition and if variable
          // run is non-zero then flush the variable run and shift the
          // tail fixed runs to start of the buffer
          if (fixedRunLength >= MIN_REPEAT && variableRunLength > 0) {
            numLiterals -= MIN_REPEAT;
            variableRunLength -= MIN_REPEAT - 1;
            // copy the tail fixed runs
            long[] tailVals = new long[MIN_REPEAT];
            System.arraycopy(literals, numLiterals, tailVals, 0, MIN_REPEAT);

            // determine variable encoding and flush values
            determineEncoding();
            writeValues();

            // shift tail fixed runs to beginning of the buffer
            for(long l : tailVals) {
              literals[numLiterals++] = l;
            }
          }

          // if fixed runs reached max repeat length then write values
          if (fixedRunLength == MAX_SCOPE) {
            determineEncoding();
            writeValues();
          }
        } else {
          // variable delta run

          // if fixed run length is non-zero and if it satisfies the
          // short repeat conditions then write the values as short repeats
          // else use delta encoding
          if (fixedRunLength >= MIN_REPEAT) {
            if (fixedRunLength <= MAX_SHORT_REPEAT_LENGTH) {
              encoding = EncodingType.SHORT_REPEAT;
              writeValues();
            } else {
              encoding = EncodingType.DELTA;
              isFixedDelta = true;
              writeValues();
            }
          }

          // if fixed run length is <MIN_REPEAT and current value is
          // different from previous then treat it as variable run
          if (fixedRunLength > 0 && fixedRunLength < MIN_REPEAT) {
            if (val != literals[numLiterals - 1]) {
              variableRunLength = fixedRunLength;
              fixedRunLength = 0;
            }
          }

          // after writing values re-initialize the variables
          if (numLiterals == 0) {
            initializeLiterals(val);
          } else {
            // keep updating variable run lengths
            prevDelta = val - literals[numLiterals - 1];
            literals[numLiterals++] = val;
            variableRunLength += 1;

            // if variable run length reach the max scope, write it
            if (variableRunLength == MAX_SCOPE) {
              determineEncoding();
              writeValues();
            }
          }
        }
      }
    }
  }

  private void initializeLiterals(long val) {
    literals[numLiterals++] = val;
    fixedRunLength = 1;
    variableRunLength = 1;
  }

  @Override
  public void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(numLiterals);
  }
}
