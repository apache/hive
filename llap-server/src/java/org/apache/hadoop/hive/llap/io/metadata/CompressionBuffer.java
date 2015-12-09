/**
 *   Copyright 2014 Prasanth Jayachandran
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.io.metadata;

/**
 *
 */
public class CompressionBuffer {
  // stripe position within file
  private int stripePos;

  // row group position within stripe
  private int rowGroupPos;

  // start offset of compression buffer corresponding to above row group
  private long startOffset;

  // length of compression buffer (compressed or uncompressed length)
  private long length;

  // offset within compression buffer where the row group begins
  private int uncompressedOffset;

  // if uncompressedOffset is in a middle of integer encoding runs (RLE, Delta etc.), consume
  // these many values to reach beginning of the row group
  private int consume;

  // For run length byte encoding, record the number of bits within current byte to consume to
  // reach beginning of the row group. This is required for IS_PRESENT stream.
  private int consumeBits;

  // if last row group is set to true, it means the above row group spans compression buffer
  // boundary. Length will span two compression buffers.
  private boolean lastRowGroup;

  private CompressionBuffer(int sp, int rgp, long s, long len, int u, int c, int cb, boolean last) {
    this.stripePos = sp;
    this.rowGroupPos = rgp;
    this.startOffset = s;
    this.length = len;
    this.uncompressedOffset = u;
    this.consume = c;
    this.consumeBits = cb;
    this.lastRowGroup = last;
  }

  private static class Builder {
    private int stripePos;
    private int rowGroupPos;
    private long startOffset;
    private long length;
    private int offsetWithinBuffer;
    private int consume;
    private int consumeBits;
    private boolean lastRowGroup;

    public Builder setStripePosition(int stripePos) {
      this.stripePos = stripePos;
      return this;
    }

    public Builder setRowGroupPosition(int rowGroupPos) {
      this.rowGroupPos = rowGroupPos;
      return this;
    }

    public Builder setStartOffset(long startOffset) {
      this.startOffset = startOffset;
      return this;
    }

    public Builder setLength(long length) {
      this.length = length;
      return this;
    }

    public Builder setOffsetWithInBuffer(int offsetWithInBuffer) {
      this.offsetWithinBuffer = offsetWithInBuffer;
      return this;
    }

    public Builder consumeRuns(int consume) {
      this.consume = consume;
      return this;
    }

    public Builder consumeBits(int consumeBits) {
      this.consumeBits = consumeBits;
      return this;
    }

    public Builder setLastRowGroup(boolean lastRowGroup) {
      this.lastRowGroup = lastRowGroup;
      return this;
    }

    public CompressionBuffer build() {
      return new CompressionBuffer(stripePos, rowGroupPos, startOffset, length, offsetWithinBuffer,
          consume, consumeBits, lastRowGroup);
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
