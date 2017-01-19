package org.apache.hadoop.hive.llap.io.encoded;

import java.io.IOException;

import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.ReaderWithOffsets;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

@SuppressWarnings("rawtypes") class PassThruOffsetReader implements ReaderWithOffsets {
  protected final RecordReader sourceReader;
  protected final Object key;
  protected final Writable value;

  PassThruOffsetReader(RecordReader sourceReader) {
    this.sourceReader = sourceReader;
    key = sourceReader.createKey();
    value = (Writable)sourceReader.createValue();
  }

  @Override
  public boolean next() throws IOException {
    return sourceReader.next(key, value);
  }

  @Override
  public Writable getCurrentRow() {
    return value;
  }

  @Override
  public void close() throws IOException {
    sourceReader.close();
  }

  @Override
  public long getCurrentRowStartOffset() {
    return -1;
  }

  @Override
  public long getCurrentRowEndOffset() {
    return -1;
  }

  @Override
  public boolean hasOffsets() {
    return false;
  }
}