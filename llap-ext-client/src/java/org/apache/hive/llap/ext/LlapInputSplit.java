package org.apache.hive.llap.ext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InputSplitWithLocationInfo;
import org.apache.hadoop.mapred.SplitLocationInfo;


public class LlapInputSplit<V extends WritableComparable> implements InputSplitWithLocationInfo {
  InputSplitWithLocationInfo nativeSplit;
  String inputFormatClassName;

  public LlapInputSplit() {
  }

  public LlapInputSplit(InputSplitWithLocationInfo nativeSplit, String inputFormatClassName) {
    this.nativeSplit = nativeSplit;
    this.inputFormatClassName = inputFormatClassName;
  }

  @Override
  public long getLength() throws IOException {
    return nativeSplit.getLength();
  }

  @Override
  public String[] getLocations() throws IOException {
    return nativeSplit.getLocations();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(inputFormatClassName);
    out.writeUTF(nativeSplit.getClass().getName());
    nativeSplit.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    inputFormatClassName = in.readUTF();
    String splitClass = in.readUTF();
    try {
      nativeSplit = (InputSplitWithLocationInfo)Class.forName(splitClass).newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
    nativeSplit.readFields(in);
  }

  @Override
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return nativeSplit.getLocationInfo();
  }

  public InputSplit getSplit() {
    return nativeSplit;
  }

  public InputFormat<NullWritable, V> getInputFormat() throws IOException {
    try {
      return (InputFormat<NullWritable, V>) Class.forName(inputFormatClassName)
          .newInstance();
    } catch(Exception e) {
      throw new IOException(e);
    }
  }
}
