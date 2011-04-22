package org.apache.hadoop.hive.cassandra.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collection;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.hadoop.io.Writable;

public class HiveIColumn implements IColumn, Writable {

  private byte[] name;
  private byte[] value;
  private long timestamp;

  public HiveIColumn() {

  }

  @Override
  public ByteBuffer name() {
    return ByteBuffer.wrap(name);
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public ByteBuffer value() {
    return ByteBuffer.wrap(value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    name = new byte[in.readInt()];
    in.readFully(name);

    value = new byte[in.readInt()];
    in.readFully(value);

    timestamp = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(name.length);
    out.write(name);

    out.writeInt(value.length);
    out.write(value);

    out.writeLong(timestamp);
  }

  // bean patterns

  public byte[] getName() {
    return name;
  }

  public void setName(byte[] name) {
    this.name = name;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("HiveIColumn[");
    sb.append("name " + new String(this.name) + " ");
    sb.append("value " + new String(this.value) + " ");
    sb.append("timestamp " + this.timestamp + " ");
    return sb.toString();
  }

  // not needed for current integration

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addColumn(IColumn arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IColumn diff(IColumn arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLocalDeletionTime() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getMarkedForDeleteAt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(AbstractType arg0) {
    throw new UnsupportedOperationException();
  }


  @Override
  public Collection<IColumn> getSubColumns() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isMarkedForDelete() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long mostRecentLiveChangeAt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int serializedSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDigest(MessageDigest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IColumn getSubColumn(ByteBuffer arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isLive() {
    throw new UnsupportedOperationException();
  }

  @Override
  public IColumn reconcile(IColumn arg0) {
    throw new UnsupportedOperationException();
  }

  public IColumn localCopy(ColumnFamilyStore arg0) {
    throw new UnsupportedOperationException();
  }

  public int serializationFlags() {
    throw new UnsupportedOperationException();
  }

  public void validateFields(CFMetaData arg0) throws MarshalException {
    throw new UnsupportedOperationException();
  }
}
