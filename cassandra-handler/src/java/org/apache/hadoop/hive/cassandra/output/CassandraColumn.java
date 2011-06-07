package org.apache.hadoop.hive.cassandra.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This represents a cassandra column.
 *
 */
public class CassandraColumn implements Writable{

  private String columnFamily;
  private long timeStamp;
  private byte [] column;
  private byte [] value;

  @Override
  public void readFields(DataInput din) throws IOException {
    columnFamily = din.readUTF();
    timeStamp = din.readLong();
    int clength= din.readInt();
    column = new byte[clength];
    din.readFully(column, 0, clength);
    int vlength = din.readInt();
    value = new byte [vlength ];
    din.readFully( value, 0 , vlength);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(columnFamily);
    out.writeLong(timeStamp);
    out.writeInt( column.length );
    out.write(column);
    out.writeInt( value.length );
    out.write(value);
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public void setColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily;
  }

  public byte[] getColumn() {
    return column;
  }

  public void setColumn(byte[] column) {
    this.column = column;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append( "cf:"+ this.columnFamily);
    sb.append( "column:"+ new String (this.column));
    sb.append( "value:"+ new String (this.value));
    return sb.toString();
  }
}