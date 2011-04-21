package org.apache.hadoop.hive.cassandra.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.Writable;

public class CassandraPut implements Writable{

  private ByteBuffer key;
  //private ConsistencyLevel level;
  private List<CassandraColumn> columns;

  public CassandraPut(){
    columns = new ArrayList<CassandraColumn>();
  }

  public CassandraPut(ByteBuffer key){
    this();
    this.key = key;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int keyLen = in.readInt();
    byte[] keyBytes = new byte[keyLen];
    in.readFully(keyBytes);
    key = ByteBuffer.wrap(keyBytes);
    int ilevel = in.readInt();
    int cols = in.readInt();
    for (int i =0;i<cols;i++){
      CassandraColumn cc = new CassandraColumn();
      cc.readFields(in);
      columns.add(cc);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(key.remaining());
    out.write(ByteBufferUtil.getArray(key));
    out.writeInt(1);
    out.writeInt(columns.size());
    for (CassandraColumn c: columns){
      c.write(out);
    }
  }

  public ByteBuffer getKey() {
    return key;
  }

  public void setKey(ByteBuffer key) {
    this.key = key;
  }

  /*
  public ConsistencyLevel getLevel() {
    return level;
  }

  public void setLevel(ConsistencyLevel level) {
    this.level = level;
  }
*/

  public List<CassandraColumn> getColumns() {
    return columns;
  }

  public void setColumns(List<CassandraColumn> columns) {
    this.columns = columns;
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("key: ");
    sb.append(this.key);
    for (CassandraColumn col:this.columns){
      sb.append( "column : [" );
      sb.append( col.toString() );
      sb.append( "]" );
    }
    return sb.toString();
  }
}