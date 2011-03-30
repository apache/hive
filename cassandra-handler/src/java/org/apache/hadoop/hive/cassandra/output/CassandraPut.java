package org.apache.hadoop.hive.cassandra.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class CassandraPut implements Writable{

  private String key;
  //private ConsistencyLevel level;
  private List<CassandraColumn> columns;

  public CassandraPut(){
    columns = new ArrayList<CassandraColumn>();
  }

  public CassandraPut(String key){
    this();
    setKey(key);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    key = in.readUTF();
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
    out.writeUTF(this.key);
    out.writeInt(1);
    out.writeInt(this.columns.size());
    for (CassandraColumn c: this.columns){
      c.write(out);
    }
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
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