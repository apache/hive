package org.apache.hadoop.hive.cassandra.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.apache.hadoop.io.Writable;
import org.apache.thrift.TException;

/**
 * This represents a standard column family. It implements hadoop Writable interface.
 *
 */
public class CassandraPut implements Writable, Put {

  private ByteBuffer key;
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

  @Override
  public void write(String keySpace, CassandraProxyClient client, ConsistencyLevel flevel) throws IOException {
    Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();

    Map<String, List<Mutation>> maps = new HashMap<String, List<Mutation>>();

    for (CassandraColumn col : columns) {
      Column cassCol = new Column();
      cassCol.setValue(col.getValue());
      cassCol.setTimestamp(col.getTimeStamp());
      cassCol.setName(col.getColumn());

      ColumnOrSuperColumn thisCol = new ColumnOrSuperColumn();
      thisCol.setColumn(cassCol);

      Mutation mutation = new Mutation();
      mutation.setColumn_or_supercolumn(thisCol);

      List<Mutation> mutList = maps.get(col.getColumnFamily());
      if (mutList == null) {
        mutList = new ArrayList<Mutation>();
        maps.put(col.getColumnFamily(), mutList);
      }

      mutList.add(mutation);
    }
    mutation_map.put(key, maps);

    try {
      client.getProxyConnection().set_keyspace(keySpace);
      client.getProxyConnection().batch_mutate(mutation_map, flevel);
    } catch (InvalidRequestException e) {
      throw new IOException(e);
    } catch (UnavailableException e) {
      throw new IOException(e);
    } catch (TimedOutException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  }
}