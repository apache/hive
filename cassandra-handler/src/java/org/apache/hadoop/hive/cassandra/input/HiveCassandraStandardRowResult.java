package org.apache.hadoop.hive.cassandra.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/**
 *
 * HiveCassandraStandardRowResult. Used as the value side of
 * the InputFormat
 *
 */
public class HiveCassandraStandardRowResult implements Writable{

    private Text key;
    private MapWritable value;

    public HiveCassandraStandardRowResult() {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      key = new Text();
      key.readFields(in);
      value = new MapWritable();
      value.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      key.write(out);
      value.write(out);
    }

    public Text getKey() {
      //Text might contain more bytes than expected.
      //construct the key based on the length of the Text.
      if (key.getBytes().length > key.getLength()) {
        Text newKey = new Text();
        newKey.set(key.getBytes(), 0, key.getLength());
        return newKey;
      } else {
        return key;
      }
    }

    public void setKey(Text key) {
      this.key = key;
    }

    public MapWritable getValue() {
      return value;
    }

    public void setValue(MapWritable value) {
      this.value = value;
    }

    @Override
    public String toString(){
     StringBuffer sb = new StringBuffer();
     sb.append("RowResult  key:"+key );
     for (Map.Entry<Writable,Writable> entry : value.entrySet()){
       sb.append( "entry key:"+entry.getKey()+" " );
       sb.append( "entry value:"+entry.getValue()+" " );
     }
     return sb.toString();
    }

}
