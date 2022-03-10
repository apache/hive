/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.api.repl;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import org.apache.commons.io.IOExceptionWithCause;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hive.hcatalog.api.HCatDatabase;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.data.ReaderWriter;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;

public class ReplicationUtils {

  public final static String REPL_STATE_ID = ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString();

  private ReplicationUtils(){
    // dummy private constructor, since this class is a collection of static utility methods.
  }

  /**
   * Gets the last known replication state of this db. This is
   * applicable only if it is the destination of a replication
   * and has had data replicated into it via imports previously.
   * Defaults to 0.
   */
  public static long getLastReplicationId(HCatDatabase db){
    Map<String, String> props = db.getProperties();
    if (props != null){
      if (props.containsKey(REPL_STATE_ID)){
        return Long.parseLong(props.get(REPL_STATE_ID));
      }
    }
    return 0l; // default is to return earliest possible state.
  }


  /**
   * Gets the last known replication state of the provided table. This
   * is applicable only if it is the destination of a replication
   * and has had data replicated into it via imports previously.
   * Defaults to 0.
   */
  public static long getLastReplicationId(HCatTable tbl) {
    Map<String, String> tblProps = tbl.getTblProps();
    if (tblProps != null){
      if (tblProps.containsKey(REPL_STATE_ID)){
        return Long.parseLong(tblProps.get(REPL_STATE_ID));
      }
    }
    return 0l; // default is to return earliest possible state.
  }

  /**
   * Gets the last known replication state of the provided partition.
   * This is applicable only if it is the destination of a replication
   * and has had data replicated into it via imports previously.
   * If that is not available, but parent table is provided,
   * defaults to parent table's replication state. If that is also
   * unknown, defaults to 0.
   */
  public static long getLastReplicationId(HCatPartition ptn, @Nullable HCatTable parentTable) {
    Map<String,String> parameters = ptn.getParameters();
    if (parameters != null){
      if (parameters.containsKey(REPL_STATE_ID)){
        return Long.parseLong(parameters.get(REPL_STATE_ID));
      }
    }

    if (parentTable != null){
      return getLastReplicationId(parentTable);
    }
    return 0l; // default is to return earliest possible state.
  }

  /**
   * Used to generate a unique key for a combination of given event id, dbname,
   * tablename and partition keyvalues. This is used to feed in a name for creating
   * staging directories for exports and imports. This should be idempotent given
   * the same values, i.e. hashcode-like, but at the same time, be guaranteed to be
   * different for every possible partition, while being "readable-ish". Basically,
   * we concat the alphanumberic versions of all of the above, along with a hashcode
   * of the db, tablename and ptn key-value pairs
   */
  public static String getUniqueKey(long eventId, String db, String table, Map<String, String> ptnDesc) {
    StringBuilder sb = new StringBuilder();
    sb.append(eventId);
    sb.append('.');
    sb.append(toStringWordCharsOnly(db));
    sb.append('.');
    sb.append(toStringWordCharsOnly(table));
    sb.append('.');
    sb.append(toStringWordCharsOnly(ptnDesc));
    sb.append('.');
    sb.append(Objects.hashCode(db, table, ptnDesc));
    return sb.toString();
  }

  /**
   * Return alphanumeric(and '_') representation of a Map<String,String>
   *
   */
  private static String toStringWordCharsOnly(Map<String, String> map) {
    if (map == null){
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String,String> e : map.entrySet()){
      if (!first){
        sb.append(',');
      }
      sb.append(toStringWordCharsOnly(e.getKey()));
      sb.append('=');
      sb.append(toStringWordCharsOnly(e.getValue()));
      first = false;
    }
    return sb.toString();
  }

  /**
   * Return alphanumeric(and '_') chars only of a string, lowercased
   */
  public static String toStringWordCharsOnly(String s){
    return (s == null) ? "null" : s.replaceAll("[\\W]", "").toLowerCase();
  }

  /**
   * Utility function to use in conjunction with .withDbNameMapping / .withTableNameMapping,
   * if we desire usage of a Map<String,String> instead of implementing a Function<String,String>
   */
  Function<String,String> mapBasedFunction(final Map<String,String> m){
    return new Function<String,String>(){

      @Nullable
      @Override
      public String apply(@Nullable String s) {
        if ((m == null) || (!m.containsKey(s))){
          return s;
        }
        return m.get(s);
      }
    };
  }

  /**
   * Return a mapping from a given map function if available, and the key itself if not.
   */
  public static String mapIfMapAvailable(String s, Function<String, String> mapping){
    try {
      if (mapping != null){
        return mapping.apply(s);
      }
    } catch (IllegalArgumentException iae){
      // The key wasn't present in the mapping, and the function didn't
      // return a default value - ignore, and use our default.
    }
    // We return the key itself, since no mapping was available/returned
    return s;
  }

  public static String partitionDescriptor(Map<String,String> ptnDesc) {
    StringBuilder sb = new StringBuilder();
    if ((ptnDesc != null) && (!ptnDesc.isEmpty())){
      boolean first = true;
      sb.append(" PARTITION (");
      for (Map.Entry e : ptnDesc.entrySet()){
        if (!first){
          sb.append(", ");
        } else {
          first = false;
        }
        sb.append(e.getKey()); // TODO : verify if any quoting is needed for keys
        sb.append('=');
        sb.append('"');
        sb.append(e.getValue()); // TODO : verify if any escaping is needed for values
        sb.append('"');
      }
      sb.append(')');
    }
    return sb.toString();
  }

  /**
   * Command implements Writable, but that's not terribly easy to use compared
   * to String, even if it plugs in easily into the rest of Hadoop. Provide
   * utility methods to easily serialize and deserialize Commands
   *
   * serializeCommand returns a base64 String representation of given command
   */
  public static String serializeCommand(Command command) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(baos);
    ReaderWriter.writeDatum(dataOutput,command.getClass().getName());
    command.write(dataOutput);
    return Base64.getUrlEncoder().encodeToString(baos.toByteArray());
  }

  /**
   * Command implements Writable, but that's not terribly easy to use compared
   * to String, even if it plugs in easily into the rest of Hadoop. Provide
   * utility methods to easily serialize and deserialize Commands
   *
   * deserializeCommand instantiates a concrete Command and initializes it,
   * given a base64 String representation of it.
   */
   public static Command deserializeCommand(String s) throws IOException {
    DataInput dataInput = new DataInputStream(new ByteArrayInputStream(Base64.getUrlDecoder().decode(s)));
    String clazz = (String) ReaderWriter.readDatum(dataInput);
    Command cmd;
    try {
      cmd = (Command)Class.forName(clazz).newInstance();
    } catch (Exception e) {
      throw new IOExceptionWithCause("Error instantiating class "+clazz,e);
    }
    cmd.readFields(dataInput);
    return cmd;
  }

}
