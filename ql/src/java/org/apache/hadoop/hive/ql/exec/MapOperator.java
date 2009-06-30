/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import java.util.*;
import java.io.*;
import java.net.URLClassLoader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Map operator. This triggers overall map side processing.
 * This is a little different from regular operators in that
 * it starts off by processing a Writable data structure from
 * a Table (instead of a Hive Object).
 **/
public class MapOperator extends Operator <mapredWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  public static enum Counter {DESERIALIZE_ERRORS}
  transient private LongWritable deserialize_error_count = new LongWritable ();
  transient private Deserializer deserializer;
  
  transient private Object[] rowWithPart;
  transient private StructObjectInspector rowObjectInspector;
  transient private boolean isPartitioned;
  
  private static class MapInputPath {
    String path;
    String alias;
    Operator<? extends Serializable> op;
    
    /**
     * @param path
     * @param alias
     * @param op
     */
    public MapInputPath(String path, String alias,
        Operator<? extends Serializable> op) {
      this.path = path;
      this.alias = alias;
      this.op = op;
    }

    public boolean equals(Object o) {
      if (o instanceof MapInputPath) {
        MapInputPath mObj = (MapInputPath)o;
        if (mObj == null)
          return false;
        return path.equals(mObj.path) && alias.equals(mObj.alias) && op.equals(mObj.op);
      }
      
      return false;
    }

    public int hashCode() {
      return (op == null) ? 0 : op.hashCode();
    }
  }
  
  private static class MapOpCtx {
    boolean               isPartitioned;
    StructObjectInspector rowObjectInspector;
    Object[]              rowWithPart;
    Deserializer          deserializer;
    
    /**
     * @param isPartitioned
     * @param rowObjectInspector
     * @param rowWithPart
     */
    public MapOpCtx(boolean isPartitioned,
        StructObjectInspector rowObjectInspector, Object[] rowWithPart, Deserializer deserializer) {
      this.isPartitioned = isPartitioned;
      this.rowObjectInspector = rowObjectInspector;
      this.rowWithPart = rowWithPart;
      this.deserializer = deserializer;
    }

    /**
     * @return the isPartitioned
     */
    public boolean isPartitioned() {
      return isPartitioned;
    }

    /**
     * @return the rowObjectInspector
     */
    public StructObjectInspector getRowObjectInspector() {
      return rowObjectInspector;
    }

    /**
     * @return the rowWithPart
     */
    public Object[] getRowWithPart() {
      return rowWithPart;
    }

    /**
     * @return the deserializer
     */
    public Deserializer getDeserializer() {
      return deserializer;
    }
  }
  
  private MapOpCtx initObjectInspector(Configuration hconf, String onefile) throws HiveException, ClassNotFoundException, InstantiationException, IllegalAccessException, SerDeException {
    partitionDesc pd = conf.getPathToPartitionInfo().get(onefile);
    LinkedHashMap<String, String> partSpec = pd.getPartSpec();
    tableDesc td = pd.getTableDesc();
    Properties p = td.getProperties();

    // Add alias, table name, and partitions to hadoop conf
    HiveConf.setVar(hconf, HiveConf.ConfVars.HIVETABLENAME, String.valueOf(p.getProperty("name")));
    HiveConf.setVar(hconf, HiveConf.ConfVars.HIVEPARTITIONNAME, String.valueOf(partSpec));
    Class sdclass = td.getDeserializerClass();
    if(sdclass == null) {
      String className = td.getSerdeClassName();
      if ((className == "") || (className == null)) {
        throw new HiveException("SerDe class or the SerDe class name is not set for table: " + td.getProperties().getProperty("name"));
      }
      sdclass = hconf.getClassByName(className);
    }
    
    deserializer = (Deserializer) sdclass.newInstance();
    deserializer.initialize(hconf, p);
    rowObjectInspector = (StructObjectInspector)deserializer.getObjectInspector();
    
    // Next check if this table has partitions and if so
    // get the list of partition names as well as allocate
    // the serdes for the partition columns
    String pcols = p.getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS);
    if (pcols != null && pcols.length() > 0) {
      String[] partKeys = pcols.trim().split("/");
      List<String> partNames = new ArrayList<String>(partKeys.length);
      Object[] partValues = new Object[partKeys.length];
      List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>(partKeys.length);
      for(int i = 0; i < partKeys.length; i++ ) {
        String key = partKeys[i];
        partNames.add(key);
        partValues[i] = new Text(partSpec.get(key));
        partObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      }
      StructObjectInspector partObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(partNames, partObjectInspectors);
      
      rowWithPart = new Object[2];
      rowWithPart[1] = partValues;
      rowObjectInspector = ObjectInspectorFactory.getUnionStructObjectInspector(Arrays.asList(new StructObjectInspector[]{
                                                                                                rowObjectInspector, 
                                                                                                partObjectInspector}));
      return new MapOpCtx(true, rowObjectInspector, rowWithPart, deserializer);
    }
    else {
      return new MapOpCtx(false, rowObjectInspector, null, deserializer);
    }
  }  

  public void initializeOp(Configuration hconf, Reporter reporter,
      ObjectInspector[] inputObjInspector) throws HiveException {
    Path fpath = new Path((new Path(HiveConf.getVar(hconf,
        HiveConf.ConfVars.HADOOPMAPFILENAME))).toUri().getPath());
    ArrayList<Operator<? extends Serializable>> todo = new ArrayList<Operator<? extends Serializable>>();
    Map<MapInputPath, MapOpCtx> opCtx = new HashMap<MapInputPath, MapOpCtx>();
    statsMap.put(Counter.DESERIALIZE_ERRORS, deserialize_error_count);

    try {
      // initialize the complete subtree
      for (String onefile : conf.getPathToAliases().keySet()) {
        MapOpCtx ctx = initObjectInspector(hconf, onefile);

        List<String> aliases = conf.getPathToAliases().get(onefile);
        for (String onealias : aliases) {
          Operator<? extends Serializable> op = conf.getAliasToWork().get(
              onealias);
          opCtx.put(new MapInputPath(onefile, onealias, op), ctx);
        }
      }

      boolean done = false;
      // for each configuration path that fpath can be relativized against ..
      for (String onefile : conf.getPathToAliases().keySet()) {
        Path onepath = new Path(new Path(onefile).toUri().getPath());
        if (!onepath.toUri().relativize(fpath.toUri()).equals(fpath.toUri())) {

          // pick up work corresponding to this configuration path
          List<String> aliases = conf.getPathToAliases().get(onefile);
          for (String onealias : aliases) {
            LOG.info("Adding alias " + onealias + " to work list for file "
                + fpath.toUri().getPath());
            Operator<? extends Serializable> op = conf.getAliasToWork().get(
                onealias);
            List<Operator<? extends Serializable>> parents = new ArrayList<Operator<? extends Serializable>>();
            parents.add(this);
            op.setParentOperators(parents);
            todo.add(op);
            MapInputPath inp = new MapInputPath(onefile, onealias, op);
            LOG.info("dump " + opCtx.get(inp).getRowObjectInspector().getTypeName());
            op.initialize(hconf, reporter, new ObjectInspector[] { opCtx.get(inp).getRowObjectInspector() });

            if (!done) {
              deserializer = opCtx.get(inp).getDeserializer();
              isPartitioned = opCtx.get(inp).isPartitioned();
              rowWithPart = opCtx.get(inp).getRowWithPart();
              rowObjectInspector = opCtx.get(inp).getRowObjectInspector();
              done = true;
            }
          }
        }
      }

      for (MapInputPath input : opCtx.keySet()) {
        Operator<? extends Serializable> op = input.op;
        op.initialize(hconf, reporter, new ObjectInspector[] { opCtx.get(input).getRowObjectInspector() });
      }

      if (todo.size() == 0) {
        // didn't find match for input file path in configuration!
        // serious problem ..
        LOG.error("Configuration does not have any alias for path: "
            + fpath.toUri().getPath());
        throw new HiveException("Configuration and input path are inconsistent");
      }

      // we found all the operators that we are supposed to process. now
      // bootstrap
      this.setChildOperators(todo);
      // the child operators may need the global mr configuration. set it now so
      // that they can get access during initiaize.
      this.setMapredWork(conf);
      // way hacky - need to inform child operators about output collector
      this.setOutputCollector(out);

    } catch (SerDeException e) {
      e.printStackTrace();
      throw new HiveException(e);
    } catch (InstantiationException e) {
      throw new HiveException(e);
    } catch (IllegalAccessException e) {
      throw new HiveException(e);
    } catch (ClassNotFoundException e) {
      throw new HiveException(e);
    }
  }

  public void process(Writable value) throws HiveException {
    try {
      if (!isPartitioned) {
        Object row = deserializer.deserialize(value);
        forward(row, rowObjectInspector);
      } else {
        rowWithPart[0] = deserializer.deserialize(value);
        forward(rowWithPart, rowObjectInspector);
      }
    } catch (SerDeException e) {
      // TODO: policy on deserialization errors
      deserialize_error_count.set(deserialize_error_count.get()+1);
      throw new HiveException (e);
    }
  }

  public void process(Object row, ObjectInspector rowInspector, int tag)
      throws HiveException {
    throw new HiveException("Hive 2 Internal error: should not be called!");
  }
}
