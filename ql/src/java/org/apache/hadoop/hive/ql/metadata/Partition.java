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

package org.apache.hadoop.hive.ql.metadata;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

/**
 * A Hive Table Partition: is a fundamental storage unit within a Table
 */
public class Partition {

  @SuppressWarnings("nls")
  static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Partition");

  private Table table;
  private org.apache.hadoop.hive.metastore.api.Partition tPartition;
  /**
   * @return the tPartition
   */
  public org.apache.hadoop.hive.metastore.api.Partition getTPartition() {
    return tPartition;
  }

  private LinkedHashMap<String, String> spec;

  /**
   * @return The values of the partition
   * @see org.apache.hadoop.hive.metastore.api.Partition#getValues()
   */
  public List<String> getValues() {
    return tPartition.getValues();
  }

  private Path partPath;
  private URI partURI;

  public Partition(Table tbl, org.apache.hadoop.hive.metastore.api.Partition tp) throws HiveException {
    initialize(tbl, tp);
  }

  /**
   * Create partition object with the given info.
   * @param tbl Table the partition will be in.
   * @param partSpec Partition specifications.
   * @param location Location of the partition, relative to the table.
   * @throws HiveException Thrown if we could not create the partition.
   */
  public Partition(Table tbl, Map<String, String> partSpec,
      Path location) throws HiveException {

    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : tbl.getPartCols()) {
      String val = partSpec.get(field.getName());
      if (val == null) {
        throw new HiveException("partition spec is invalid. field.getName() does not exist in input.");
      }
      pvals.add(val);
    }

    org.apache.hadoop.hive.metastore.api.Partition tpart =
      new org.apache.hadoop.hive.metastore.api.Partition();
    tpart.setDbName(tbl.getDbName());
    tpart.setTableName(tbl.getName());
    tpart.setValues(pvals);

    StorageDescriptor sd = new StorageDescriptor();
    try {
      //replace with THRIFT-138
      TMemoryBuffer buffer = new TMemoryBuffer(1024);
      TBinaryProtocol prot = new TBinaryProtocol(buffer);
      tbl.getTTable().getSd().write(prot);

      sd.read(prot);
    } catch (TException e) {
      LOG.error("Could not create a copy of StorageDescription");
      throw new HiveException("Could not create a copy of StorageDescription");
    }

    tpart.setSd(sd);
    if (location != null) {
      tpart.getSd().setLocation(location.toString());
    } else {
      tpart.getSd().setLocation(null);
    }

    initialize(tbl, tpart);
  }

  /**
   * Initializes this object with the given variables
   * @param tbl Table the partition belongs to
   * @param tp Thrift Partition object
   * @throws HiveException Thrown if we cannot initialize the partition
   */
  private void initialize(Table tbl,
      org.apache.hadoop.hive.metastore.api.Partition tp)
  throws HiveException {

    table = tbl;
    tPartition = tp;
    partName = "";

    if(tbl.isPartitioned()) {
      try {
        partName = Warehouse.makePartName(tbl.getPartCols(),
            tp.getValues());
        if (tp.getSd().getLocation() == null) {
          // set default if location is not set
          partPath = new Path(tbl.getDataLocation().toString(), partName);
          tp.getSd().setLocation(partPath.toString());
        } else {
          partPath = new Path(tp.getSd().getLocation());
        }
      } catch (MetaException e) {
        throw new HiveException("Invalid partition for table " + tbl.getName(),
            e);
      }
    } else {
      // We are in the HACK territory.
      // SemanticAnalyzer expects a single partition whose schema
      // is same as the table partition.
      partPath = table.getPath();
    }

    spec = tbl.createSpec(tp);
    partURI = partPath.toUri();
  }

  public String getName() {
    return partName;
  }

  public Table getTable() {
    return table;
  }

  public Path [] getPath() {
    Path [] ret = new Path [1];
    ret[0] = partPath;
    return(ret);
  }

  public Path getPartitionPath() {
    return partPath;
  }

  final public URI getDataLocation() {
    return partURI;
  }

  /**
   * The number of buckets is a property of the partition. However - internally we are just
   * storing it as a property of the table as a short term measure.
   */
  public int getBucketCount() {
    return table.getNumBuckets();
    /*
      TODO: Keeping this code around for later use when we will support
      sampling on tables which are not created with CLUSTERED INTO clause

      // read from table meta data
      int numBuckets = this.table.getNumBuckets();
      if (numBuckets == -1) {
        // table meta data does not have bucket information
        // check if file system has multiple buckets(files) in this partition
        String pathPattern = this.partPath.toString() + "/*";
        try {
          FileSystem fs = FileSystem.get(this.table.getDataLocation(), Hive.get().getConf());
          FileStatus srcs[] = fs.globStatus(new Path(pathPattern));
          numBuckets = srcs.length;
        }
        catch (Exception e) {
          throw new RuntimeException("Cannot get bucket count for table " + this.table.getName(), e);
        }
      }
      return numBuckets;
     */
  }

  public List<String> getBucketCols() {
    return table.getBucketCols();
  }

  /**
   * mapping from bucket number to bucket path
   */
  //TODO: add test case and clean it up
  @SuppressWarnings("nls")
  public Path getBucketPath(int bucketNum) {
    try {
      FileSystem fs = FileSystem.get(table.getDataLocation(), Hive.get().getConf());
      String pathPattern = partPath.toString();
      if (getBucketCount() > 0) {
        pathPattern = pathPattern + "/*";
      }
      LOG.info("Path pattern = " + pathPattern);
      FileStatus srcs[] = fs.globStatus(new Path(pathPattern));
      Arrays.sort(srcs);
      for (FileStatus src: srcs) {
        LOG.info("Got file: " + src.getPath());
      }
      return srcs[bucketNum].getPath();
    }
    catch (Exception e) {
      throw new RuntimeException("Cannot get bucket path for bucket " + bucketNum, e);
    }
  }

  /**
   * mapping from a Path to the bucket number if any
   */
  private static Pattern bpattern = Pattern.compile("part-([0-9][0-9][0-9][0-9][0-9])");

  private String partName;
  @SuppressWarnings("nls")
  public static int getBucketNum(Path p) {
    Matcher m = bpattern.matcher(p.getName());
    if(m.find()) {
      String bnum_str = m.group(1);
      try {
        return (Integer.parseInt(bnum_str));
      } catch (NumberFormatException e) {
        throw new RuntimeException("Unexpected error parsing: "+p.getName()+","+bnum_str);
      }
    }
    return 0;
  }


  @SuppressWarnings("nls")
  public Path [] getPath(Sample s) throws HiveException {
    if(s == null) {
      return getPath();
    } else {
      int bcount = getBucketCount();
      if(bcount == 0) {
        return getPath();
      }

      Dimension d = s.getSampleDimension();
      if(!d.getDimensionId().equals(table.getBucketingDimensionId())) {
        // if the bucket dimension is not the same as the sampling dimension
        // we must scan all the data
        return getPath();
      }

      int scount = s.getSampleFraction();
      ArrayList<Path> ret = new ArrayList<Path> ();

      if(bcount == scount) {
        ret.add(getBucketPath(s.getSampleNum()-1));
      } else if (bcount < scount) {
        if((scount/bcount)*bcount != scount) {
          throw new HiveException("Sample Count"+scount+" is not a multiple of bucket count " +
              bcount + " for table " + table.getName());
        }
        // undersampling a bucket
        ret.add(getBucketPath((s.getSampleNum()-1)%bcount));
      } else if (bcount > scount) {
        if((bcount/scount)*scount != bcount) {
          throw new HiveException("Sample Count"+scount+" is not a divisor of bucket count " +
              bcount + " for table " + table.getName());
        }
        // sampling multiple buckets
        for(int i=0; i<bcount/scount; i++) {
          ret.add(getBucketPath(i*scount + (s.getSampleNum()-1)));
        }
      }
      return(ret.toArray(new Path[ret.size()]));
    }
  }

  public LinkedHashMap<String, String> getSpec() {
    return spec;
  }


  @SuppressWarnings("nls")
  @Override
  public String toString() {
    String pn = "Invalid Partition";
    try {
      pn = Warehouse.makePartName(spec);
    } catch (MetaException e) {
      // ignore as we most probably in an exception path already otherwise this error wouldn't occur
    }
    return table.toString() + "(" + pn + ")";
  }

  public void setProperty(String name, String value) {
    getTPartition().putToParameters(name, value);
  }

  /**
   * getProperty
   *
   */
  public String getProperty(String name) {
    Map<String,String> params = getTPartition().getParameters();
    if (params == null)
      return null;
    return params.get(name);
  }

}
