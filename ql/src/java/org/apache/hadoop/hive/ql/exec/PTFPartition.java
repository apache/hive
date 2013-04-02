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

import java.util.ConcurrentModificationException;
import java.util.Iterator;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.PTFPersistence.ByteBasedList;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

/*
 * represents a collection of rows that is acted upon by a TableFunction or a WindowFunction.
 */
public class PTFPartition
{
  SerDe serDe;
  StructObjectInspector OI;
  private ByteBasedList elems;
  private Writable wRow;
  private int sz;

  public PTFPartition(HiveConf cfg, SerDe serDe, StructObjectInspector oI) throws HiveException
  {
    String partitionClass = HiveConf.getVar(cfg, ConfVars.HIVE_PTF_PARTITION_PERSISTENCE_CLASS);
    int partitionMemSize = HiveConf.getIntVar(cfg, ConfVars.HIVE_PTF_PARTITION_PERSISTENT_SIZE);
    init(partitionClass, partitionMemSize, serDe, oI);
  }

  public PTFPartition(String partitionClass, int partitionMemSize, SerDe serDe, StructObjectInspector oI) throws HiveException
  {
    init(partitionClass, partitionMemSize, serDe, oI);
  }

  private void init(String partitionClass, int partitionMemSize, SerDe serDe, StructObjectInspector oI) throws HiveException
  {
    this.serDe = serDe;
    OI = oI;
    elems = PTFPersistence.createList(partitionClass, partitionMemSize);
    sz = 0;
    wRow = createWritable();
  }

  public void reset() throws HiveException {
    sz = 0;
    elems.reset(0);
  }

  public SerDe getSerDe()
  {
    return serDe;
  }
  public void setSerDe(SerDe serDe)
  {
    this.serDe = serDe;
  }
  public StructObjectInspector getOI()
  {
    return OI;
  }
  public void setOI(StructObjectInspector oI)
  {
    OI = oI;
  }

  private Writable createWritable() throws HiveException
  {
    try
    {
      return serDe.getSerializedClass().newInstance();
    }
    catch(Throwable t)
    {
      throw new HiveException(t);
    }
  }

  public Object getAt(int i) throws HiveException
  {
    try
    {
      elems.get(i, wRow);
      Object o = serDe.deserialize(wRow);
      return o;
    }
    catch(SerDeException  se)
    {
      throw new HiveException(se);
    }
  }

  public Object getWritableAt(int i) throws HiveException
  {
    elems.get(i, wRow);
    return wRow;
  }

  public void append(Writable o) throws HiveException
  {
    elems.append(o);
    sz++;
  }

  public void append(Object o) throws HiveException
  {
    try
    {
      append(serDe.serialize(o, OI));
    }
    catch(SerDeException e)
    {
      throw new HiveException(e);
    }
  }

  public int size()
  {
    return sz;
  }

  public PTFPartitionIterator<Object> iterator()
  {
    return new PItr(0, size());
  }

  public PTFPartitionIterator<Object> range(int start, int end)
  {
    assert(start >= 0);
    assert(end <= size());
    assert(start <= end);
    return new PItr(start, end);
  }

  class PItr implements PTFPartitionIterator<Object>
  {
    int idx;
    final int start;
    final int end;
    final int createTimeSz;

    PItr(int start, int end)
    {
      this.idx = start;
      this.start = start;
      this.end = end;
      createTimeSz = PTFPartition.this.size();
    }

    public boolean hasNext()
    {
      checkForComodification() ;
      return idx < end;
    }

    public Object next()
    {
      checkForComodification();
      try
      {
        return PTFPartition.this.getAt(idx++);
      }
      catch(HiveException e)
      {
        throw new RuntimeException(e);
      }
    }

    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    final void checkForComodification()
    {
        if (createTimeSz != PTFPartition.this.size()) {
          throw new ConcurrentModificationException();
        }
    }

    @Override
    public int getIndex()
    {
      return idx;
    }

    private Object getAt(int i)
    {
      try
      {
        return PTFPartition.this.getAt(i);
      }
      catch(HiveException e)
      {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Object lead(int amt)
    {
      int i = idx + amt;
      i = i >= end ? end - 1 : i;
      return getAt(i);
    }

    @Override
    public Object lag(int amt)
    {
      int i = idx - amt;
      i = i < start ? start : i;
      return getAt(i);
    }

    @Override
    public Object resetToIndex(int idx)
    {
      if ( idx < start || idx >= end )
      {
        return null;
      }
      Object o = getAt(idx);
      this.idx = idx + 1;
      return o;
    }

    @Override
    public PTFPartition getPartition()
    {
      return PTFPartition.this;
    }

    @Override
    public void reset()
    {
      idx = start;
    }
  };

  /*
   * provide an Iterator on the rows in a Partiton.
   * Iterator exposes the index of the next location.
   * Client can invoke lead/lag relative to the next location.
   */
  public static interface PTFPartitionIterator<T> extends Iterator<T>
  {
    int getIndex();

    T lead(int amt);

    T lag(int amt);

    /*
     * after a lead and lag call, allow Object associated with SerDe and writable associated with partition to be reset
     * to the value for the current Index.
     */
    Object resetToIndex(int idx);

    PTFPartition getPartition();

    void reset();
  }


}
