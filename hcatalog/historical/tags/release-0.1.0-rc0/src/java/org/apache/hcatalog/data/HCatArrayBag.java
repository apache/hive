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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hcatalog.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

public class HCatArrayBag<T> implements DataBag {

  private static final long DUMMY_SIZE = 40;
  List<T>  rawItemList = null;
  DataBag convertedBag = null;
//  List<Tuple> tupleList = null;

  public class HowlArrayBagIterator implements Iterator<Tuple> {

    Iterator<T> iter = null;

    public HowlArrayBagIterator(List<T> rawItemList) {
      iter = rawItemList.iterator();
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public Tuple next() {
      Tuple t = new DefaultTuple();
      t.append(iter.next());
      return t;
    }

    @Override
    public void remove() {
      iter.remove();
    }

  }

  public HCatArrayBag(List<T> list) {
    rawItemList = list;
  }

  private void convertFromRawToTupleForm(){
    if (convertedBag == null){
      List<Tuple> ltuples = new ArrayList<Tuple>();
      for (T item : rawItemList){
        Tuple t = new DefaultTuple();
        t.append(item);
        ltuples.add(t);
      }
      convertedBag = DefaultBagFactory.getInstance().newDefaultBag(ltuples);
    }else{
      // TODO : throw exception or be silent? Currently going with silence, but needs revisiting.
    }
  }

  @Override
  public void add(Tuple t) {
    if (convertedBag == null){
      convertFromRawToTupleForm();
    }
    convertedBag.add(t);
  }

  @Override
  public void addAll(DataBag db) {
    Tuple t;
    for (Iterator<Tuple> dbi = db.iterator() ; dbi.hasNext();){
      this.add(dbi.next());
    }
  }

  @Override
  public void clear() {
    rawItemList = null;
    if (convertedBag != null){
      convertedBag.clear();
      convertedBag = null;
    }
  }

  @Override
  public boolean isDistinct() {
    return false;
  }

  @Override
  public boolean isSorted() {
    return false;
  }

  @Override
  public Iterator<Tuple> iterator() {
    if (convertedBag != null){
      return convertedBag.iterator();
    }else{
      return new HowlArrayBagIterator(rawItemList);
    }
  }

  @Override
  public void markStale(boolean arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public long size() {
    return (convertedBag == null ? (rawItemList == null ? 0 : rawItemList.size()) : convertedBag.size() );
  }

  @Override
  public long getMemorySize() {
    // FIXME: put in actual impl
    if (convertedBag != null){
      return convertedBag.getMemorySize() + DUMMY_SIZE;
    }else {
      return DUMMY_SIZE;
    }
  }

  @Override
  public long spill() {
    // FIXME: put in actual spill impl even for the list case
    if (convertedBag != null){
      return convertedBag.spill();
    }
    return 0;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    convertedBag = new DefaultDataBag();
    convertedBag.readFields(arg0);
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    convertFromRawToTupleForm();
    convertedBag.write(arg0);
  }

  @Override
  public int compareTo(Object arg0) {
    // TODO Auto-generated method stub - really need to put in a better implementation here, also, equality case not considered yet
    return arg0.hashCode() < this.hashCode() ? -1 : 1;
  }

}
