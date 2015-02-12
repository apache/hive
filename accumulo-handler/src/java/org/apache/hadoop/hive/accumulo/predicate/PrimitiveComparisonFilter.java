/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.accumulo.predicate;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMappingFactory;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloColumnMapping;
import org.apache.hadoop.hive.accumulo.predicate.compare.CompareOp;
import org.apache.hadoop.hive.accumulo.predicate.compare.PrimitiveComparison;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * Operates over a single qualifier.
 *
 * Delegates to PrimitiveCompare and CompareOpt instances for value acceptance.
 *
 * The PrimitiveCompare strategy assumes a consistent value type for the same column family and
 * qualifier.
 */
public class PrimitiveComparisonFilter extends WholeRowIterator {
  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(PrimitiveComparisonFilter.class);

  public static final String FILTER_PREFIX = "accumulo.filter.compare.iterator.";
  public static final String P_COMPARE_CLASS = "accumulo.filter.iterator.p.compare.class";
  public static final String COMPARE_OPT_CLASS = "accumulo.filter.iterator.compare.opt.class";
  public static final String CONST_VAL = "accumulo.filter.iterator.const.val";
  public static final String COLUMN = "accumulo.filter.iterator.qual";

  private Text cfHolder, cqHolder, columnMappingFamily, columnMappingQualifier;
  private HiveAccumuloColumnMapping columnMapping;
  private CompareOp compOpt;

  @Override
  protected boolean filter(Text currentRow, List<Key> keys, List<Value> values) {
    SortedMap<Key,Value> items;
    boolean allow;
    try { // if key doesn't contain CF, it's an encoded value from a previous iterator.
      while (keys.get(0).getColumnFamily().getBytes().length == 0) {
        items = decodeRow(keys.get(0), values.get(0));
        keys = Lists.newArrayList(items.keySet());
        values = Lists.newArrayList(items.values());
      }
      allow = accept(keys, values);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return allow;
  }

  private boolean accept(Collection<Key> keys, Collection<Value> values) {
    Iterator<Key> kIter = keys.iterator();
    Iterator<Value> vIter = values.iterator();
    while (kIter.hasNext()) {
      Key k = kIter.next();
      Value v = vIter.next();
      if (matchQualAndFam(k)) {
        return compOpt.accept(v.get());
      }
    }
    return false;
  }

  private boolean matchQualAndFam(Key k) {
    k.getColumnFamily(cfHolder);
    k.getColumnQualifier(cqHolder);
    return cfHolder.equals(columnMappingFamily) && cqHolder.equals(columnMappingQualifier);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    String serializedColumnMapping = options.get(COLUMN);
    Entry<String,String> pair = ColumnMappingFactory.parseMapping(serializedColumnMapping);

    // The ColumnEncoding, column name and type are all irrelevant at this point, just need the
    // cf:[cq]
    columnMapping = new HiveAccumuloColumnMapping(pair.getKey(), pair.getValue(),
        ColumnEncoding.STRING, "column", "string");
    columnMappingFamily = new Text(columnMapping.getColumnFamily());
    columnMappingQualifier = new Text(columnMapping.getColumnQualifier());
    cfHolder = new Text();
    cqHolder = new Text();

    try {
      Class<?> pClass = Class.forName(options.get(P_COMPARE_CLASS));
      Class<?> cClazz = Class.forName(options.get(COMPARE_OPT_CLASS));
      PrimitiveComparison pCompare = pClass.asSubclass(PrimitiveComparison.class).newInstance();
      compOpt = cClazz.asSubclass(CompareOp.class).newInstance();
      byte[] constant = getConstant(options);
      pCompare.init(constant);
      compOpt.setPrimitiveCompare(pCompare);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  protected byte[] getConstant(Map<String,String> options) {
    String b64Const = options.get(CONST_VAL);
    return Base64.decodeBase64(b64Const.getBytes());
  }
}
