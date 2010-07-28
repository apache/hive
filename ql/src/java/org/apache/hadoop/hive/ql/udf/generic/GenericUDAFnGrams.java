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
package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.Text;

/**
 * Estimates the top-k n-grams in arbitrary sequential data using a heuristic.
 */
@Description(name = "ngrams",
    value = "_FUNC_(expr, n, k, pf) - Estimates the top-k n-grams in rows that consist of "
            + "sequences of strings, represented as arrays of strings, or arrays of arrays of "
            + "strings. 'pf' is an optional precision factor that controls memory usage.",
    extended = "The parameter 'n' specifies what type of n-grams are being estimated. Unigrams "
             + "are n = 1, and bigrams are n = 2. Generally, n will not be greater than about 5. "
             + "The 'k' parameter specifies how many of the highest-frequency n-grams will be "
             + "returned by the UDAF. The optional precision factor 'pf' specifies how much "
             + "memory to use for estimation; more memory will give more accurate frequency "
             + "counts, but could crash the JVM. The default value is 20, which internally "
             + "maintains 20*k n-grams, but only returns the k highest frequency ones. "
             + "The output is an array of structs with the top-k n-grams. It might be convenient "
             + "to explode() the output of this UDAF.")
public class GenericUDAFnGrams implements GenericUDAFResolver {
  static final Log LOG = LogFactory.getLog(GenericUDAFnGrams.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 3 && parameters.length != 4) {
      throw new UDFArgumentTypeException(parameters.length-1,
          "Please specify either three or four arguments.");
    }
    
    // Validate the first parameter, which is the expression to compute over. This should be an
    // array of strings type, or an array of arrays of strings.
    PrimitiveTypeInfo pti;
    if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
      throw new UDFArgumentTypeException(0,
          "Only list type arguments are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1.");
    }
    switch (((ListTypeInfo) parameters[0]).getListElementTypeInfo().getCategory()) {
    case PRIMITIVE:
      // Parameter 1 was an array of primitives, so make sure the primitives are strings.
      pti = (PrimitiveTypeInfo) ((ListTypeInfo) parameters[0]).getListElementTypeInfo();
      break;

    case LIST:
      // Parameter 1 was an array of arrays, so make sure that the inner arrays contain
      // primitive strings.
      ListTypeInfo lti = (ListTypeInfo)
                         ((ListTypeInfo) parameters[0]).getListElementTypeInfo();
      pti = (PrimitiveTypeInfo) lti.getListElementTypeInfo();
      break;

    default:
      throw new UDFArgumentTypeException(0,
          "Only arrays of strings or arrays of arrays of strings are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1.");
    }
    if(pti.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
      throw new UDFArgumentTypeException(0,
          "Only array<string> or array<array<string>> is allowed, but " 
          + parameters[0].getTypeName() + " was passed as parameter 1.");
    }

    // Validate the second parameter, which should be an integer
    if(parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1, "Only integers are accepted but "
          + parameters[1].getTypeName() + " was passed as parameter 2.");
    } 
    switch(((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      break;

    default:
      throw new UDFArgumentTypeException(1, "Only integers are accepted but "
          + parameters[1].getTypeName() + " was passed as parameter 2.");
    }

    // Validate the third parameter, which should also be an integer
    if(parameters[2].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(2, "Only integers are accepted but "
            + parameters[2].getTypeName() + " was passed as parameter 3.");
    } 
    switch(((PrimitiveTypeInfo) parameters[2]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      break;

    default:
      throw new UDFArgumentTypeException(2, "Only integers are accepted but "
            + parameters[2].getTypeName() + " was passed as parameter 3.");
    }

    // If we have the optional fourth parameter, make sure it's also an integer
    if(parameters.length == 4) {
      if(parameters[3].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(3, "Only integers are accepted but "
            + parameters[3].getTypeName() + " was passed as parameter 4.");
      } 
      switch(((PrimitiveTypeInfo) parameters[3]).getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        break;

      default:
        throw new UDFArgumentTypeException(3, "Only integers are accepted but "
            + parameters[3].getTypeName() + " was passed as parameter 4.");
      }
    }

    return new GenericUDAFnGramEvaluator();
  }

  /**
   * A constant-space heuristic to estimate the top-k n-grams.
   */
  public static class GenericUDAFnGramEvaluator extends GenericUDAFEvaluator {
    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private StandardListObjectInspector outerInputOI;
    private StandardListObjectInspector innerInputOI;
    private PrimitiveObjectInspector inputOI;
    private PrimitiveObjectInspector nOI;
    private PrimitiveObjectInspector kOI;
    private PrimitiveObjectInspector pOI;

    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations 
    private StandardListObjectInspector loi;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // Init input object inspectors
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        outerInputOI = (StandardListObjectInspector) parameters[0];
        if(outerInputOI.getListElementObjectInspector().getCategory() ==
            ObjectInspector.Category.LIST) {
          // We're dealing with input that is an array of arrays of strings
          innerInputOI = (StandardListObjectInspector) outerInputOI.getListElementObjectInspector();
          inputOI = (PrimitiveObjectInspector) innerInputOI.getListElementObjectInspector();
        } else {
          // We're dealing with input that is an array of strings
          inputOI = (PrimitiveObjectInspector) outerInputOI.getListElementObjectInspector();
          innerInputOI = null;
        }
        nOI = (PrimitiveObjectInspector) parameters[1];
        kOI = (PrimitiveObjectInspector) parameters[2];
        if(parameters.length == 4) {
          pOI = (PrimitiveObjectInspector) parameters[3];
        } else {
          pOI = null;
        }
      } else {
          // Init the list object inspector for handling partial aggregations
          loi = (StandardListObjectInspector) parameters[0];
      }

      // Init output object inspectors.
      //
      // The return type for a partial aggregation is still a list of strings.
      // 
      // The return type for FINAL and COMPLETE is a full aggregation result, which is 
      // an array of structures containing the n-gram and its estimated frequency.
      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        return ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      } else {
        // Final return type that goes back to Hive: a list of structs with n-grams and their
        // estimated frequencies.
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                  PrimitiveObjectInspectorFactory.writableStringObjectInspector));
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("ngram");
        fname.add("estfrequency");               
        return ObjectInspectorFactory.getStandardListObjectInspector(
                 ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi) );
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if(partial == null) { 
        return;
      }
      NGramAggBuf myagg = (NGramAggBuf) agg;

      ArrayList partialNGrams = (ArrayList) loi.getList(partial);
      int k = Integer.parseInt(((Text)partialNGrams.get(0)).toString());
      int n = Integer.parseInt(((Text)partialNGrams.get(1)).toString());
      int pf = Integer.parseInt(((Text)partialNGrams.get(2)).toString());
      if(myagg.k > 0 && myagg.k != k) {
        throw new HiveException(getClass().getSimpleName() + ": mismatch in value for 'k'" 
            + ", which usually is caused by a non-constant expression. Found '"+k+"' and '"
            + myagg.k + "'.");
      }
      if(myagg.n > 0 && myagg.n != n) {
        throw new HiveException(getClass().getSimpleName() + ": mismatch in value for 'n'" 
            + ", which usually is caused by a non-constant expression. Found '"+n+"' and '"
            + myagg.n + "'.");
      }
      if(myagg.pf > 0 && myagg.pf != pf) {
        throw new HiveException(getClass().getSimpleName() + ": mismatch in value for 'pf'" 
            + ", which usually is caused by a non-constant expression. Found '"+pf+"' and '"
            + myagg.pf + "'.");
      }
      myagg.k = k;
      myagg.n = n;
      myagg.pf = pf;

      for(int i = 3; i < partialNGrams.size(); i++) {
        ArrayList<String> key = new ArrayList<String>();
        for(int j = 0; j < n; j++) {
          key.add(((Text)partialNGrams.get(i+j)).toString());
        }
        i += n;
        double val = Double.parseDouble( ((Text)partialNGrams.get(i)).toString() );
        Double myval = (Double)myagg.ngrams.get(key);
        if(myval == null) {
          myval = new Double(val);
        } else {
          myval += val;
        }
        myagg.ngrams.put(key, myval);
      }
      trim(myagg, myagg.k*myagg.pf);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      NGramAggBuf myagg = (NGramAggBuf) agg;

      ArrayList<Text> result = new ArrayList<Text>();
      result.add(new Text(Integer.toString(myagg.k)));
      result.add(new Text(Integer.toString(myagg.n)));
      result.add(new Text(Integer.toString(myagg.pf)));
      for(Iterator<ArrayList<String> > it = myagg.ngrams.keySet().iterator(); it.hasNext(); ) {
        ArrayList<String> mykey = it.next();
        for(int i = 0; i < mykey.size(); i++) {
          result.add(new Text(mykey.get(i)));
        }
        Double myval = (Double) myagg.ngrams.get(mykey);
        result.add(new Text(myval.toString()));
      }

      return result;
    }

    private void trim(NGramAggBuf agg, int N) {
      ArrayList list = new ArrayList(agg.ngrams.entrySet());
      if(list.size() <= N) {
        return;
      }
      Collections.sort(list, new Comparator() {
          public int compare(Object o1, Object o2) {
          return ((Double)((Map.Entry)o1).getValue()).compareTo(
            ((Double)((Map.Entry)o2).getValue()) );
          }
          });
      for(int i = 0; i < list.size() - N; i++) {
        agg.ngrams.remove( ((Map.Entry)list.get(i)).getKey() );
      }
    }

    private void processNgrams(NGramAggBuf agg, ArrayList<String> seq) {
      for(int i = seq.size()-agg.n; i >= 0; i--) {
        ArrayList<String> ngram = new ArrayList<String>();
        for(int j = 0; j < agg.n; j++)  {
          ngram.add(seq.get(i+j));
        }
        Double curVal = (Double) agg.ngrams.get(ngram);
        if(curVal == null) {
          // new n-gram
          curVal = new Double(1);
        } else {
          // existing n-gram, just increment count
          curVal++;
        }
        agg.ngrams.put(ngram, curVal);
      }

      // do we have too many ngrams? 
      if(agg.ngrams.size() > agg.k * agg.pf) {
        // delete low-support n-grams
        trim(agg, agg.k * agg.pf);
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 3);
      if(parameters[0] == null || parameters[1] == null || parameters[2] == null) {
        return;
      }
      NGramAggBuf myagg = (NGramAggBuf) agg;
    
      // Parse out 'n' and 'k' if we haven't already done so, and while we're at it,
      // also parse out the precision factor 'pf' if the user has supplied one.
      if(myagg.n == 0 || myagg.k == 0) {
        myagg.n = PrimitiveObjectInspectorUtils.getInt(parameters[1], nOI);
        myagg.k = PrimitiveObjectInspectorUtils.getInt(parameters[2], kOI);
        if(myagg.n < 1) {
          throw new HiveException(getClass().getSimpleName() + " needs 'n' to be at least 1, "
                                  + "but you supplied " + myagg.n);
        }
        if(myagg.k < 1) {
          throw new HiveException(getClass().getSimpleName() + " needs 'k' to be at least 1, "
                                  + "but you supplied " + myagg.k);
        }
        if(parameters.length == 4) {
          myagg.pf = PrimitiveObjectInspectorUtils.getInt(parameters[3], pOI);
          if(myagg.pf < 1) {
            throw new HiveException(getClass().getSimpleName() + " needs 'pf' to be at least 1, "
                + "but you supplied " + myagg.pf);
          }
        }

        // Enforce a minimum n-gram buffer size
        if(myagg.pf*myagg.k < 1000) {
          myagg.pf = 1000 / myagg.k;
        }
      }

      // get the input expression
      ArrayList outer = (ArrayList) outerInputOI.getList(parameters[0]);
      if(innerInputOI != null) {
        // we're dealing with an array of arrays of strings
        for(int i = 0; i < outer.size(); i++) {
          ArrayList inner = (ArrayList) innerInputOI.getList(outer.get(i));
          ArrayList<String> words = new ArrayList<String>();
          for(int j = 0; j < inner.size(); j++) {
            String word = PrimitiveObjectInspectorUtils.getString(inner.get(j), inputOI);
            words.add(word);
          }

          // parse out n-grams, update frequency counts
          processNgrams(myagg, words);
        } 
      } else {
        // we're dealing with an array of strings
        ArrayList<String> words = new ArrayList<String>();
        for(int i = 0; i < outer.size(); i++) {
          String word = PrimitiveObjectInspectorUtils.getString(outer.get(i), inputOI);
          words.add(word);
        }

        // parse out n-grams, update frequency counts
        processNgrams(myagg, words);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      NGramAggBuf myagg = (NGramAggBuf) agg;
      if (myagg.ngrams.size() < 1) { // SQL standard - return null for zero elements
        return null;
      } 

      ArrayList<Object[]> result = new ArrayList<Object[]>();

      ArrayList list = new ArrayList(myagg.ngrams.entrySet());
      Collections.sort(list, new Comparator() {
          public int compare(Object o1, Object o2) {
          return ((Double)((Map.Entry)o2).getValue()).compareTo(
            ((Double)((Map.Entry)o1).getValue()) );
          }
          });

      for(int i = 0; i < list.size() && i < myagg.k; i++) {
        ArrayList<String> key = (ArrayList<String>)((Map.Entry)list.get(i)).getKey();
        Double val = (Double)((Map.Entry)list.get(i)).getValue();

        Object[] ngram = new Object[2];
        ngram[0] = new ArrayList<Text>();
        for(int j = 0; j < key.size(); j++) {
          ((ArrayList<Text>)ngram[0]).add(new Text(key.get(j)));
        }
        ngram[1] = new DoubleWritable(val.doubleValue());
        result.add(ngram);
      }

      return result;
    }


    // Aggregation buffer methods. 
    static class NGramAggBuf implements AggregationBuffer {
      HashMap ngrams;
      int n;
      int k;
      int pf;
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      NGramAggBuf result = new NGramAggBuf();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      NGramAggBuf result = (NGramAggBuf) agg;
      result.ngrams = new HashMap();
      result.n = result.k = result.pf = 0;
    }
  }
}
