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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
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
  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFnGrams.class.getName());

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
    case TIMESTAMP:
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
    case TIMESTAMP:
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
      case TIMESTAMP:
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
    private transient ListObjectInspector outerInputOI;
    private transient StandardListObjectInspector innerInputOI;
    private transient PrimitiveObjectInspector inputOI;
    private transient PrimitiveObjectInspector nOI;
    private transient PrimitiveObjectInspector kOI;
    private transient PrimitiveObjectInspector pOI;

    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
    private transient ListObjectInspector loi;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // Init input object inspectors
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        outerInputOI = (ListObjectInspector) parameters[0];
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
          loi = (ListObjectInspector) parameters[0];
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
      List partialNGrams = (List) loi.getList(partial);
      int n = Integer.parseInt(partialNGrams.get(partialNGrams.size()-1).toString());

      // A value of 0 for n indicates that the mapper processed data that does not meet
      // filter criteria, so merge() should be NO-OP.
      if (n == 0) {
        return;
      }

      if(myagg.n > 0 && myagg.n != n) {
        throw new HiveException(getClass().getSimpleName() + ": mismatch in value for 'n'"
            + ", which usually is caused by a non-constant expression. Found '"+n+"' and '"
            + myagg.n + "'.");
      }
      myagg.n = n;
      partialNGrams.remove(partialNGrams.size()-1);
      myagg.nge.merge(partialNGrams);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      NGramAggBuf myagg = (NGramAggBuf) agg;
      ArrayList<Text> result = myagg.nge.serialize();
      result.add(new Text(Integer.toString(myagg.n)));
      return result;
    }

    private void processNgrams(NGramAggBuf agg, ArrayList<String> seq) throws HiveException {
      for(int i = seq.size()-agg.n; i >= 0; i--) {
        ArrayList<String> ngram = new ArrayList<String>();
        for(int j = 0; j < agg.n; j++)  {
          ngram.add(seq.get(i+j));
        }
        agg.nge.add(ngram);
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 3 || parameters.length == 4);
      if(parameters[0] == null || parameters[1] == null || parameters[2] == null) {
        return;
      }
      NGramAggBuf myagg = (NGramAggBuf) agg;

      // Parse out 'n' and 'k' if we haven't already done so, and while we're at it,
      // also parse out the precision factor 'pf' if the user has supplied one.
      if(!myagg.nge.isInitialized()) {
        int n = PrimitiveObjectInspectorUtils.getInt(parameters[1], nOI);
        int k = PrimitiveObjectInspectorUtils.getInt(parameters[2], kOI);
        int pf = 0;
        if(n < 1) {
          throw new HiveException(getClass().getSimpleName() + " needs 'n' to be at least 1, "
                                  + "but you supplied " + n);
        }
        if(k < 1) {
          throw new HiveException(getClass().getSimpleName() + " needs 'k' to be at least 1, "
                                  + "but you supplied " + k);
        }
        if(parameters.length == 4) {
          pf = PrimitiveObjectInspectorUtils.getInt(parameters[3], pOI);
          if(pf < 1) {
            throw new HiveException(getClass().getSimpleName() + " needs 'pf' to be at least 1, "
                + "but you supplied " + pf);
          }
        } else {
          pf = 1; // placeholder; minimum pf value is enforced in NGramEstimator
        }

        // Set the parameters
        myagg.n = n;
        myagg.nge.initialize(k, pf, n);
      }

      // get the input expression
      List<Text> outer = (List<Text>) outerInputOI.getList(parameters[0]);
      if(innerInputOI != null) {
        // we're dealing with an array of arrays of strings
        for(int i = 0; i < outer.size(); i++) {
          List<Text> inner = (List<Text>) innerInputOI.getList(outer.get(i));
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
      return myagg.nge.getNGrams();
    }

    // Aggregation buffer methods.
    static class NGramAggBuf extends AbstractAggregationBuffer {
      NGramEstimator nge;
      int n;
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      NGramAggBuf result = new NGramAggBuf();
      result.nge = new NGramEstimator();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      NGramAggBuf result = (NGramAggBuf) agg;
      result.nge.reset();
      result.n = 0;
    }
  }
}
