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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
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
 * Estimates the top-k contextual n-grams in arbitrary sequential data using a heuristic.
 */
@Description(name = "context_ngrams",
    value = "_FUNC_(expr, array<string1, string2, ...>, k, pf) estimates the top-k most " +
      "frequent n-grams that fit into the specified context. The second parameter specifies " +
      "a string of words that specify the positions of the n-gram elements, with a null value " +
      "standing in for a 'blank' that must be filled by an n-gram element.",
    extended = "The primary expression must be an array of strings, or an array of arrays of " +
      "strings, such as the return type of the sentences() UDF. The second parameter specifies " +
      "the context -- for example, array(\"i\", \"love\", null) -- which would estimate the top " +
      "'k' words that follow the phrase \"i love\" in the primary expression. The optional " +
      "fourth parameter 'pf' controls the memory used by the heuristic. Larger values will " +
      "yield better accuracy, but use more memory. Example usage:\n" +
      "  SELECT context_ngrams(sentences(lower(review)), array(\"i\", \"love\", null, null), 10)" +
      " FROM movies\n" +
      "would attempt to determine the 10 most common two-word phrases that follow \"i love\" " +
      "in a database of free-form natural language movie reviews.")
public class GenericUDAFContextNGrams implements GenericUDAFResolver {
  static final Log LOG = LogFactory.getLog(GenericUDAFContextNGrams.class.getName());

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

    // Validate the second parameter, which should be an array of strings
    if(parameters[1].getCategory() != ObjectInspector.Category.LIST ||
       ((ListTypeInfo) parameters[1]).getListElementTypeInfo().getCategory() !=
         ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1, "Only arrays of strings are accepted but "
          + parameters[1].getTypeName() + " was passed as parameter 2.");
    }
    if(((PrimitiveTypeInfo) ((ListTypeInfo)parameters[1]).getListElementTypeInfo()).
        getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
      throw new UDFArgumentTypeException(1, "Only arrays of strings are accepted but "
          + parameters[1].getTypeName() + " was passed as parameter 2.");
    }

    // Validate the third parameter, which should be an integer to represent 'k'
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

    // If the fourth parameter -- precision factor 'pf' -- has been specified, make sure it's
    // an integer.
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

    return new GenericUDAFContextNGramEvaluator();
  }

  /**
   * A constant-space heuristic to estimate the top-k contextual n-grams.
   */
  public static class GenericUDAFContextNGramEvaluator extends GenericUDAFEvaluator {
    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private transient StandardListObjectInspector outerInputOI;
    private transient StandardListObjectInspector innerInputOI;
    private transient StandardListObjectInspector contextListOI;
    private PrimitiveObjectInspector contextOI;
    private PrimitiveObjectInspector inputOI;
    private transient PrimitiveObjectInspector kOI;
    private transient PrimitiveObjectInspector pOI;

    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
    private transient StandardListObjectInspector loi;

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
        contextListOI = (StandardListObjectInspector) parameters[1];
        contextOI = (PrimitiveObjectInspector) contextListOI.getListElementObjectInspector();
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
    public void merge(AggregationBuffer agg, Object obj) throws HiveException {
      if(obj == null) {
        return;
      }
      NGramAggBuf myagg = (NGramAggBuf) agg;
      List<Text> partial = (List<Text>) loi.getList(obj);

      // remove the context words from the end of the list
      int contextSize = Integer.parseInt( ((Text)partial.get(partial.size()-1)).toString() );
      partial.remove(partial.size()-1);
      if(myagg.context.size() > 0)  {
        if(contextSize != myagg.context.size()) {
          throw new HiveException(getClass().getSimpleName() + ": found a mismatch in the" +
              " context string lengths. This is usually caused by passing a non-constant" +
              " expression for the context.");
        }
      } else {
        for(int i = partial.size()-contextSize; i < partial.size(); i++) {
          String word = partial.get(i).toString();
          if(word.equals("")) {
            myagg.context.add( null );
          } else {
            myagg.context.add( word );
          }
        }
        partial.subList(partial.size()-contextSize, partial.size()).clear();
        myagg.nge.merge(partial);
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      NGramAggBuf myagg = (NGramAggBuf) agg;
      ArrayList<Text> result = myagg.nge.serialize();

      // push the context on to the end of the serialized n-gram estimation
      for(int i = 0; i < myagg.context.size(); i++) {
        if(myagg.context.get(i) == null) {
          result.add(new Text(""));
        } else {
          result.add(new Text(myagg.context.get(i)));
        }
      }
      result.add(new Text(Integer.toString(myagg.context.size())));

      return result;
    }

    // Finds all contextual n-grams in a sequence of words, and passes the n-grams to the
    // n-gram estimator object
    private void processNgrams(NGramAggBuf agg, ArrayList<String> seq) throws HiveException {
      // generate n-grams wherever the context matches
      assert(agg.context.size() > 0);
      ArrayList<String> ng = new ArrayList<String>();
      for(int i = seq.size() - agg.context.size(); i >= 0; i--) {
        // check if the context matches
        boolean contextMatches = true;
        ng.clear();
        for(int j = 0; j < agg.context.size(); j++) {
          String contextWord = agg.context.get(j);
          if(contextWord == null) {
            ng.add(seq.get(i+j));
          } else {
            if(!contextWord.equals(seq.get(i+j))) {
              contextMatches = false;
              break;
            }
          }
        }

        // add to n-gram estimation only if the context matches
        if(contextMatches) {
          agg.nge.add(ng);
          ng = new ArrayList<String>();
        }
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 3 || parameters.length == 4);
      if(parameters[0] == null || parameters[1] == null || parameters[2] == null) {
        return;
      }
      NGramAggBuf myagg = (NGramAggBuf) agg;

      // Parse out the context and 'k' if we haven't already done so, and while we're at it,
      // also parse out the precision factor 'pf' if the user has supplied one.
      if(!myagg.nge.isInitialized()) {
        int k = PrimitiveObjectInspectorUtils.getInt(parameters[2], kOI);
        int pf = 0;
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

        // Parse out the context and make sure it isn't empty
        myagg.context.clear();
        List<Text> context = (List<Text>) contextListOI.getList(parameters[1]);
        int contextNulls = 0;
        for(int i = 0; i < context.size(); i++) {
          String word = PrimitiveObjectInspectorUtils.getString(context.get(i), contextOI);
          if(word == null) {
            contextNulls++;
          }
          myagg.context.add(word);
        }
        if(context.size() == 0) {
          throw new HiveException(getClass().getSimpleName() + " needs a context array " +
            "with at least one element.");
        }
        if(contextNulls == 0) {
          throw new HiveException(getClass().getSimpleName() + " the context array needs to " +
            "contain at least one 'null' value to indicate what should be counted.");
        }

        // Set parameters in the n-gram estimator object
        myagg.nge.initialize(k, pf, contextNulls);
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
      ArrayList<String> context;
      NGramEstimator nge;
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      NGramAggBuf result = new NGramAggBuf();
      result.nge = new NGramEstimator();
      result.context = new ArrayList<String>();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      NGramAggBuf result = (NGramAggBuf) agg;
      result.context.clear();
      result.nge.reset();
    }
  }
}
