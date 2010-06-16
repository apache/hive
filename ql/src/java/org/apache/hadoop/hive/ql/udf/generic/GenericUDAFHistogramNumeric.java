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
import java.util.Arrays;
import java.util.Random;

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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * Computes an approximate histogram of a numerical column using a user-specified number of bins.
 * 
 * The output is an array of (x,y) pairs as Hive struct objects that represents the histogram's
 * bin centers and heights.
 */
@Description(name = "histogram_numeric",
    value = "_FUNC_(expr, nb) - Computes a histogram on numeric 'expr' using nb bins.")
public class GenericUDAFHistogramNumeric implements GenericUDAFResolver {
  // class static variables
  static final Log LOG = LogFactory.getLog(GenericUDAFHistogramNumeric.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Please specify exactly two arguments.");
    }
    
    // validate the first parameter, which is the expression to compute over
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1.");
    }
    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
      break;
    case STRING:
    case BOOLEAN:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric type arguments are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1.");
    }

    // validate the second parameter, which is the number of histogram bins
    if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1,
          "Only primitive type arguments are accepted but "
          + parameters[1].getTypeName() + " was passed as parameter 2.");
    }
    if( ((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()
        != PrimitiveObjectInspector.PrimitiveCategory.INT) {
      throw new UDFArgumentTypeException(1,
          "Only an integer argument is accepted as parameter 2, but "
          + parameters[1].getTypeName() + " was passed instead.");
    }

    return new GenericUDAFHistogramNumericEvaluator();
  }

  /**
   * Construct a histogram using the algorithm described by Ben-Haim and Tom-Tov.
   *
   * The algorithm is a heuristic adapted from the following paper:
   * Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel decision tree algorithm",
   * J. Machine Learning Research 11 (2010), pp. 849--872. Although there are no approximation
   * guarantees, it appears to work well with adequate data and a large (e.g., 20-80) number
   * of histogram bins.
   */
  public static class GenericUDAFHistogramNumericEvaluator extends GenericUDAFEvaluator {

    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private PrimitiveObjectInspector inputOI;
    private PrimitiveObjectInspector nbinsOI;

    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list of doubles)
    private StandardListObjectInspector loi;

    // A PRNG for breaking ties in histogram bin merging
    Random prng;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // init the RNG for breaking ties in histogram merging. A fixed seed is specified here
      // to aid testing, but can be eliminated to use a time-based seed (and have non-deterministic
      // results).
      prng = new Random(31183);

      // init input object inspectors
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        assert(parameters.length == 2);
        inputOI = (PrimitiveObjectInspector) parameters[0];
        nbinsOI = (PrimitiveObjectInspector) parameters[1];
      } else {
        loi = (StandardListObjectInspector) parameters[0];
      }

      // init output object inspectors
      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        // The output of a partial aggregation is a list of doubles representing the
        // histogram being constructed. The first element in the list is the user-specified
        // number of bins in the histogram, and the histogram itself is represented as (x,y)
        // pairs following the first element, so the list length should *always* be odd.
        return ObjectInspectorFactory.getStandardListObjectInspector(
                 PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
      } else {
        // The output of FINAL and COMPLETE is a full aggregation, which is a
        // list of DoubleWritable structs that represent the final histogram as
        // (x,y) pairs of bin centers and heights.
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("x");
        fname.add("y");

        return ObjectInspectorFactory.getStandardListObjectInspector(
                 ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi) );
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      ArrayList<DoubleWritable> partialResult = new ArrayList<DoubleWritable>();
      StdAgg myagg = (StdAgg) agg;

      // Return a single ArrayList where the first element is the number of histogram bins, 
      // and subsequent elements represent histogram (x,y) pairs.
      partialResult.add(new DoubleWritable(myagg.nbins));
      if(myagg.hist != null) {
        for(int i = 0; i < myagg.nusedbins; i++) {
          partialResult.add(new DoubleWritable(myagg.hist[i].x));
          partialResult.add(new DoubleWritable(myagg.hist[i].y));
        }
      }

      return partialResult;
    }


    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg) agg;

      if (myagg.nusedbins < 1) { // SQL standard - return null for zero elements
        return null;
      } else {
        ArrayList<DoubleWritable[]> result = new ArrayList<DoubleWritable[]>();
        for(int i = 0; i < myagg.nusedbins; i++) {
          DoubleWritable[] bin = new DoubleWritable[2];
          bin[0] = new DoubleWritable(myagg.hist[i].x);
          bin[1] = new DoubleWritable(myagg.hist[i].y);
          result.add(bin);
        }
        return result;
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if(partial == null) {
        return;
      }
      ArrayList partialHistogram = (ArrayList) loi.getList(partial);
      StdAgg myagg = (StdAgg) agg;
      
      if(myagg.nbins == 0 || myagg.nusedbins == 0)  {
        // The aggregation buffer has nothing in it, so just copy over 'partial' into myagg
        // by deserializing the ArrayList of (x,y) pairs into an array of Coord objects
        myagg.nbins = (int) ((DoubleWritable) partialHistogram.get(0)).get();
        myagg.nusedbins = (partialHistogram.size()-1)/2; 
        myagg.hist = new StdAgg.Coord[myagg.nbins+1]; // +1 to hold a temporary bin for insert()
        for(int i = 1; i < partialHistogram.size(); i+=2) {
          myagg.hist[(i-1)/2] = new StdAgg.Coord();
          myagg.hist[(i-1)/2].x = ((DoubleWritable) partialHistogram.get(i)).get();
          myagg.hist[(i-1)/2].y = ((DoubleWritable) partialHistogram.get(i+1)).get();
        }
      } else {
        // The aggregation buffer already contains a partial histogram. Therefore, we need
        // to merge histograms using Algorithm #2 from the Ben-Haim and Tom-Tov paper.
        StdAgg.Coord[] tmp_histogram = new StdAgg.Coord[myagg.nusedbins
                                                        + (partialHistogram.size()-1)/2];
        for(int j = 0; j < tmp_histogram.length; j++) {
          tmp_histogram[j] = new StdAgg.Coord();
        }

        // Copy all the histogram bins from 'myagg' and 'partial' into an overstuffed histogram
        int i;
        for(i = 0; i < myagg.nusedbins; i++) {
          tmp_histogram[i].x = myagg.hist[i].x;
          tmp_histogram[i].y = myagg.hist[i].y;
        }
        for(int j = 1; j < partialHistogram.size(); j+=2, i++) {
          tmp_histogram[i].x = ((DoubleWritable) partialHistogram.get(j)).get();
          tmp_histogram[i].y = ((DoubleWritable) partialHistogram.get(j+1)).get();
        }
        Arrays.sort(tmp_histogram);

        // Now trim the overstuffed histogram down to the correct number of bins
        myagg.hist = tmp_histogram;
        myagg.nusedbins += (partialHistogram.size()-1)/2;
        trim(myagg);
      }
    }

    // Algorithm #1 from the Ben-Haim and Tom-Tov paper: histogram update procedure for a single
    // new data point 'v'.
    private void insert(StdAgg myagg, double v) {
      StdAgg.Coord[] histogram = myagg.hist;

      // Binary search to find the closest bucket that v should go into.
      // 'bin' should be interpreted as the bin to shift right in order to accomodate
      // v. As a result, bin is in the range [0,N], where N means that the value v is
      // greater than all the N bins currently in the histogram. It is also possible that
      // a bucket centered at 'v' already exists, so this must be checked in the next step.
      int bin = 0;
      for(int l=0, r=myagg.nusedbins; l < r; ) {
        bin = (l+r)/2;
        if(histogram[bin].x > v) {
          r = bin;
        } else {
          if(histogram[bin].x < v) {
            l = ++bin;
          } else {
            break; // break loop on equal comparator
          }
        }
      }

      // If we found an exact bin match for value v, then just increment that bin's count.
      // Otherwise, we need to insert a new bin and trim the resulting histogram back to size.
      // A possible optimization here might be to set some threshold under which 'v' is just
      // assumed to be equal to the closest bin -- if fabs(v-histogram[bin].x) < THRESHOLD, then
      // just increment 'bin'
      if(bin < myagg.nusedbins && histogram[bin].x == v) {
        histogram[bin].y++;
      } else {
        for(int i = myagg.nusedbins; i > bin; i--) {
          myagg.hist[i].x = myagg.hist[i-1].x;
          myagg.hist[i].y = myagg.hist[i-1].y;
        }
        myagg.hist[bin].x = v; // new histogram bin for value 'v'
        myagg.hist[bin].y = 1; // of height 1 unit

        // Trim the histogram down to the correct number of bins.
        if(++myagg.nusedbins > myagg.nbins) {
          trim(myagg);
        }
      }
    }

    // Trims a histogram down to 'nbins' bins by iteratively merging the closest bins.
    // If two pairs of bins are equally close to each other, decide uniformly at random which
    // pair to merge, based on a PRNG.
    private void trim(StdAgg myagg) {
      // Ensure that there are at least 3 histogram bins (because nbins>=2).
      if(myagg.nusedbins <= myagg.nbins) {
        return;
      }
      StdAgg.Coord[] histogram = myagg.hist;

      while(myagg.nusedbins > myagg.nbins) {
        // Find the closest histogram bins in terms of x coordinates. Break ties randomly.
        double smallestdiff = histogram[1].x - histogram[0].x;
        int smallestdiffloc = 0, smallestdiffcount = 1;
        for(int i = 1; i < myagg.nusedbins-1; i++) {
          double diff = histogram[i+1].x - histogram[i].x;
          if(diff < smallestdiff)  {
            smallestdiff = diff;
            smallestdiffloc = i;
            smallestdiffcount = 1;
          } else {
            if(diff == smallestdiff && prng.nextDouble() <= (1.0/++smallestdiffcount) ) {
                smallestdiffloc = i;
            }
          }
        }

        // Merge the two closest bins into their average x location, weighted by their heights.
        // The height of the new bin is the sum of the heights of the old bins.
        double d = histogram[smallestdiffloc].y + histogram[smallestdiffloc+1].y;
        histogram[smallestdiffloc].x *= histogram[smallestdiffloc].y / d;
        histogram[smallestdiffloc].x += histogram[smallestdiffloc+1].x / d *
                                        histogram[smallestdiffloc+1].y;
        histogram[smallestdiffloc].y = d;

        // Shift the remaining bins left one position
        for(int i = smallestdiffloc+1; i < myagg.nusedbins-1; i++) {
          histogram[i].x = histogram[i+1].x;
          histogram[i].y = histogram[i+1].y;
        }
        myagg.nusedbins--;      
      }
    }

    private boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 2);
      if(parameters[0] == null || parameters[1] == null) {
        return;
      }
      StdAgg myagg = (StdAgg) agg;

      // Parse out the number of histogram bins only once, if we haven't already done
      // so before. We need at least 2 bins; otherwise, there is no point in creating
      // a histogram.
      if(myagg.nbins == 0) {
        try {
          myagg.nbins = PrimitiveObjectInspectorUtils.getInt(parameters[1], nbinsOI);
        } catch(NumberFormatException e) {
          throw new HiveException(getClass().getSimpleName() + " " + 
                                  StringUtils.stringifyException(e));
        }
        if(myagg.nbins < 2) {
          throw new HiveException(getClass().getSimpleName() + " needs nbins to be at least 2,"
                                  + " but you supplied " + myagg.nbins + ".");
        }

        // allocate memory for the histogram 
        myagg.hist = new StdAgg.Coord[myagg.nbins+1]; // +1 is used for holding a temporary bin
        for(int i = 0; i < myagg.nbins+1; i++) {
          myagg.hist[i] = new StdAgg.Coord();
        }
        myagg.nusedbins = 0;
      }

      // Process the current data point
      Object p = parameters[0];
      if (p != null) {
        double v = PrimitiveObjectInspectorUtils.getDouble(p, inputOI);
        insert(myagg, v);
      }
    }


    // Aggregation buffer definition and manipulation methods 
    static class StdAgg implements AggregationBuffer {
      static class Coord implements Comparable {
        double x;
        double y;

        public int compareTo(Object other) {
          Coord o = (Coord) other;
          if(x < o.x) {
            return -1;
          }
          if(x > o.x) {
            return 1;
          }
          return 0;
        }
      };

      int nbins; // maximum number of histogram bins
      int nusedbins; // number of histogram bins actually used
      Coord[] hist; // histogram coordinates
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      StdAgg result = new StdAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg) agg;
      myagg.nbins = 0;
      myagg.nusedbins = 0;
      myagg.hist = null;
    }
  }
}
