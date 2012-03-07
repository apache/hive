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
import java.util.Arrays;
import java.util.Random;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;


/**
 * A generic, re-usable histogram class that supports partial aggregations.
 * The algorithm is a heuristic adapted from the following paper:
 * Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel decision tree algorithm",
 * J. Machine Learning Research 11 (2010), pp. 849--872. Although there are no approximation
 * guarantees, it appears to work well with adequate data and a large (e.g., 20-80) number
 * of histogram bins.
 */
public class NumericHistogram {
  /**
   * The Coord class defines a histogram bin, which is just an (x,y) pair.
   */
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

  // Class variables
  private int nbins;
  private int nusedbins;
  private Coord[] bins;
  private Random prng;

  /**
   * Creates a new histogram object. Note that the allocate() or merge()
   * method must be called before the histogram can be used.
   */
  public NumericHistogram() {
    nbins = 0;
    nusedbins = 0;
    bins = null;

    // init the RNG for breaking ties in histogram merging. A fixed seed is specified here
    // to aid testing, but can be eliminated to use a time-based seed (which would
    // make the algorithm non-deterministic).
    prng = new Random(31183);
  }

  /**
   * Resets a histogram object to its initial state. allocate() or merge() must be
   * called again before use.
   */
  public void reset() {
    bins = null;
    nbins = nusedbins = 0;
  }

  /**
   * Returns the number of bins currently being used by the histogram.
   */
  public int getUsedBins() {
    return nusedbins;
  }

  /**
   * Returns true if this histogram object has been initialized by calling merge()
   * or allocate().
   */
  public boolean isReady() {
    return nbins != 0;
  }

  /**
   * Returns a particular histogram bin.
   */
  public Coord getBin(int b) {
    return bins[b];
  }

  /**
   * Sets the number of histogram bins to use for approximating data.
   *
   * @param num_bins Number of non-uniform-width histogram bins to use
   */
  public void allocate(int num_bins) {
    nbins = num_bins;
    bins = new Coord[nbins+1];
    for(int i = 0; i < nbins+1; i++) {
      bins[i] = new Coord();
    }
    nusedbins = 0;
  }

  /**
   * Takes a serialized histogram created by the serialize() method and merges
   * it with the current histogram object.
   *
   * @param other A serialized histogram created by the serialize() method
   * @see #merge
   */
  public void merge(List<DoubleWritable> other) {
    if(other == null) {
      return;
    }

    if(nbins == 0 || nusedbins == 0)  {
      // Our aggregation buffer has nothing in it, so just copy over 'other'
      // by deserializing the ArrayList of (x,y) pairs into an array of Coord objects
      nbins = (int) other.get(0).get();
      nusedbins = (other.size()-1)/2;
      bins = new Coord[nbins+1]; // +1 to hold a temporary bin for insert()
      for(int i = 1; i < other.size(); i+=2) {
        bins[(i-1)/2] = new Coord();
        bins[(i-1)/2].x = other.get(i).get();
        bins[(i-1)/2].y = other.get(i+1).get();
      }
    } else {
      // The aggregation buffer already contains a partial histogram. Therefore, we need
      // to merge histograms using Algorithm #2 from the Ben-Haim and Tom-Tov paper.
      Coord[] tmp_bins = new Coord[nusedbins + (other.size()-1)/2];
      for(int j = 0; j < tmp_bins.length; j++) {
        tmp_bins[j] = new Coord();
      }

      // Copy all the histogram bins from us and 'other' into an overstuffed histogram
      int i;
      for(i = 0; i < nusedbins; i++) {
        tmp_bins[i].x = bins[i].x;
        tmp_bins[i].y = bins[i].y;
      }
      for(int j = 1; j < other.size(); j+=2, i++) {
        tmp_bins[i].x = other.get(j).get();
        tmp_bins[i].y = other.get(j+1).get();
      }
      Arrays.sort(tmp_bins);

      // Now trim the overstuffed histogram down to the correct number of bins
      bins = tmp_bins;
      nusedbins += (other.size()-1)/2;
      trim();
    }
  }


  /**
   * Adds a new data point to the histogram approximation. Make sure you have
   * called either allocate() or merge() first. This method implements Algorithm #1
   * from Ben-Haim and Tom-Tov, "A Streaming Parallel Decision Tree Algorithm", JMLR 2010.
   *
   * @param v The data point to add to the histogram approximation.
   */
  public void add(double v) {
    // Binary search to find the closest bucket that v should go into.
    // 'bin' should be interpreted as the bin to shift right in order to accomodate
    // v. As a result, bin is in the range [0,N], where N means that the value v is
    // greater than all the N bins currently in the histogram. It is also possible that
    // a bucket centered at 'v' already exists, so this must be checked in the next step.
    int bin = 0;
    for(int l=0, r=nusedbins; l < r; ) {
      bin = (l+r)/2;
      if(bins[bin].x > v) {
        r = bin;
      } else {
        if(bins[bin].x < v) {
          l = ++bin;
        } else {
          break; // break loop on equal comparator
        }
      }
    }

    // If we found an exact bin match for value v, then just increment that bin's count.
    // Otherwise, we need to insert a new bin and trim the resulting histogram back to size.
    // A possible optimization here might be to set some threshold under which 'v' is just
    // assumed to be equal to the closest bin -- if fabs(v-bins[bin].x) < THRESHOLD, then
    // just increment 'bin'. This is not done now because we don't want to make any
    // assumptions about the range of numeric data being analyzed.
    if(bin < nusedbins && bins[bin].x == v) {
      bins[bin].y++;
    } else {
      for(int i = nusedbins; i > bin; i--) {
        bins[i].x = bins[i-1].x;
        bins[i].y = bins[i-1].y;
      }
      bins[bin].x = v; // new bins bin for value 'v'
      bins[bin].y = 1; // of height 1 unit

      // Trim the bins down to the correct number of bins.
      if(++nusedbins > nbins) {
        trim();
      }
    }
  }

  /**
   * Trims a histogram down to 'nbins' bins by iteratively merging the closest bins.
   * If two pairs of bins are equally close to each other, decide uniformly at random which
   * pair to merge, based on a PRNG.
   */
  private void trim() {
    while(nusedbins > nbins) {
      // Find the closest pair of bins in terms of x coordinates. Break ties randomly.
      double smallestdiff = bins[1].x - bins[0].x;
      int smallestdiffloc = 0, smallestdiffcount = 1;
      for(int i = 1; i < nusedbins-1; i++) {
        double diff = bins[i+1].x - bins[i].x;
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
      double d = bins[smallestdiffloc].y + bins[smallestdiffloc+1].y;
      bins[smallestdiffloc].x *= bins[smallestdiffloc].y / d;
      bins[smallestdiffloc].x += bins[smallestdiffloc+1].x / d *
        bins[smallestdiffloc+1].y;
      bins[smallestdiffloc].y = d;

      // Shift the remaining bins left one position
      for(int i = smallestdiffloc+1; i < nusedbins-1; i++) {
        bins[i].x = bins[i+1].x;
        bins[i].y = bins[i+1].y;
      }
      nusedbins--;
    }
  }

  /**
   * Gets an approximate quantile value from the current histogram. Some popular
   * quantiles are 0.5 (median), 0.95, and 0.98.
   *
   * @param q The requested quantile, must be strictly within the range (0,1).
   * @return The quantile value.
   */
  public double quantile(double q) {
    assert(bins != null && nusedbins > 0 && nbins > 0);
    double sum = 0, csum = 0;
    int b;
    for(b = 0; b < nusedbins; b++)  {
      sum += bins[b].y;
    }
    for(b = 0; b < nusedbins; b++) {
      csum += bins[b].y;
      if(csum / sum >= q) {
        if(b == 0) {
          return bins[b].x;
        }
        csum -= bins[b].y;
        double r = bins[b-1].x + (q*sum - csum) * (bins[b].x-bins[b-1].x)/(bins[b].y);
        return r;
      }
    }
    return -1; // for Xlint, code will never reach here
  }

  /**
   * In preparation for a Hive merge() call, serializes the current histogram object into an
   * ArrayList of DoubleWritable objects. This list is deserialized and merged by the
   * merge method.
   *
   * @return An ArrayList of Hadoop DoubleWritable objects that represents the current
   * histogram.
   * @see #merge
   */
  public ArrayList<DoubleWritable> serialize() {
    ArrayList<DoubleWritable> result = new ArrayList<DoubleWritable>();

    // Return a single ArrayList where the first element is the number of bins bins,
    // and subsequent elements represent bins (x,y) pairs.
    result.add(new DoubleWritable(nbins));
    if(bins != null) {
      for(int i = 0; i < nusedbins; i++) {
        result.add(new DoubleWritable(bins[i].x));
        result.add(new DoubleWritable(bins[i].y));
      }
    }

    return result;
  }
}
