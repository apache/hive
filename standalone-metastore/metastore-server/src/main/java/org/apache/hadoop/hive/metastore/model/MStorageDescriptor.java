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

package org.apache.hadoop.hive.metastore.model;

import java.util.List;
import java.util.Map;

public class MStorageDescriptor {
  private MColumnDescriptor cd;
  private String location;
  private String inputFormat;
  private String outputFormat;
  private boolean isCompressed = false;
  private int numBuckets = 1;
  private MSerDeInfo serDeInfo;
  private List<String> bucketCols;
  private List<MOrder> sortCols;
  private Map<String, String> parameters;
  private List<String> skewedColNames;
  private List<MStringList> skewedColValues;
  private Map<MStringList, String> skewedColValueLocationMaps;
  private boolean isStoredAsSubDirectories;

  public MStorageDescriptor() {}


  /**
   * @param cd
   * @param location
   * @param inputFormat
   * @param outputFormat
   * @param isCompressed
   * @param numBuckets
   * @param serDeInfo
   * @param bucketCols
   * @param sortOrder
   * @param parameters
   */
  public MStorageDescriptor(MColumnDescriptor cd, String location, String inputFormat,
      String outputFormat, boolean isCompressed, int numBuckets, MSerDeInfo serDeInfo,
      List<String> bucketCols, List<MOrder> sortOrder, Map<String, String> parameters,
      List<String> skewedColNames, List<MStringList> skewedColValues,
      Map<MStringList, String> skewedColValueLocationMaps, boolean storedAsSubDirectories) {
    this.cd = cd;
    this.location = location;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.isCompressed = isCompressed;
    this.numBuckets = numBuckets;
    this.serDeInfo = serDeInfo;
    this.bucketCols = bucketCols;
    this.sortCols = sortOrder;
    this.parameters = parameters;
    this.skewedColNames = skewedColNames;
    this.skewedColValues = skewedColValues;
    this.skewedColValueLocationMaps = skewedColValueLocationMaps;
    this.isStoredAsSubDirectories = storedAsSubDirectories;
  }


  /**
   * @return the location
   */
  public String getLocation() {
    return location;
  }

  /**
   * @param location the location to set
   */
  public void setLocation(String location) {
    this.location = location;
  }

  /**
   * @return the isCompressed
   */
  public boolean isCompressed() {
    return isCompressed;
  }

  /**
   * @param isCompressed the isCompressed to set
   */
  public void setCompressed(boolean isCompressed) {
    this.isCompressed = isCompressed;
  }

  /**
   * @return the numBuckets
   */
  public int getNumBuckets() {
    return numBuckets;
  }

  /**
   * @param numBuckets the numBuckets to set
   */
  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  /**
   * @return the bucketCols
   */
  public List<String> getBucketCols() {
    return bucketCols;
  }

  /**
   * @param bucketCols the bucketCols to set
   */
  public void setBucketCols(List<String> bucketCols) {
    this.bucketCols = bucketCols;
  }

  /**
   * @return the parameters
   */
  public Map<String, String> getParameters() {
    return parameters;
  }

  /**
   * @param parameters the parameters to set
   */
  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  /**
   * @return the inputFormat
   */
  public String getInputFormat() {
    return inputFormat;
  }

  /**
   * @param inputFormat the inputFormat to set
   */
  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  /**
   * @return the outputFormat
   */
  public String getOutputFormat() {
    return outputFormat;
  }

  /**
   * @param outputFormat the outputFormat to set
   */
  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  /**
   * @return the column descriptor
   */
  public MColumnDescriptor getCD() {
    return cd;
  }

  /**
   * @param cd the Column Descriptor to set
   */
  public void setCD(MColumnDescriptor cd) {
    this.cd = cd;
  }

  /**
   * @return the serDe
   */
  public MSerDeInfo getSerDeInfo() {
    return serDeInfo;
  }

  /**
   * @param serDe the serDe to set
   */
  public void setSerDeInfo(MSerDeInfo serDe) {
    this.serDeInfo = serDe;
  }


  /**
   * @param sortOrder the sortOrder to set
   */
  public void setSortCols(List<MOrder> sortOrder) {
    this.sortCols = sortOrder;
  }


  /**
   * @return the sortOrder
   */
  public List<MOrder> getSortCols() {
    return sortCols;
  }

  /**
   * @return the skewedColNames
   */
  public List<String> getSkewedColNames() {
    return skewedColNames;
  }

  /**
   * @param skewedColNames the skewedColNames to set
   */
  public void setSkewedColNames(List<String> skewedColNames) {
    this.skewedColNames = skewedColNames;
  }

  /**
   * @return the skewedColValues
   */
  public List<MStringList> getSkewedColValues() {
    return skewedColValues;
  }

  /**
   * @param skewedColValues the skewedColValues to set
   */
  public void setSkewedColValues(List<MStringList> skewedColValues) {
    this.skewedColValues = skewedColValues;
  }

  /**
   * @return the skewedColValueLocationMaps
   */
  public Map<MStringList, String> getSkewedColValueLocationMaps() {
    return skewedColValueLocationMaps;
  }

  /**
   * @param listBucketColValuesMapping the skewedColValueLocationMaps to set
   */
  public void setSkewedColValueLocationMaps(Map<MStringList, String> listBucketColValuesMapping) {
    this.skewedColValueLocationMaps = listBucketColValuesMapping;
  }


  /**
   * @return the storedAsSubDirectories
   */
  public boolean isStoredAsSubDirectories() {
    return isStoredAsSubDirectories;
  }


  /**
   * @param storedAsSubDirectories the storedAsSubDirectories to set
   */
  public void setStoredAsSubDirectories(boolean storedAsSubDirectories) {
    this.isStoredAsSubDirectories = storedAsSubDirectories;
  }

}
