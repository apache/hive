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

package org.apache.hadoop.hive.tools;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

/**
 *
 * GenVectorTestCode.
 * This class is mutable and maintains a hashmap of TestSuiteClassName to test cases.
 * The tests cases are added over the course of vectorized expressions class generation,
 * with test classes being outputted at the end. For each column vector (inputs and/or outputs)
 * a matrix of pairwise covering Booleans is used to generate test cases across nulls and
 * repeating dimensions. Based on the input column vector(s) nulls and repeating states
 * the states of the output column vector (if there is one) is validated, along with the null
 * vector. For filter operations the selection vector is validated against the generated
 * data. Each template corresponds to a class representing a test suite.
 */
public class GenVectorTestCode {

  public enum TestSuiteClassName{
    TestColumnScalarOperationVectorExpressionEvaluation,
    TestColumnScalarFilterVectorExpressionEvaluation,
    TestColumnColumnOperationVectorExpressionEvaluation,
    TestColumnColumnFilterVectorExpressionEvaluation,
  }

  private final String testOutputDir;
  private final String testTemplateDirectory;
  private final HashMap<TestSuiteClassName,StringBuilder> testsuites;

  public GenVectorTestCode(String testOutputDir, String testTemplateDirectory) {
    this.testOutputDir = testOutputDir;
    this.testTemplateDirectory = testTemplateDirectory;
    testsuites = new HashMap<TestSuiteClassName, StringBuilder>();

    for(TestSuiteClassName className : TestSuiteClassName.values()) {
      testsuites.put(className,new StringBuilder());
    }

  }

  public void addColumnScalarOperationTestCases(boolean op1IsCol, String vectorExpClassName,
      String inputColumnVectorType, String outputColumnVectorType, String scalarType)
          throws IOException {

    TestSuiteClassName template =
        TestSuiteClassName.TestColumnScalarOperationVectorExpressionEvaluation;

    //Read the template into a string;
    String templateFile = GenVectorCode.joinPath(this.testTemplateDirectory,template.toString()+".txt");
    String templateString = removeTemplateComments(GenVectorCode.readFile(templateFile));

    for(Boolean[] testMatrix :new Boolean[][]{
        // Pairwise: InitOuputColHasNulls, InitOuputColIsRepeating, ColumnHasNulls, ColumnIsRepeating
        {false,   true,    true,    true},
        {false,   false,   false,   false},
        {true,    false,   true,    false},
        {true,    true,    false,   false},
        {true,    false,   false,   true}}) {
      String testCase = templateString;
      testCase = testCase.replaceAll("<TestName>",
          "test"
           + vectorExpClassName
           + createNullRepeatingNameFragment("Out", testMatrix[0], testMatrix[1])
           + createNullRepeatingNameFragment("Col", testMatrix[2], testMatrix[3]));
      testCase = testCase.replaceAll("<VectorExpClassName>", vectorExpClassName);
      testCase = testCase.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
      testCase = testCase.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
      testCase = testCase.replaceAll("<ScalarType>", scalarType);
      testCase = testCase.replaceAll("<CamelCaseScalarType>", GenVectorCode.getCamelCaseType(scalarType));
      testCase = testCase.replaceAll("<InitOuputColHasNulls>", testMatrix[0].toString());
      testCase = testCase.replaceAll("<InitOuputColIsRepeating>", testMatrix[1].toString());
      testCase = testCase.replaceAll("<ColumnHasNulls>", testMatrix[2].toString());
      testCase = testCase.replaceAll("<ColumnIsRepeating>", testMatrix[3].toString());

      if(op1IsCol){
        testCase = testCase.replaceAll("<ConstructorParams>","0, scalarValue");
      }else{
        testCase = testCase.replaceAll("<ConstructorParams>","scalarValue, 0");
      }

      testsuites.get(template).append(testCase);
    }
  }

  public void addColumnScalarFilterTestCases(boolean op1IsCol, String vectorExpClassName,
      String inputColumnVectorType, String scalarType, String operatorSymbol)
          throws IOException {

    TestSuiteClassName template =
        TestSuiteClassName.TestColumnScalarFilterVectorExpressionEvaluation;

    //Read the template into a string;
    String templateFile = GenVectorCode.joinPath(this.testTemplateDirectory,template.toString()+".txt");
    String templateString = removeTemplateComments(GenVectorCode.readFile(templateFile));

    for(Boolean[] testMatrix : new Boolean[][]{
        // Pairwise: ColumnHasNulls, ColumnIsRepeating
        {true,  true},
        {true,  false},
        {false, false},
        {false, true}}) {
      String testCase = templateString;
      testCase = testCase.replaceAll("<TestName>",
          "test"
           + vectorExpClassName
           + createNullRepeatingNameFragment("Col", testMatrix[0], testMatrix[1]));
      testCase = testCase.replaceAll("<VectorExpClassName>", vectorExpClassName);
      testCase = testCase.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
      testCase = testCase.replaceAll("<ScalarType>", scalarType);
      testCase = testCase.replaceAll("<CamelCaseScalarType>", GenVectorCode.getCamelCaseType(scalarType));
      testCase = testCase.replaceAll("<ColumnHasNulls>", testMatrix[0].toString());
      testCase = testCase.replaceAll("<ColumnIsRepeating>", testMatrix[1].toString());
      testCase = testCase.replaceAll("<Operator>", operatorSymbol);

      if(op1IsCol){
        testCase = testCase.replaceAll("<Operand1>","inputColumnVector.vector[i]");
        testCase = testCase.replaceAll("<Operand2>","scalarValue");
        testCase = testCase.replaceAll("<ConstructorParams>","0, scalarValue");
      }else{
        testCase = testCase.replaceAll("<Operand1>","scalarValue");
        testCase = testCase.replaceAll("<Operand2>","inputColumnVector.vector[i]");
        testCase = testCase.replaceAll("<ConstructorParams>","scalarValue, 0");
      }

      testsuites.get(template).append(testCase);
    }
  }

  public void addColumnColumnOperationTestCases(String vectorExpClassName,
      String inputColumnVectorType1, String inputColumnVectorType2, String outputColumnVectorType)
          throws IOException {

    TestSuiteClassName template=
     TestSuiteClassName.TestColumnColumnOperationVectorExpressionEvaluation;

    //Read the template into a string;
    String templateFile = GenVectorCode.joinPath(this.testTemplateDirectory,template.toString()+".txt");
    String templateString = removeTemplateComments(GenVectorCode.readFile(templateFile));

    for(Boolean[] testMatrix : new Boolean[][]{
        // Pairwise: InitOuputColHasNulls, InitOuputColIsRepeating, Column1HasNulls,
        // Column1IsRepeating, Column2HasNulls, Column2IsRepeating
        {true,    true,    false,   true,    true,    true},
        {false,   false,   true,    false,   false,   false},
        {true,    false,   true,    false,   true,    true},
        {true,    true,    true,    true,    false,   false},
        {false,   false,   false,   true,    true,    false},
        {false,   true,    false,   false,   false,   true}}) {
      String testCase = templateString;
      testCase = testCase.replaceAll("<TestName>",
          "test"
          + vectorExpClassName
          + createNullRepeatingNameFragment("Out", testMatrix[0], testMatrix[1])
          + createNullRepeatingNameFragment("C1", testMatrix[2], testMatrix[3])
          + createNullRepeatingNameFragment("C2", testMatrix[4], testMatrix[5]));
      testCase = testCase.replaceAll("<VectorExpClassName>", vectorExpClassName);
      testCase = testCase.replaceAll("<InputColumnVectorType1>", inputColumnVectorType1);
      testCase = testCase.replaceAll("<InputColumnVectorType2>", inputColumnVectorType2);
      testCase = testCase.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
      testCase = testCase.replaceAll("<InitOuputColHasNulls>", testMatrix[0].toString());
      testCase = testCase.replaceAll("<InitOuputColIsRepeating>", testMatrix[1].toString());
      testCase = testCase.replaceAll("<Column1HasNulls>", testMatrix[2].toString());
      testCase = testCase.replaceAll("<Column1IsRepeating>", testMatrix[3].toString());
      testCase = testCase.replaceAll("<Column2HasNulls>", testMatrix[4].toString());
      testCase = testCase.replaceAll("<Column2IsRepeating>", testMatrix[5].toString());

      testsuites.get(template).append(testCase);
    }
  }

  public void addColumnColumnFilterTestCases(String vectorExpClassName,
      String inputColumnVectorType1, String inputColumnVectorType2,  String operatorSymbol)
          throws IOException {

      TestSuiteClassName template=
          TestSuiteClassName.TestColumnColumnFilterVectorExpressionEvaluation;

      //Read the template into a string;
      String templateFile = GenVectorCode.joinPath(this.testTemplateDirectory,template.toString()+".txt");
      String templateString = removeTemplateComments(GenVectorCode.readFile(templateFile));

      for(Boolean[] testMatrix : new Boolean[][]{
          // Pairwise: Column1HasNulls, Column1IsRepeating, Column2HasNulls, Column2IsRepeating
          {false,   true,    true,    true},
          {false,   false,   false,   false},
          {true,    false,   true,    false},
          {true,    true,    false,   false},
          {true,    false,   false,   true}}) {
        String testCase = templateString;
        testCase = testCase.replaceAll("<TestName>",
            "test"
            + vectorExpClassName
            + createNullRepeatingNameFragment("C1", testMatrix[0], testMatrix[1])
            + createNullRepeatingNameFragment("C2", testMatrix[2], testMatrix[3]));
        testCase = testCase.replaceAll("<VectorExpClassName>", vectorExpClassName);
        testCase = testCase.replaceAll("<InputColumnVectorType1>", inputColumnVectorType1);
        testCase = testCase.replaceAll("<InputColumnVectorType2>", inputColumnVectorType2);
        testCase = testCase.replaceAll("<Column1HasNulls>", testMatrix[0].toString());
        testCase = testCase.replaceAll("<Column1IsRepeating>", testMatrix[1].toString());
        testCase = testCase.replaceAll("<Column2HasNulls>", testMatrix[2].toString());
        testCase = testCase.replaceAll("<Column2IsRepeating>", testMatrix[3].toString());
        testCase = testCase.replaceAll("<Operator>", operatorSymbol);

        testsuites.get(template).append(testCase);
      }
    }

  public void generateTestSuites() throws IOException {

    String templateFile = GenVectorCode.joinPath(this.testTemplateDirectory, "TestClass.txt");
    for(TestSuiteClassName testClass : testsuites.keySet()) {

      String templateString = GenVectorCode.readFile(templateFile);
      templateString = templateString.replaceAll("<ClassName>", testClass.toString());
      templateString = templateString.replaceAll("<TestCases>", testsuites.get(testClass).toString());

      String outputFile = GenVectorCode.joinPath(this.testOutputDir, testClass + ".java");

      GenVectorCode.writeFile(new File(outputFile), templateString);
    }
  }

  private static String createNullRepeatingNameFragment(String idenitfier, boolean nulls, boolean repeating)
  {
    if(nulls || repeating){
      if(nulls){
        idenitfier+="Nulls";
      }
      if(repeating){
        idenitfier+="Repeats";
      }
      return idenitfier;
    }

    return "";
  }

  private static String removeTemplateComments(String templateString){
    return templateString.replaceAll("(?s)<!--(.*)-->", "");
  }
}
