package org.apache.hadoop.hive.ql.exec.vector.expressions.templates;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CodeGen {

  static String [][] templateExpansions =
    {
      {"ColumnArithmeticScalar", "Add", "long", "long", "+"},
      {"ColumnArithmeticScalar", "Subtract", "long", "long", "-"},
      {"ColumnArithmeticScalar", "Multiply", "long", "long", "*"},
      {"ColumnArithmeticScalar", "Divide", "long", "long", "/"},
      {"ColumnArithmeticScalar", "Modulo", "long", "long", "%"},

      {"ColumnArithmeticScalar", "Add", "long", "double", "+"},
      {"ColumnArithmeticScalar", "Subtract", "long", "double", "-"},
      {"ColumnArithmeticScalar", "Multiply", "long", "double", "*"},
      {"ColumnArithmeticScalar", "Divide", "long", "double", "/"},
      {"ColumnArithmeticScalar", "Modulo", "long", "double", "%"},

      {"ColumnArithmeticScalar", "Add", "double", "long", "+"},
      {"ColumnArithmeticScalar", "Subtract", "double", "long", "-"},
      {"ColumnArithmeticScalar", "Multiply", "double", "long", "*"},
      {"ColumnArithmeticScalar", "Divide", "double", "long", "/"},
      {"ColumnArithmeticScalar", "Modulo", "double", "long", "%"},

      {"ColumnArithmeticScalar", "Add", "double", "double", "+"},
      {"ColumnArithmeticScalar", "Subtract", "double", "double", "-"},
      {"ColumnArithmeticScalar", "Multiply", "double", "double", "*"},
      {"ColumnArithmeticScalar", "Divide", "double", "double", "/"},
      {"ColumnArithmeticScalar", "Modulo", "double", "double", "%"},

      {"ScalarArithmeticColumn", "Add", "long", "long", "+"},
      {"ScalarArithmeticColumn", "Subtract", "long", "long", "-"},
      {"ScalarArithmeticColumn", "Multiply", "long", "long", "*"},
      {"ScalarArithmeticColumn", "Divide", "long", "long", "/"},
      {"ScalarArithmeticColumn", "Modulo", "long", "long", "%"},

      {"ScalarArithmeticColumn", "Add", "long", "double", "+"},
      {"ScalarArithmeticColumn", "Subtract", "long", "double", "-"},
      {"ScalarArithmeticColumn", "Multiply", "long", "double", "*"},
      {"ScalarArithmeticColumn", "Divide", "long", "double", "/"},
      {"ScalarArithmeticColumn", "Modulo", "long", "double", "%"},

      {"ScalarArithmeticColumn", "Add", "double", "long", "+"},
      {"ScalarArithmeticColumn", "Subtract", "double", "long", "-"},
      {"ScalarArithmeticColumn", "Multiply", "double", "long", "*"},
      {"ScalarArithmeticColumn", "Divide", "double", "long", "/"},
      {"ScalarArithmeticColumn", "Modulo", "double", "long", "%"},

      {"ScalarArithmeticColumn", "Add", "double", "double", "+"},
      {"ScalarArithmeticColumn", "Subtract", "double", "double", "-"},
      {"ScalarArithmeticColumn", "Multiply", "double", "double", "*"},
      {"ScalarArithmeticColumn", "Divide", "double", "double", "/"},
      {"ScalarArithmeticColumn", "Modulo", "double", "double", "%"},

      {"ColumnArithmeticColumn", "Add", "long", "long", "+"},
      {"ColumnArithmeticColumn", "Subtract", "long", "long", "-"},
      {"ColumnArithmeticColumn", "Multiply", "long", "long", "*"},
      {"ColumnArithmeticColumn", "Divide", "long", "long", "/"},
      {"ColumnArithmeticColumn", "Modulo", "long", "long", "%"},

      {"ColumnArithmeticColumn", "Add", "long", "double", "+"},
      {"ColumnArithmeticColumn", "Subtract", "long", "double", "-"},
      {"ColumnArithmeticColumn", "Multiply", "long", "double", "*"},
      {"ColumnArithmeticColumn", "Divide", "long", "double", "/"},
      {"ColumnArithmeticColumn", "Modulo", "long", "double", "%"},

      {"ColumnArithmeticColumn", "Add", "double", "long", "+"},
      {"ColumnArithmeticColumn", "Subtract", "double", "long", "-"},
      {"ColumnArithmeticColumn", "Multiply", "double", "long", "*"},
      {"ColumnArithmeticColumn", "Divide", "double", "long", "/"},
      {"ColumnArithmeticColumn", "Modulo", "double", "long", "%"},

      {"ColumnArithmeticColumn", "Add", "double", "double", "+"},
      {"ColumnArithmeticColumn", "Subtract", "double", "double", "-"},
      {"ColumnArithmeticColumn", "Multiply", "double", "double", "*"},
      {"ColumnArithmeticColumn", "Divide", "double", "double", "/"},
      {"ColumnArithmeticColumn", "Modulo", "double", "double", "%"},

      {"ColumnCompareScalar", "Equal", "long", "double", "=="},
      {"ColumnCompareScalar", "Equal", "double", "double", "=="},
      {"ColumnCompareScalar", "NotEqual", "long", "double", "!="},
      {"ColumnCompareScalar", "NotEqual", "double", "double", "!="},
      {"ColumnCompareScalar", "Less", "long", "double", "<"},
      {"ColumnCompareScalar", "Less", "double", "double", "<"},
      {"ColumnCompareScalar", "LessEqual", "long", "double", "<="},
      {"ColumnCompareScalar", "LessEqual", "double", "double", "<="},
      {"ColumnCompareScalar", "Greater", "long", "double", ">"},
      {"ColumnCompareScalar", "Greater", "double", "double", ">"},
      {"ColumnCompareScalar", "GreaterEqual", "long", "double", ">="},
      {"ColumnCompareScalar", "GreaterEqual", "double", "double", ">="},

      {"FilterColumnCompareScalar", "Equal", "long", "double", "=="},
      {"FilterColumnCompareScalar", "Equal", "double", "double", "=="},
      {"FilterColumnCompareScalar", "NotEqual", "long", "double", "!="},
      {"FilterColumnCompareScalar", "NotEqual", "double", "double", "!="},
      {"FilterColumnCompareScalar", "Less", "long", "double", "<"},
      {"FilterColumnCompareScalar", "Less", "double", "double", "<"},
      {"FilterColumnCompareScalar", "LessEqual", "long", "double", "<="},
      {"FilterColumnCompareScalar", "LessEqual", "double", "double", "<="},
      {"FilterColumnCompareScalar", "Greater", "long", "double", ">"},
      {"FilterColumnCompareScalar", "Greater", "double", "double", ">"},
      {"FilterColumnCompareScalar", "GreaterEqual", "long", "double", ">="},
      {"FilterColumnCompareScalar", "GreaterEqual", "double", "double", ">="},

      {"FilterColumnCompareScalar", "Equal", "long", "long", "=="},
      {"FilterColumnCompareScalar", "Equal", "double", "long", "=="},
      {"FilterColumnCompareScalar", "NotEqual", "long", "long", "!="},
      {"FilterColumnCompareScalar", "NotEqual", "double", "long", "!="},
      {"FilterColumnCompareScalar", "Less", "long", "long", "<"},
      {"FilterColumnCompareScalar", "Less", "double", "long", "<"},
      {"FilterColumnCompareScalar", "LessEqual", "long", "long", "<="},
      {"FilterColumnCompareScalar", "LessEqual", "double", "long", "<="},
      {"FilterColumnCompareScalar", "Greater", "long", "long", ">"},
      {"FilterColumnCompareScalar", "Greater", "double", "long", ">"},
      {"FilterColumnCompareScalar", "GreaterEqual", "long", "long", ">="},
      {"FilterColumnCompareScalar", "GreaterEqual", "double", "long", ">="},

      {"FilterScalarCompareColumn", "Equal", "long", "double", "=="},
      {"FilterScalarCompareColumn", "Equal", "double", "double", "=="},
      {"FilterScalarCompareColumn", "NotEqual", "long", "double", "!="},
      {"FilterScalarCompareColumn", "NotEqual", "double", "double", "!="},
      {"FilterScalarCompareColumn", "Less", "long", "double", "<"},
      {"FilterScalarCompareColumn", "Less", "double", "double", "<"},
      {"FilterScalarCompareColumn", "LessEqual", "long", "double", "<="},
      {"FilterScalarCompareColumn", "LessEqual", "double", "double", "<="},
      {"FilterScalarCompareColumn", "Greater", "long", "double", ">"},
      {"FilterScalarCompareColumn", "Greater", "double", "double", ">"},
      {"FilterScalarCompareColumn", "GreaterEqual", "long", "double", ">="},
      {"FilterScalarCompareColumn", "GreaterEqual", "double", "double", ">="},

      {"FilterScalarCompareColumn", "Equal", "long", "long", "=="},
      {"FilterScalarCompareColumn", "Equal", "double", "long", "=="},
      {"FilterScalarCompareColumn", "NotEqual", "long", "long", "!="},
      {"FilterScalarCompareColumn", "NotEqual", "double", "long", "!="},
      {"FilterScalarCompareColumn", "Less", "long", "long", "<"},
      {"FilterScalarCompareColumn", "Less", "double", "long", "<"},
      {"FilterScalarCompareColumn", "LessEqual", "long", "long", "<="},
      {"FilterScalarCompareColumn", "LessEqual", "double", "long", "<="},
      {"FilterScalarCompareColumn", "Greater", "long", "long", ">"},
      {"FilterScalarCompareColumn", "Greater", "double", "long", ">"},
      {"FilterScalarCompareColumn", "GreaterEqual", "long", "long", ">="},
      {"FilterScalarCompareColumn", "GreaterEqual", "double", "long", ">="},

      {"FilterStringColumnCompareScalar", "Equal", "=="},
      {"FilterStringColumnCompareScalar", "NotEqual", "!="},
      {"FilterStringColumnCompareScalar", "Less", "<"},
      {"FilterStringColumnCompareScalar", "LessEqual", "<="},
      {"FilterStringColumnCompareScalar", "Greater", ">"},
      {"FilterStringColumnCompareScalar", "GreaterEqual", ">="},

      {"FilterColumnCompareColumn", "Equal", "long", "double", "=="},
      {"FilterColumnCompareColumn", "Equal", "double", "double", "=="},
      {"FilterColumnCompareColumn", "NotEqual", "long", "double", "!="},
      {"FilterColumnCompareColumn", "NotEqual", "double", "double", "!="},
      {"FilterColumnCompareColumn", "Less", "long", "double", "<"},
      {"FilterColumnCompareColumn", "Less", "double", "double", "<"},
      {"FilterColumnCompareColumn", "LessEqual", "long", "double", "<="},
      {"FilterColumnCompareColumn", "LessEqual", "double", "double", "<="},
      {"FilterColumnCompareColumn", "Greater", "long", "double", ">"},
      {"FilterColumnCompareColumn", "Greater", "double", "double", ">"},
      {"FilterColumnCompareColumn", "GreaterEqual", "long", "double", ">="},
      {"FilterColumnCompareColumn", "GreaterEqual", "double", "double", ">="},

        {"FilterColumnCompareColumn", "Equal", "long", "long", "=="},
        {"FilterColumnCompareColumn", "Equal", "double", "long", "=="},
        {"FilterColumnCompareColumn", "NotEqual", "long", "long", "!="},
        {"FilterColumnCompareColumn", "NotEqual", "double", "long", "!="},
        {"FilterColumnCompareColumn", "Less", "long", "long", "<"},
        {"FilterColumnCompareColumn", "Less", "double", "long", "<"},
        {"FilterColumnCompareColumn", "LessEqual", "long", "long", "<="},
        {"FilterColumnCompareColumn", "LessEqual", "double", "long", "<="},
        {"FilterColumnCompareColumn", "Greater", "long", "long", ">"},
        {"FilterColumnCompareColumn", "Greater", "double", "long", ">"},
        {"FilterColumnCompareColumn", "GreaterEqual", "long", "long", ">="},
        {"FilterColumnCompareColumn", "GreaterEqual", "double", "long", ">="},

        {"ColumnUnaryMinus", "long"},
        {"ColumnUnaryMinus", "double"},

        //template, <ClassName>, <ValueType>, <OperatorSymbol>, <DescriptionName>, <DescriptionValue>
        {"VectorUDAFMinMax", "VectorUDAFMinLong", "long", "<", "min", "_FUNC_(expr) - Returns the minimum value of expr (vectorized, type: long)"},
        {"VectorUDAFMinMax", "VectorUDAFMinDouble", "double", "<", "min", "_FUNC_(expr) - Returns the minimum value of expr (vectorized, type: double)"},
        {"VectorUDAFMinMax", "VectorUDAFMaxLong", "long", ">", "max", "_FUNC_(expr) - Returns the maximum value of expr (vectorized, type: long)"},
        {"VectorUDAFMinMax", "VectorUDAFMaxDouble", "double", ">", "max", "_FUNC_(expr) - Returns the maximum value of expr (vectorized, type: double)"},

        //template, <ClassName>, <ValueType>
        {"VectorUDAFCount", "VectorUDAFCountLong", "long"},
        {"VectorUDAFCount", "VectorUDAFCountDouble", "double"},

        //template, <ClassName>, <ValueType>
        {"VectorUDAFSum", "VectorUDAFSumLong", "long"},
        {"VectorUDAFSum", "VectorUDAFSumDouble", "double"},
        {"VectorUDAFAvg", "VectorUDAFAvgLong", "long"},
        {"VectorUDAFAvg", "VectorUDAFAvgDouble", "double"},

      //template, <ClassName>, <ValueType>, <VarianceFormula>, <DescriptionName>, <DescriptionValue>
        {"VectorUDAFVar", "VectorUDAFVarPopLong", "long", "myagg.variance / myagg.count", "variance, var_pop", "_FUNC_(x) - Returns the variance of a set of numbers (vectorized, long)"},
        {"VectorUDAFVar", "VectorUDAFVarPopDouble", "double", "myagg.variance / myagg.count", "variance, var_pop", "_FUNC_(x) - Returns the variance of a set of numbers (vectorized, double)"},
        {"VectorUDAFVar", "VectorUDAFVarSampLong", "long", "myagg.variance / (myagg.count-1.0)", "var_samp", "_FUNC_(x) - Returns the sample variance of a set of numbers (vectorized, long)"},
        {"VectorUDAFVar", "VectorUDAFVarSampDouble", "double", "myagg.variance / (myagg.count-1.0)", "var_samp", "_FUNC_(x) - Returns the sample variance of a set of numbers (vectorized, double)"},
        {"VectorUDAFVar", "VectorUDAFStdPopLong", "long", "Math.sqrt(myagg.variance / (myagg.count))", "std,stddev,stddev_pop", "_FUNC_(x) - Returns the standard deviation of a set of numbers (vectorized, long)"},
        {"VectorUDAFVar", "VectorUDAFStdPopDouble", "double", "Math.sqrt(myagg.variance / (myagg.count))", "std,stddev,stddev_pop", "_FUNC_(x) - Returns the standard deviation of a set of numbers (vectorized, double)"},
        {"VectorUDAFVar", "VectorUDAFStdSampLong", "long", "Math.sqrt(myagg.variance / (myagg.count-1.0))", "stddev_samp", "_FUNC_(x) - Returns the sample standard deviation of a set of numbers (vectorized, long)"},
        {"VectorUDAFVar", "VectorUDAFStdSampDouble", "double", "Math.sqrt(myagg.variance / (myagg.count-1.0))", "stddev_samp", "_FUNC_(x) - Returns the sample standard deviation of a set of numbers (vectorized, double)"},

    };

  private final String templateDirectory;
  private final String outputDirectory;

    private static String joinPath(String...parts) {
    String path = parts[0];
    for (int i=1; i < parts.length; ++i) {
      path += File.separatorChar + parts[i];
    }
    return path;
  }

  public CodeGen() {
    templateDirectory = System.getProperty("user.dir");
    File f = new File(templateDirectory);
    outputDirectory = joinPath(f.getParent(), "gen");
  }

  public CodeGen(String templateDirectory, String outputDirectory) {
    this.templateDirectory = templateDirectory;
    this.outputDirectory = outputDirectory;
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    CodeGen gen;
    if (args != null && args[0] != null) {
      gen = new CodeGen(args[0], args[1]);
    } else {
      gen = new CodeGen();
    }
    gen.generate();
  }

  private void generate() throws Exception {

    for (String [] tdesc : templateExpansions) {
      if (tdesc[0].equals("ColumnArithmeticScalar")) {
        generateColumnArithmeticScalar(tdesc);
      } else if (tdesc[0].equals("ColumnCompareScalar")) {
        generateColumnCompareScalar(tdesc);
      } else if (tdesc[0].equals("FilterColumnCompareScalar")) {
        generateFilterColumnCompareScalar(tdesc);
      } else if (tdesc[0].equals("ScalarArithmeticColumn")) {
        generateScalarArithmeticColumn(tdesc);
      } else if (tdesc[0].equals("FilterColumnCompareColumn")) {
        generateFilterColumnCompareColumn(tdesc);
      } else if (tdesc[0].equals("ColumnArithmeticColumn")) {
        generateColumnArithmeticColumn(tdesc);
      } else if (tdesc[0].equals("ColumnUnaryMinus")) {
        generateColumnUnaryMinus(tdesc);
      } else if (tdesc[0].equals("FilterStringColumnCompareScalar")) {
        generateFilterStringColumnCompareScalar(tdesc);
      } else {
        continue;
      }
    }
  }

  private void generateVectorUDAFMinMax(String[] tdesc) throws Exception {
    String className = tdesc[1];
    String valueType = tdesc[2];
    String operatorSymbol = tdesc[3];
    String descName = tdesc[4];
    String descValue = tdesc[5];
    String columnType = getColumnVectorType(valueType);
    String writableType = getOutputWritableType(valueType);
    String inspectorType = getOutputObjectInspector(valueType);

    String outputFile = joinPath(this.outputDirectory, className + ".java");
    String templateFile = joinPath(this.templateDirectory, tdesc[0] + ".txt");

    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<ValueType>", valueType);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<InputColumnVectorType>", columnType);
    templateString = templateString.replaceAll("<DescriptionName>", descName);
    templateString = templateString.replaceAll("<DescriptionValue>", descValue);
    templateString = templateString.replaceAll("<OutputType>", writableType);
    templateString = templateString.replaceAll("<OutputTypeInspector>", inspectorType);
    writeFile(outputFile, templateString);

  }

  private void generateVectorUDAFCount(String[] tdesc) throws IOException {
    String className = tdesc[1];
    String valueType = tdesc[2];
    String columnType = getColumnVectorType(valueType);

    String outputFile = joinPath(this.outputDirectory, className + ".java");
    String templateFile = joinPath(this.templateDirectory, tdesc[0] + ".txt");

    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<ValueType>", valueType);
    templateString = templateString.replaceAll("<InputColumnVectorType>", columnType);
    writeFile(outputFile, templateString);
  }

  private void generateVectorUDAFSum(String[] tdesc) throws Exception {
  //template, <ClassName>, <ValueType>, <OutputType>, <OutputTypeInspector>
    String className = tdesc[1];
    String valueType = tdesc[2];
    String columnType = getColumnVectorType(valueType);
    String writableType = getOutputWritableType(valueType);
    String inspectorType = getOutputObjectInspector(valueType);

    String outputFile = joinPath(this.outputDirectory, className + ".java");
    String templateFile = joinPath(this.templateDirectory, tdesc[0] + ".txt");

    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<ValueType>", valueType);
    templateString = templateString.replaceAll("<InputColumnVectorType>", columnType);
    templateString = templateString.replaceAll("<OutputType>", writableType);
    templateString = templateString.replaceAll("<OutputTypeInspector>", inspectorType);
    writeFile(outputFile, templateString);
  }

  private void generateVectorUDAFAvg(String[] tdesc) throws IOException {
    String className = tdesc[1];
    String valueType = tdesc[2];
    String columnType = getColumnVectorType(valueType);

    String outputFile = joinPath(this.outputDirectory, className + ".java");
    String templateFile = joinPath(this.templateDirectory, tdesc[0] + ".txt");

    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<ValueType>", valueType);
    templateString = templateString.replaceAll("<InputColumnVectorType>", columnType);
    writeFile(outputFile, templateString);
  }

  private void generateVectorUDAFVar(String[] tdesc) throws IOException {
    String className = tdesc[1];
    String valueType = tdesc[2];
    String varianceFormula = tdesc[3];
    String descriptionName = tdesc[4];
    String descriptionValue = tdesc[5];
    String columnType = getColumnVectorType(valueType);

    String outputFile = joinPath(this.outputDirectory, className + ".java");
    String templateFile = joinPath(this.templateDirectory, tdesc[0] + ".txt");

    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<ValueType>", valueType);
    templateString = templateString.replaceAll("<InputColumnVectorType>", columnType);
    templateString = templateString.replaceAll("<VarianceFormula>", varianceFormula);
    templateString = templateString.replaceAll("<DescriptionName>", descriptionName);
    templateString = templateString.replaceAll("<DescriptionValue>", descriptionValue);
    writeFile(outputFile, templateString);
  }


  private void generateFilterStringColumnCompareScalar(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "FilterStringCol" + operatorName + "StringScalar";
    generateFilterStringColumnCompareScalar(tdesc,className);
  }

  private void generateFilterStringColumnCompareScalar(String[] tdesc, String className) throws IOException {
   String operatorSymbol = tdesc[2];
   String outputFile = joinPath(this.outputDirectory, className + ".java");
   // Read the template into a string;
   String templateFile = joinPath(this.templateDirectory, tdesc[0] + ".txt");
   String templateString = readFile(templateFile);
   // Expand, and write result
   templateString = templateString.replaceAll("<ClassName>", className);
   templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
   writeFile(outputFile, templateString);
  }

  private void generateFilterColumnCompareColumn(String[] tdesc) throws IOException {
    //The variables are all same as ColumnCompareScalar except that
    //this template doesn't need a return type. Pass anything as return type.
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = "Filter" + this.getCamelCaseType(operandType1)
        + "Col" + operatorName + this.getCamelCaseType(operandType2) + "Column";
    generateColumnBinaryOperatorColumn(tdesc, "doesn't matter", className);
  }

  private void generateColumnUnaryMinus(String[] tdesc) throws IOException {
    String operandType = tdesc[1];
    String inputColumnVectorType = this.getColumnVectorType(operandType);
    String outputColumnVectorType = inputColumnVectorType;
    String returnType = operandType;
    String className = this.getCamelCaseType(operandType) + "ColUnaryMinus";
    String outputFile = joinPath(this.outputDirectory, className + ".java");
    String templateFile = joinPath(this.templateDirectory, tdesc[0] + ".txt");
    String templateString = readFile(templateFile);
    // Expand, and write result
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    writeFile(outputFile, templateString);
  }

  private void generateColumnArithmeticColumn(String [] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = this.getCamelCaseType(operandType1)
        + "Col" + operatorName + this.getCamelCaseType(operandType2) + "Column";
    String returnType = getArithmeticReturnType(operandType1, operandType2);
    generateColumnBinaryOperatorColumn(tdesc, returnType, className);
  }

  private void generateFilterColumnCompareScalar(String[] tdesc) throws IOException {
    //The variables are all same as ColumnCompareScalar except that
    //this template doesn't need a return type. Pass anything as return type.
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = "Filter" + this.getCamelCaseType(operandType1)
        + "Col" + operatorName + this.getCamelCaseType(operandType2) + "Scalar";
    generateColumnBinaryOperatorScalar(tdesc, "doesn't matter", className);
  }

  private void generateFilterScalarCompareColumn(String[] tdesc) throws IOException {
    //this template doesn't need a return type. Pass anything as return type.
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = "Filter" + this.getCamelCaseType(operandType1)
        + "Scalar" + operatorName + this.getCamelCaseType(operandType2) + "Column";
    generateScalarBinaryOperatorColumn(tdesc, "doesn't matter", className);
  }

  private void generateColumnCompareScalar(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String returnType = "long";
    String className = this.getCamelCaseType(operandType1)
        + "Col" + operatorName + this.getCamelCaseType(operandType2) + "Scalar";
    generateColumnBinaryOperatorScalar(tdesc, returnType, className);
  }

  private void generateColumnBinaryOperatorColumn(String[] tdesc, String returnType,
         String className) throws IOException {
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String outputColumnVectorType = this.getColumnVectorType(returnType);
    String inputColumnVectorType1 = this.getColumnVectorType(operandType1);
    String inputColumnVectorType2 = this.getColumnVectorType(operandType2);
    String operatorSymbol = tdesc[4];
    String outputFile = joinPath(this.outputDirectory, className + ".java");

    //Read the template into a string;
    String templateFile = joinPath(this.templateDirectory, tdesc[0] + ".txt");
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType1>", inputColumnVectorType1);
    templateString = templateString.replaceAll("<InputColumnVectorType2>", inputColumnVectorType2);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<OperandType1>", operandType1);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    writeFile(outputFile, templateString);
  }

  private void generateColumnBinaryOperatorScalar(String[] tdesc, String returnType,
     String className) throws IOException {
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String outputColumnVectorType = this.getColumnVectorType(returnType);
    String inputColumnVectorType = this.getColumnVectorType(operandType1);
    String operatorSymbol = tdesc[4];
    String outputFile = joinPath(this.outputDirectory, className + ".java");

    //Read the template into a string;
    String templateFile = joinPath(this.templateDirectory, tdesc[0] + ".txt");
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<OperandType1>", operandType1);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    writeFile(outputFile, templateString);
  }

  private void generateScalarBinaryOperatorColumn(String[] tdesc, String returnType,
     String className) throws IOException {
     String operandType1 = tdesc[2];
     String operandType2 = tdesc[3];
     String outputColumnVectorType = this.getColumnVectorType(returnType);
     String inputColumnVectorType = this.getColumnVectorType(operandType2);
     String operatorSymbol = tdesc[4];
     String outputFile = joinPath(this.outputDirectory, className + ".java");

     //Read the template into a string;
     String templateFile = joinPath(this.templateDirectory, tdesc[0] + ".txt");
     String templateString = readFile(templateFile);
     templateString = templateString.replaceAll("<ClassName>", className);
     templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
     templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
     templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
     templateString = templateString.replaceAll("<OperandType1>", operandType1);
     templateString = templateString.replaceAll("<OperandType2>", operandType2);
     templateString = templateString.replaceAll("<ReturnType>", returnType);
     writeFile(outputFile, templateString);
   }

  //Binary arithmetic operator
  private void generateColumnArithmeticScalar(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = this.getCamelCaseType(operandType1)
        + "Col" + operatorName + this.getCamelCaseType(operandType2) + "Scalar";
    String returnType = getArithmeticReturnType(operandType1, operandType2);
    generateColumnBinaryOperatorScalar(tdesc, returnType, className);
  }

  private void generateScalarArithmeticColumn(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = this.getCamelCaseType(operandType1)
        + "Scalar" + operatorName + this.getCamelCaseType(operandType2) + "Column";
    String returnType = getArithmeticReturnType(operandType1, operandType2);
    generateScalarBinaryOperatorColumn(tdesc, returnType, className);
  }

  private void writeFile(String outputFile, String str) throws IOException {
    BufferedWriter w = new BufferedWriter(new FileWriter(outputFile));
    w.write(str);
    w.close();
  }

  private String readFile(String templateFile) throws IOException {
    BufferedReader r = new BufferedReader(new FileReader(templateFile));
    String line = r.readLine();
    StringBuilder b = new StringBuilder();
    while (line != null) {
      b.append(line);
      b.append("\n");
      line = r.readLine();
    }
    r.close();
    return b.toString();
  }

  private String getCamelCaseType(String type) {
    if (type.equals("long")) {
      return "Long";
    } else if (type.equals("double")) {
      return "Double";
    } else {
      return type;
    }
  }

  private String getArithmeticReturnType(String operandType1,
      String operandType2) {
    if (operandType1.equals("double") ||
        operandType2.equals("double")) {
      return "double";
    } else {
      return "long";
    }
  }

  private String getColumnVectorType(String primitiveType) {
    if (primitiveType.equals("long")) {
      return "LongColumnVector";
    } else if (primitiveType.equals("double")) {
      return "DoubleColumnVector";
    }
    return "LongColumnVector";
  }

  private String getOutputWritableType(String primitiveType) throws Exception {
    if (primitiveType.equals("long")) {
      return "LongWritable";
    } else if (primitiveType.equals("double")) {
      return "DoubleWritable";
    }
    throw new Exception("Unimplemented primitive output writable: " + primitiveType);
  }

  private String getOutputObjectInspector(String primitiveType) throws Exception {
    if (primitiveType.equals("long")) {
      return "PrimitiveObjectInspectorFactory.writableLongObjectInspector";
    } else if (primitiveType.equals("double")) {
      return "PrimitiveObjectInspectorFactory.writableDoubleObjectInspector";
    }
    throw new Exception("Unimplemented primitive output inspector: " + primitiveType);
  }
}

