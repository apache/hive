package org.apache.hadoop.hive.registry.util;

import org.junit.runner.Runner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.ParametersRunnerFactory;
import org.junit.runners.parameterized.TestWithParameters;

public class CustomParameterizedBlockJUnit4RunnerFactory implements ParametersRunnerFactory {

  public Runner createRunnerForTestWithParameters(TestWithParameters test)
          throws InitializationError {
    return new CustomParameterizedBlockJUnit4Runner(test);
  }
}
