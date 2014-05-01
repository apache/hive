@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

:help
  echo "Usage ./hive <parameters> --service serviceName <service parameters>"
  echo "Service List: $SERVICE_LIST"
  echo "Parameters parsed:"
  echo "  --auxpath : Auxillary jars "
  echo "  --config : Hive configuration directory"
  echo "  --service : Starts specific service/component. cli is default"
  echo "Parameters used:"
  echo "  HADOOP_HOME or HADOOP_PREFIX : Hadoop install directory"
  echo "  HIVE_OPT : Hive options"
  echo "For help on a particular service:"
  echo "  ./hive --service serviceName --help"
  echo "Debug help:  ./hive --debug --help"
goto :EOF