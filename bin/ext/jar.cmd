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

if [%1]==[jar_help] goto :jar_help

:jar

  set RUNJAR=%1
  shift

  set RUNCLASS=%1
  shift

  if "%RUNJAR%"== ""(
    echo "RUNJAR not specified"
    exit 3
  )

  if "%RUNCLASS%" == "" (
    echo "RUNCLASS not specified"
    exit 3
  )
  @rem hadoop 20 or newer - skip the aux_jars option and hiveconf
  %HADOOP% jar %$RUNJAR% %RUNCLASS% %HIVE_OPTS% %*
goto :EOF

:jar_help
  echo "Used for applications that require Hadoop and Hive classpath and environment."
  echo "./hive --service jar <yourjar> <yourclass> HIVE_OPTS <your_args>"
goto :EOF