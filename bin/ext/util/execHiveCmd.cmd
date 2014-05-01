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

if [%1]==[] (
	echo "No class set to run.  Please specify the class to run."
	exit /b 1
)
set CLASS=%1
@rem hadoop 20 or newer - skip the aux_jars option. picked up from hiveconf
call %HADOOP% jar %JAR% %CLASS% %HIVE_OPTS% %HIVEARGS%
goto :EOF
