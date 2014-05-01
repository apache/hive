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


@rem processes --config and --auxpath option from command line

if defined HIVE_HOME goto :DoneSetHiveHome
set HIVE_HOME=%~dp0
for %%i in (%HIVE_HOME%.) do (
  set HIVE_HOME=%%~dpi
)

if "%HIVE_HOME:~-1%" == "\" (
  set HIVE_HOME=%HIVE_HOME:~0,-1%
)

:DoneSetHiveHome
set HIVE_CONF_DIR=
rem set HIVE_AUX_JARS_PATH=
:Loop
	if [%1]==[] GOTO :FinishLoop

	if [%1]==[--config] (
	goto :SetConfig
	)

	if [%1]==[--auxpath] (
	goto :SetAux
	)

	@rem current argument does not match any aux params, finish loop here
	goto :FinishLoop
	SHIFT
	GOTO Loop

	:SetConfig
		set HIVE_CONF_DIR=%2
		shift
		shift
	goto :Loop

	:SetAux
		set HIVE_AUX_JARS_PATH=%2
		shift
		shift
	goto :Loop

:FinishLoop
