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
@echo off

set RECURSIVE=
set PORT=
set MAINSUSPEND=
set CHILDSUSPEND=
set SWAPSUSPEND=
set HIVE_MAIN_CLIENT_DEBUG_OPTS=
set HIVE_CHILD_CLIENT_DEBUG_OPTS=
if %1==HELP (
	goto :debug_help
) else (
	call :get_debug_params %*
)

@rem must use java 1.5 or later prereq
:ParseDebugArgs
	:ProcessDebugArgsLoop
		if [%1]==[] goto :EndProcessDebug

		set params=%1
		set temp=%params:~0, 8%

		@rem trim off the --debug[ if it is the 1st param
		if %temp%==--debug[ (
			set params=%params:--debug[=%
		)

		@rem trim off the ] if necessary on the value of the param
		set value=%2
		set value=%value:]=%

		if %params%==recursive (
			set RECURSIVE=%value%
			shift
			shift
			goto :ProcessDebugArgsLoop
		)

		if %params%==port (
			set PORT=%value%
			shift
			shift
			goto :ProcessDebugArgsLoop
		)

		if %params%==mainSuspend (
			set MAINSUSPEND=%value%
			shift
			shift
			goto :ProcessDebugArgsLoop
		)
		if %params%==childSuspend (
			set CHILDSUSPEND=%value%
			shift
			shift
			goto :ProcessDebugArgsLoop
		)
		if %params%==swapSuspend (
			set childTemp=%CHILDSUSPEND%
			set CHILDSUSPEND=%MAINSUSPEND%
			set MAINSUSPEND=%childTemp%
			shift
			goto :ProcessDebugArgsLoop
		)

		shift
		goto :ProcessDebugArgsLoop

	:EndProcessDebug
goto :EOF

:set_debug_defaults
  set RECURSIVE="y"
  set PORT=address=8000
  set MAINSUSPEND=suspend=y
  set CHILDSUSPEND=suspend=n
goto :EOF

:get_debug_params
	call :set_debug_defaults
	call :ParseDebugArgs %*
	set HIVE_MAIN_CLIENT_DEBUG_OPTS= -XX:+UseParallelGC -Xdebug -Xrunjdwp:transport=dt_socket,server=y,%PORT%,%MAINSUSPEND%
	set HIVE_CHILD_CLIENT_DEBUG_OPTS= -XX:+UseParallelGC -Xdebug -Xrunjdwp:transport=dt_socket,server=y,%CHILDSUSPEND%
goto :EOF

:debug_help
  echo "Allows to debug Hive by connecting to it via JDI API"
  echo "Usage: hive --debug[:comma-separated parameters list]"
  echo "Parameters:"
  echo "recursive=<y|n>             Should child JVMs also be started in debug mode. Default: y"
  echo "port=<port_number>          Port on which main JVM listens for debug connection. Default: 8000"
  echo "mainSuspend=<y|n>           Should main JVM wait with execution for the debugger to connect. Default: y"
  echo "childSuspend=<y|n>          Should child JVMs wait with execution for the debugger to connect. Default: n"
  echo "swapSuspend                 Swaps suspend options between main and child JVMs"
goto :EOF
