@setlocal enabledelayedexpansion
@echo off

for %%a in (%*) do set "%%a=1"

set timing_flag=
@rem make it actually optimize
if "%timing%"=="1" set timing_flag=/DTIMING_DATA /DNDEBUG /O2

set spall_flag=
@rem requires building https://gist.github.com/mmozeiko/6dd86354b06c40980982a8a4c04a4d39
@rem and putting the dll and lib in this directory
if "%spall%"=="1" set spall_flag=/GH /Gh /link msvc_spall.lib

@echo on

cl /nologo /std:c++latest /D_CONTAINER_DEBUG_LEVEL=1 /EHsc /W4 /WX /diagnostics:caret /diagnostics:color /Z7 %timing_flag% fredbuf-test.cpp /Fefredbuf-test.exe %spall_flag% /INCREMENTAL:NO

cl /nologo /std:c++latest /D_CONTAINER_DEBUG_LEVEL=1 /EHsc /W4 /WX /diagnostics:caret /diagnostics:color /Zi ratbuf_btree.cpp /c
