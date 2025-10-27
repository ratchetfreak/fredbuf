setlocal enabledelayedexpansion

for %%a in (%*) do set "%%a=1"

set timing_flag=
if "%timing%"=="1" set timing_flag=/DTIMING_DATA

cl /nologo /std:c++latest /EHsc /W4 /WX /diagnostics:caret /diagnostics:color /Z7 %timing_flag% fredbuf-test.cpp /Fefredbuf-test.exe

cl /c /std:c++latest /EHsc /W4 /WX /diagnostics:caret /diagnostics:color %timing_flag%fredbuf.cpp

