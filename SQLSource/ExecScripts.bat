echo off
bteq <createTableBteq.txt>  StageTableCreation_Log.txt
set error_level=%ERRORLEVEL%
echo on
echo %error_level%
IF %error_level%==0 (
goto executeSecond
)

ELSE 
goto ShowError
exit	

:executeSecond
echo "Executing Second Script - Populating "
echo off
tbuild -f load_data_in_stage.tpt >loadDataStage_Log.txt
echo on
set error_level1=%ERRORLEVEL%
echo %ERRORLEVEL% > errorlevel_loadStage.txt
IF %error_level1%==0 (
goto executeThird
)
ELSE
goto ShowError
exit 



:executeThird
echo "Executing third script"
echo off
bteq <createTargetTableBteq.txt>  targetTableCreation_Log.txt
set error_level=%ERRORLEVEL% 
tbuild -f load_data_in_target.tpt > loadDataTarget_Log.txt
echo on
echo %ERRORLEVEL% > errorlevel_loadTarget.txt
goto commonExit

:ShowError
echo "Error in one of script file. Check Log Files"
goto commonExit

:commonExit
echo "Exiting Batch File"
