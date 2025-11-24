@echo off
setlocal

REM ============================================================================
REM  cog_creation.bat  (simple & robust version)
REM  - Processes all .tif and .tiff in INPUT_FOLDER
REM  - Creates COGs in OUTPUT_FOLDER
REM  - Data type change OPTIONAL (leave USER_DTYPE empty to keep original)
REM  - CRS change OPTIONAL (leave TARGET_CRS empty to keep original)
REM ============================================================================

REM --------------------------
REM USER SETTINGS (EDIT THIS)
REM --------------------------

set "INPUT_FOLDER=D:\OpenPas Spatial Data\Soil\masked"
set "OUTPUT_FOLDER=D:\OpenPas Spatial Data\Soil\cog"

REM Allowed: float32, int16, int8  (or leave empty "")
set "USER_DTYPE="

REM Example: EPSG:4326, EPSG:25830  (or leave empty "")
set "TARGET_CRS="

REM Only used if TARGET_CRS is set
set "RESAMPLING=bilinear"

set "OUTPUT_SUFFIX=_cog"

REM --------------------------
REM HEADER
REM --------------------------

echo(
echo ===========================================================
echo   COG BATCH CREATION SCRIPT (GDAL / OSGeo4W)
echo ===========================================================
echo   Input folder  : %INPUT_FOLDER%
echo   Output folder : %OUTPUT_FOLDER%
echo   User DType    : %USER_DTYPE%
echo   Target CRS    : %TARGET_CRS%
echo   Resampling    : %RESAMPLING%
echo   Output suffix : %OUTPUT_SUFFIX%
echo   NOTE: empty DType/CRS = same as input
echo ===========================================================
echo(

REM --------------------------
REM BASIC CHECKS
REM --------------------------

if not exist "%INPUT_FOLDER%" (
    echo [ERROR] Input folder not found:
    echo        %INPUT_FOLDER%
    goto END
)

if not exist "%OUTPUT_FOLDER%" (
    echo [INFO] Creating output folder:
    echo        %OUTPUT_FOLDER%
    mkdir "%OUTPUT_FOLDER%"
    if errorlevel 1 (
        echo [ERROR] Could not create output folder.
        goto END
    )
)

REM --------------------------
REM MAP USER_DTYPE TO GDAL TYPE (OPTIONAL)
REM --------------------------

set "GDAL_DTYPE="

if "%USER_DTYPE%"=="" goto SKIP_DTYPE

if /I "%USER_DTYPE%"=="float32" set "GDAL_DTYPE=Float32"
if /I "%USER_DTYPE%"=="int16"   set "GDAL_DTYPE=Int16"
if /I "%USER_DTYPE%"=="int8"    set "GDAL_DTYPE=Byte"
if /I "%USER_DTYPE%"=="Float32" set "GDAL_DTYPE=Float32"
if /I "%USER_DTYPE%"=="Int16"   set "GDAL_DTYPE=Int16"
if /I "%USER_DTYPE%"=="Byte"    set "GDAL_DTYPE=Byte"

if "%GDAL_DTYPE%"=="" (
    echo [ERROR] Unsupported data type: %USER_DTYPE%
    echo         Use one of: float32, int16, int8
    goto END
)

echo [INFO] Using GDAL data type: %GDAL_DTYPE%
goto AFTER_DTYPE

:SKIP_DTYPE
echo [INFO] No data type change requested (will keep original for all files)

:AFTER_DTYPE

REM --------------------------
REM SELECT TOOL (gdal_translate vs gdalwarp)
REM --------------------------

set "GDAL_TOOL="
set "GDAL_ARGS="

if "%TARGET_CRS%"=="" goto NO_CRS

REM --- We WILL change CRS: use gdalwarp ---
set "GDAL_TOOL=gdalwarp"
set "GDAL_ARGS=-of COG -t_srs %TARGET_CRS% -r %RESAMPLING% -co COMPRESS=DEFLATE -co BIGTIFF=IF_SAFER"
goto AFTER_CRS

:NO_CRS
REM --- We keep CRS: use gdal_translate ---
set "GDAL_TOOL=gdal_translate"
set "GDAL_ARGS=-of COG -co COMPRESS=DEFLATE -co BIGTIFF=IF_SAFER"

:AFTER_CRS

REM Append data type to args if requested
if "%GDAL_DTYPE%"=="" goto AFTER_ADD_DTYPE
set "GDAL_ARGS=%GDAL_ARGS% -ot %GDAL_DTYPE%"

:AFTER_ADD_DTYPE

echo [INFO] Using tool : %GDAL_TOOL%
echo [INFO] Base args  : %GDAL_ARGS%
echo(

REM --------------------------
REM MAIN LOOP
REM --------------------------

pushd "%INPUT_FOLDER%"
if errorlevel 1 (
    echo [ERROR] Could not access input folder.
    goto END
)

REM Process *.tif
for %%F in (*.tif) do call :PROCESS_ONE "%%F"

REM Process *.tiff
for %%F in (*.tiff) do call :PROCESS_ONE "%%F"

popd

echo(
echo [DONE] Batch COG creation finished.
goto END

REM --------------------------
REM SUBROUTINE: PROCESS_ONE
REM --------------------------
:PROCESS_ONE
set "INFILE=%~1"
set "OUTFILE=%OUTPUT_FOLDER%\%~n1%OUTPUT_SUFFIX%.tif"

echo -----------------------------------------------------------
echo [INFO] Input : %INFILE%
echo [INFO] Output: %OUTFILE%

%GDAL_TOOL% %GDAL_ARGS% "%INFILE%" "%OUTFILE%"

if errorlevel 1 (
    echo [ERROR] Failed to create "%OUTFILE%"
) else (
    echo [OK] Created "%OUTFILE%"
)
echo(
goto :EOF

REM --------------------------
REM END
REM --------------------------
:END
endlocal
