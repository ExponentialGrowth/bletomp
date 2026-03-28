@echo off
echo ==============================================
echo    POLICE DATA HUB: DUAL TERMINAL MODE
echo ==============================================
echo.
echo Starting Input Receiver on Port 8081...
start "INPUT_RECEIVER" cmd /k "node input_receiver.js"
echo.
echo Starting Dashboard Server on Port 8080...
start "DASHBOARD_SERVER" cmd /k "node dashboard_server.js"
echo.
echo Both servers are starting in separate windows.
echo ----------------------------------------------
echo Mobile App -> Should point to Port 8081
echo Web Admin  -> http://localhost:8080/dashboard
echo ==============================================
pause
