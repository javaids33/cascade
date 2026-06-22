@echo off
REM ============================================================================
REM  One-click Cascade MASTER on a Windows PC. Single entry point.
REM
REM  Cascade's sync hub (tursodb) is Linux-only, so the master runs inside WSL2.
REM  WSL2 sits behind NAT, so this script (run elevated) also:
REM    - forwards Windows :8080 -> WSL :8080 + opens the firewall   (so the Mac can reach the hub)
REM    - forwards <host-gw>:11434 -> 127.0.0.1:11434 + opens fw   (so the WSL master can embed on
REM                                                                  the Windows host's Ollama/GPU,
REM                                                                  no Ollama restart needed)
REM  Re-run after a reboot or WSL restart (WSL's internal IP changes).
REM ============================================================================
setlocal EnableDelayedExpansion

REM ---- self-elevate (no PowerShell on this box -> use cscript + a temp VBS) ----
net session >nul 2>&1
if %errorlevel% neq 0 (
  if not "%~1"=="elevated" (
    echo Requesting administrator privileges ^(needed for the WSL^<-^>LAN bridge^)...
    > "%temp%\cascade_elev.vbs" echo Set U = CreateObject^("Shell.Application"^)
    >>"%temp%\cascade_elev.vbs" echo U.ShellExecute "%~f0", "elevated", "", "runas", 1
    cscript //nologo "%temp%\cascade_elev.vbs"
    del "%temp%\cascade_elev.vbs" 2>nul
    exit /b
  )
)

set "DISTRO=Ubuntu"
set "PORT=8080"

REM ---- locate the repo inside WSL. Use forward slashes and strip the trailing slash so the
REM      closing quote isn't escaped by a trailing backslash ("...\" -> broken arg). ----
set "HEREWIN=%~dp0"
set "HEREWIN=!HEREWIN:\=/!"
set "HEREWIN=!HEREWIN:~0,-1!"
for /f "delims=" %%i in ('wsl -d %DISTRO% wslpath -a "!HEREWIN!"') do set "REPO=%%i"
echo Repo (WSL): !REPO!

REM ---- Blocker 1: a fresh WSL has no C toolchain (DuckDB/Iceberg/openssl need it). Install as
REM      root (wsl -u root needs no sudo password). Skipped once it's present. ----
echo.
echo [1/4] WSL build prerequisites
wsl -d %DISTRO% -- bash -lc "command -v cc >/dev/null 2>&1 && dpkg -s libclang-dev >/dev/null 2>&1"
if errorlevel 1 (
  echo   Installing build-essential, pkg-config, libssl-dev, libclang-dev, cmake ^(one-time^)...
  wsl -d %DISTRO% -u root -- bash -lc "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y build-essential pkg-config libssl-dev libclang-dev cmake"
) else (
  echo   present.
)

REM ---- Blocker 2: let the WSL master reach the Windows-host Ollama WITHOUT restarting it.
REM      Ollama listens on 127.0.0.1 only; forward the WSL-facing host IP :11434 -> 127.0.0.1:11434
REM      so WSL's CASCADE_EMBED_URL (http://<host-gateway>:11434) reaches it. No OLLAMA_HOST, no
REM      Ollama restart, no env-propagation gotcha. ----
echo.
echo [2/4] Ollama bridge (WSL -^> Windows-host Ollama, no restart needed)
set "HOSTGW="
for /f "tokens=3" %%i in ('wsl -d %DISTRO% -- ip route show default') do if not defined HOSTGW set "HOSTGW=%%i"
echo   WSL reaches the host at: !HOSTGW!
netsh interface portproxy delete v4tov4 listenport=11434 listenaddress=!HOSTGW! >nul 2>&1
netsh interface portproxy add v4tov4 listenport=11434 listenaddress=!HOSTGW! connectport=11434 connectaddress=127.0.0.1
netsh advfirewall firewall delete rule name="Cascade Ollama 11434" >nul 2>&1
netsh advfirewall firewall add rule name="Cascade Ollama 11434" dir=in action=allow protocol=TCP localport=11434 >nul
echo   forwarding !HOSTGW!:11434 -^> 127.0.0.1:11434, firewall 11434 open.

REM ---- Blocker 3: WSL2 -> LAN bridge for the sync hub ----
echo.
echo [3/4] Sync hub bridge (WSL :%PORT% -^> LAN :%PORT%)
for /f "usebackq tokens=1" %%i in (`wsl -d %DISTRO% hostname -I`) do set "WSLIP=%%i"
echo   WSL2 IP: !WSLIP!
netsh interface portproxy delete v4tov4 listenport=%PORT% listenaddress=0.0.0.0 >nul 2>&1
netsh interface portproxy add v4tov4 listenport=%PORT% listenaddress=0.0.0.0 connectport=%PORT% connectaddress=!WSLIP!
netsh advfirewall firewall delete rule name="Cascade Sync %PORT%" >nul 2>&1
netsh advfirewall firewall add rule name="Cascade Sync %PORT%" dir=in action=allow protocol=TCP localport=%PORT%

REM telemetry agent port (so the dashboard can pull this node's config/logs/metrics from the LAN)
set "APORT=7071"
netsh interface portproxy delete v4tov4 listenport=!APORT! listenaddress=0.0.0.0 >nul 2>&1
netsh interface portproxy add v4tov4 listenport=!APORT! listenaddress=0.0.0.0 connectport=!APORT! connectaddress=!WSLIP!
netsh advfirewall firewall delete rule name="Cascade Agent !APORT!" >nul 2>&1
netsh advfirewall firewall add rule name="Cascade Agent !APORT!" dir=in action=allow protocol=TCP localport=!APORT!

REM ---- find this PC's LAN IP (interface that owns the default route; take the first match) ----
set "LANIP="
REM findstr regex: '.' already matches any char, '[.]' a literal dot. Do NOT use '\.' (findstr
REM does not treat it as an escaped dot, unlike grep) or the match silently fails.
for /f "tokens=3,4" %%a in ('route print -4 ^| findstr /r /c:"^ *0.0.0.0 *0.0.0.0"') do (
  if not defined LANIP set "LANIP=%%b"
)
REM validate it looks like an IPv4 (guards against a persistent-route "Default" token)
echo !LANIP!| findstr /r /c:"[0-9][0-9]*[.][0-9]" >nul || set "LANIP="

echo.
echo [4/4] Active port forwards:
netsh interface portproxy show v4tov4
echo.
if defined LANIP (
  echo === On the Mac, set configs/replica.toml -^> sync.remote_url = "http://!LANIP!:%PORT%" ===
  echo     Mac preflight:  curl http://!LANIP!:%PORT%/    ^(expect HTTP 404, not a timeout^)
) else (
  echo === Could not auto-detect the LAN IP. Run ipconfig, take the Ethernet/Wi-Fi IPv4, ===
  echo     and on the Mac use http://^<that-ip^>:%PORT% ===
)
echo.

REM ---- start the master inside WSL (builds on first run, then serves) ----
echo Starting the Cascade master in WSL (%DISTRO%)... Ctrl-C to stop.
echo.
wsl -d %DISTRO% -- bash -lc "cd '!REPO!' && chmod +x ./start-master.sh && ./start-master.sh"

echo.
echo (master stopped) — the port forward + firewall rules remain. Press any key to close.
pause >nul
endlocal
