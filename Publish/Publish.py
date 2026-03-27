import sys
import shutil
import subprocess
from pathlib import Path

# =========================================================
# CONFIG
# =========================================================

VM_NAME = "20.68.160.100"
VM_USER = r"20.68.160.100\Sahelihub"

# Backend
BACKEND_PROJECT = r"C:\Users\shonk\source\repos\SaheliCRM\SaheliCRM\SaheliCRM.csproj"
BACKEND_PUBLISH_OUTPUT = r"C:\Users\shonk\source\repos\SaheliCRM\SaheliCRM\bin\Release\net8.0\publish"
REMOTE_API_PATH = r"C:\inetpub\SaheliBackend"
API_APP_POOL = "SaheliBackend"

# Frontend
FRONTEND_ROOT = r"C:\Users\shonk\OneDrive\Desktop\SaheliFront"
FRONTEND_PUBLISH_OUTPUT = r"C:\Users\shonk\OneDrive\Desktop\SaheliFront\dist\saheli-hub-portal\browser"
REMOTE_FRONTEND_PATH = r"C:\inetpub\wwwroot"
FRONTEND_APP_POOL = "DefaultAppPool"
ANGULAR_BASE_HREF = "/"

# Temp folders
LOCAL_TEMP_DIR = r"C:\Users\shonk\source\PythonCodes\Publish\temp"
REMOTE_TEMP_DIR = r"C:\DeployTemp"

# Build switches
BUILD_BACKEND = True
BUILD_FRONTEND = True


# =========================================================
# HELPERS
# =========================================================

def run_command(command, cwd=None, check=True):
    print(f"\n>> {' '.join(command)}")
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        capture_output=True,
        shell=False
    )

    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)

    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {result.returncode}: {' '.join(command)}"
        )

    return result


def ensure_exists(path: str, label: str):
    if not Path(path).exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def ensure_clean_dir(path: str):
    p = Path(path)
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)


def remove_file_if_exists(path: Path):
    if path.exists():
        path.unlink()


def create_zip_from_folder(source_folder: str, zip_path: str):
    source = Path(source_folder)
    zip_file = Path(zip_path)

    if not source.exists():
        raise FileNotFoundError(f"Zip source folder not found: {source_folder}")

    remove_file_if_exists(zip_file)
    base_name = str(zip_file.with_suffix(""))

    print(f"\n>> Creating zip: {zip_file}")
    shutil.make_archive(base_name=base_name, format="zip", root_dir=str(source), base_dir=".")


def ps_escape(value: str) -> str:
    return value.replace("'", "''")


def run_powershell_script(script: str):
    return run_command([
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        script
    ])


# =========================================================
# BUILD
# =========================================================

def build_backend():
    ensure_exists(BACKEND_PROJECT, "Backend project")
    project_dir = str(Path(BACKEND_PROJECT).parent)

    publish_path = Path(BACKEND_PUBLISH_OUTPUT)
    if publish_path.exists():
        shutil.rmtree(publish_path)

    run_command(["dotnet", "restore", BACKEND_PROJECT], cwd=project_dir)
    run_command(
        ["dotnet", "publish", BACKEND_PROJECT, "-c", "Release", "-o", BACKEND_PUBLISH_OUTPUT],
        cwd=project_dir
    )


def build_frontend():
    ensure_exists(FRONTEND_ROOT, "Frontend root folder")

    angular_json = Path(FRONTEND_ROOT) / "angular.json"
    package_json = Path(FRONTEND_ROOT) / "package.json"

    if not angular_json.exists():
        raise FileNotFoundError(f"angular.json not found in {FRONTEND_ROOT}")
    if not package_json.exists():
        raise FileNotFoundError(f"package.json not found in {FRONTEND_ROOT}")

    run_command(["npm.cmd", "install"], cwd=FRONTEND_ROOT)
    run_command(
        ["npx.cmd", "ng", "build", "--configuration", "production", "--base-href", ANGULAR_BASE_HREF],
        cwd=FRONTEND_ROOT
    )

    ensure_exists(FRONTEND_PUBLISH_OUTPUT, "Frontend publish output")


# =========================================================
# DEPLOY
# =========================================================

def deploy_with_single_session(api_zip: str, frontend_zip: str):
    ps = f"""
$cred = Get-Credential -UserName '{ps_escape(VM_USER)}' -Message 'Enter credentials for {ps_escape(VM_NAME)}'
$session = New-PSSession -ComputerName '{ps_escape(VM_NAME)}' -Credential $cred

try {{
    $remoteTempDir = '{ps_escape(REMOTE_TEMP_DIR)}'
    $remoteApiPath = '{ps_escape(REMOTE_API_PATH)}'
    $remoteFrontendPath = '{ps_escape(REMOTE_FRONTEND_PATH)}'
    $apiAppPool = '{ps_escape(API_APP_POOL)}'
    $frontendAppPool = '{ps_escape(FRONTEND_APP_POOL)}'

    $localApiZip = '{ps_escape(str(Path(api_zip).resolve()))}'
    $localFrontendZip = '{ps_escape(str(Path(frontend_zip).resolve()))}'

    $remoteApiZip = Join-Path $remoteTempDir 'backend_publish.zip'
    $remoteFrontendZip = Join-Path $remoteTempDir 'frontend_publish.zip'

    Invoke-Command -Session $session -ScriptBlock {{
        param($remoteTempDir, $remoteApiPath, $remoteFrontendPath, $apiAppPool)

        Import-Module WebAdministration

        New-Item -ItemType Directory -Path $remoteTempDir -Force | Out-Null
        New-Item -ItemType Directory -Path $remoteApiPath -Force | Out-Null
        New-Item -ItemType Directory -Path $remoteFrontendPath -Force | Out-Null

        @"
<html>
<head><title>Maintenance</title></head>
<body>
  <h2>Service temporarily unavailable</h2>
  <p>Please try again shortly.</p>
</body>
</html>
"@ | Set-Content -Path (Join-Path $remoteApiPath 'app_offline.htm') -Encoding UTF8

        if (Test-Path "IIS:\\AppPools\\$apiAppPool") {{
            Restart-WebAppPool -Name $apiAppPool
        }}

        if (Test-Path $remoteApiPath) {{
            Get-ChildItem -Path $remoteApiPath -Force |
                Where-Object {{ $_.Name -ne 'app_offline.htm' }} |
                Remove-Item -Recurse -Force -ErrorAction Stop
        }}

        if (Test-Path $remoteFrontendPath) {{
            Get-ChildItem -Path $remoteFrontendPath -Force |
                Remove-Item -Recurse -Force -ErrorAction Stop
        }}
    }} -ArgumentList $remoteTempDir, $remoteApiPath, $remoteFrontendPath, $apiAppPool

    Copy-Item -Path $localApiZip -Destination $remoteApiZip -ToSession $session -Force
    Copy-Item -Path $localFrontendZip -Destination $remoteFrontendZip -ToSession $session -Force

    Invoke-Command -Session $session -ScriptBlock {{
        param($remoteTempDir, $remoteApiPath, $remoteFrontendPath, $apiAppPool, $frontendAppPool)

        Import-Module WebAdministration

        $remoteApiZip = Join-Path $remoteTempDir 'backend_publish.zip'
        $remoteFrontendZip = Join-Path $remoteTempDir 'frontend_publish.zip'

        Expand-Archive -Path $remoteApiZip -DestinationPath $remoteApiPath -Force
        Expand-Archive -Path $remoteFrontendZip -DestinationPath $remoteFrontendPath -Force

        @"
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <system.webServer>
    <defaultDocument>
      <files>
        <clear />
        <add value="index.html" />
      </files>
    </defaultDocument>
    <staticContent>
      <remove fileExtension=".json" />
      <mimeMap fileExtension=".json" mimeType="application/json" />
      <remove fileExtension=".webmanifest" />
      <mimeMap fileExtension=".webmanifest" mimeType="application/manifest+json" />
    </staticContent>
  </system.webServer>
</configuration>
"@ | Set-Content -Path (Join-Path $remoteFrontendPath 'web.config') -Encoding UTF8

        $offlineFile = Join-Path $remoteApiPath 'app_offline.htm'
        if (Test-Path $offlineFile) {{
            Remove-Item $offlineFile -Force
        }}

        if (Test-Path $remoteApiZip) {{
            Remove-Item $remoteApiZip -Force
        }}

        if (Test-Path $remoteFrontendZip) {{
            Remove-Item $remoteFrontendZip -Force
        }}

        if (Test-Path "IIS:\\AppPools\\$apiAppPool") {{
            Restart-WebAppPool -Name $apiAppPool
        }}

        if (Test-Path "IIS:\\AppPools\\$frontendAppPool") {{
            Restart-WebAppPool -Name $frontendAppPool
        }}
    }} -ArgumentList $remoteTempDir, $remoteApiPath, $remoteFrontendPath, $apiAppPool, $frontendAppPool
}}
finally {{
    if ($session) {{
        Remove-PSSession $session
    }}
}}
"""
    run_powershell_script(ps)


# =========================================================
# MAIN
# =========================================================

def main():
    try:
        temp_dir = Path(LOCAL_TEMP_DIR)
        ensure_clean_dir(str(temp_dir))

        if BUILD_BACKEND:
            print("\n========== BACKEND BUILD START ==========")
            build_backend()
            print("========== BACKEND BUILD DONE ==========")

        if BUILD_FRONTEND:
            print("\n========== FRONTEND BUILD START ==========")
            build_frontend()
            print("========== FRONTEND BUILD DONE ==========")

        ensure_exists(BACKEND_PUBLISH_OUTPUT, "Backend publish output")
        ensure_exists(FRONTEND_PUBLISH_OUTPUT, "Frontend publish output")

        backend_zip = str(temp_dir / "backend_publish.zip")
        frontend_zip = str(temp_dir / "frontend_publish.zip")

        create_zip_from_folder(BACKEND_PUBLISH_OUTPUT, backend_zip)
        create_zip_from_folder(FRONTEND_PUBLISH_OUTPUT, frontend_zip)

        print("\n========== DEPLOY START ==========")
        deploy_with_single_session(backend_zip, frontend_zip)
        print("========== DEPLOY DONE ==========")

        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            print(f"Removed temp folder: {temp_dir}")

        print("\nDeployment completed successfully.")

    except Exception as exc:
        print(f"\nDeployment failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()