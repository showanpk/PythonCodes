# run_both_pipelines.py
import subprocess
import sys
from pathlib import Path

SCRIPT1 = Path(r"C:\Users\shonk\source\PythonCodes\Pycode for HealthREg\saheli_all_in_one_pipeline.py")
SCRIPT2 = Path(r"C:\Users\shonk\source\PythonCodes\Pycode for HealthREg\saheli_all_in_one_pipeline4.py")

def run_script(script_path: Path):
    if not script_path.exists():
        raise FileNotFoundError(f"File not found: {script_path}")

    print(f"\n=== Running: {script_path.name} ===")
    result = subprocess.run([sys.executable, str(script_path)], check=False)

    if result.returncode != 0:
        raise RuntimeError(f"{script_path.name} failed with exit code {result.returncode}")

    print(f"=== Done: {script_path.name} ===")

def main():
    run_script(SCRIPT1)
    run_script(SCRIPT2)
    print("\n✅ Both scripts completed successfully.")

if __name__ == "__main__":
    main()
