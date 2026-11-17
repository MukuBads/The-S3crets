"""Run the full pipeline: inspect -> expand -> inject issues -> validate & report."""
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
for step in ["01_inspect.py", "02_expand.py", "03_inject_issues.py", "04_validate_report.py"]:
    print(f"\n######## {step}")
    rc = subprocess.call([sys.executable, str(HERE / step)], cwd=HERE.parent)
    if rc:
        sys.exit(f"{step} failed with exit code {rc}")
print("\nPipeline finished.")
