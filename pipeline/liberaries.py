import subprocess
import sys

# Install required libraries into the current Python environment
packages = [
    "pandas",
    "sqlalchemy",
    "psycopg2-binary",
    "requests",
]

for package in packages:
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    print(f"Installed: {package}")

print("\nAll libraries installed successfully!")
