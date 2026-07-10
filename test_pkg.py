import subprocess
import os

print("Creating venv...")
subprocess.check_call(["python", "-m", "venv", "test-install-pkg"])

pip_path = os.path.join("test-install-pkg", "Scripts", "pip")
python_path = os.path.join("test-install-pkg", "Scripts", "python")

print("Installing package into venv...")
subprocess.check_call([pip_path, "install", "dist/pipecat_session_continuity-0.1.0-py3-none-any.whl"])

print("Testing import...")
output = subprocess.check_output([python_path, "-c", "from pipecat_session_continuity import SessionContinuity; print('ok')"])
print(output.decode())
