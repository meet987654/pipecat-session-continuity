import subprocess
import sys

def main():
    print("Building pipecat-session-continuity...")
    result = subprocess.run([sys.executable, "-m", "build"], capture_output=False)
    if result.returncode == 0:
        print("Build successful! The package is available in the dist/ directory.")
    else:
        print(f"Build failed with exit code {result.returncode}")
        sys.exit(result.returncode)

if __name__ == "__main__":
    main()
