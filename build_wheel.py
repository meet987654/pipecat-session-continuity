import os
import setuptools.build_meta

os.makedirs('dist', exist_ok=True)
setuptools.build_meta.build_wheel('dist')
print("Wheel built successfully in dist/")
