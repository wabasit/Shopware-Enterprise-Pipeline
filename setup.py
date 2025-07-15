from setuptools import setup, find_packages

setup(
    name="api_extractor",
    version="0.1.0",
    description="Reusable API ingestion utility with retry, pagination, and error handling",
    author="A. W. Basit",
    packages=find_packages(),
    install_requires=["requests"],
    python_requires='>=3.7',
)