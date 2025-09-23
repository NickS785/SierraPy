#!/usr/bin/env python3
"""
Setup script for SierraPy - A Python library for Sierra Chart data parsing and analysis.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the contents of README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8') if (this_directory / "README.md").exists() else ""

# Read requirements
requirements = []
requirements_file = this_directory / "requirements.txt"
if requirements_file.exists():
    requirements = requirements_file.read_text(encoding='utf-8').strip().split('\n')
    requirements = [req.strip() for req in requirements if req.strip() and not req.startswith('#')]

setup(
    name="sierrapy",
    version="0.1.0",
    author="Nicholas Sanders",
    author_email="sandersnicholas68@gmail.com",
    description="A Python library for Sierra Chart data parsing and analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/sierrapy",
    project_urls={
        "Bug Tracker": "https://github.com/yourusername/sierrapy/issues",
        "Documentation": "https://github.com/yourusername/sierrapy/wiki",
        "Source Code": "https://github.com/yourusername/sierrapy",
    },
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Office/Business :: Financial",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=[
        "numpy>=1.20.0",
        "pandas>=1.3.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov",
            "black",
            "flake8",
            "mypy",
        ],
        "optional": [
            "pyarrow>=5.0.0",  # For Parquet export
        ],
    },
    entry_points={
        "console_scripts": [
            "sierrapy-scid=sierrapy.parser.scid_parse:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords="sierra chart, financial data, futures, options, market data, trading",
)