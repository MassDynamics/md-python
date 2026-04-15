from setuptools import setup, find_packages

setup(
    name="md-cli",
    version="2.0.0",
    description="Command-line interface for the Mass Dynamics proteomics platform",
    packages=find_packages(),
    install_requires=["click>=8.0", "md-python>=0.2.0"],
    entry_points={"console_scripts": ["md=md_cli.main:main"]},
    python_requires=">=3.10",
)
