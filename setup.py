from setuptools import setup, find_packages

setup(
    name="v3_polars",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "polars",
    ],
)
