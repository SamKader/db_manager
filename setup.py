
#python
# setup.py
from setuptools import setup, find_packages

setup(
    name="db_manager",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[],
    author="SAMANDOULGOU",
    author_email="kadersamandoulgou15@gmail.com",
    description="Un module pour gérer une base de données SQLite",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/SamKader/SamKader.git",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)