from setuptools import setup

with open("./README.md") as fp:
    long_description = fp.read()

setup(
    name="adbcug_adapter",
    author="Anthony Mahanna",
    author_email="anthony.mahanna@arangodb.com",
    description="Convert ArangoDB graphs to cuGraph",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/arangoml/cugraph-adapter",
    keywords=["arangodb", "cugraph", "adapter"],
    packages=["adbcug_adapter"],
    include_package_data=True,
    use_scm_version=True,
    python_requires=">=3.7",
    license="Apache Software License",
    install_requires=[
        "python-arango>=7.3.4",
        "setuptools>=42",
    ],
    extras_require={
        "dev": [
            "black",
            "flake8>=3.8.0",
            "isort>=5.0.0",
            "mypy>=0.790",
            "pytest>=6.0.0",
            "pytest-cov>=2.0.0",
            "coveralls>=3.3.1",
            "setuptools_scm[toml]>=3.4",
            "types-setuptools",
        ],
    },
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
