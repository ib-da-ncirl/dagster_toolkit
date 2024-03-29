import os
import setuptools

_here = os.path.abspath(os.path.dirname(__file__))

with open("README.md", "r") as fh:
    long_description = fh.read()

version = {}
with open(os.path.join(_here, 'dagster_toolkit', 'version.py')) as f:
    exec(f.read(), version)

setuptools.setup(
    name="dagster_toolkit",
    version=version['__version__'],
    author="Ian Buttimer",
    author_email="author@example.com",
    description="Dagster utility functions/solids",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ib-da-ncirl/dagster_toolkit",
    license='MIT',
    packages=setuptools.find_packages(),
    install_requires=[
      'psycopg2>=2.8.4',
      'pymongo>=3.10.0',
      'dagster>=0.13.1',
      'dagit>=0.13.1',
      'dagster_pandas>=0.13.1',
      'pandas>=1.3.4'
    ],
    dependency_links=[
        'git+https://github.com/ib-da-ncirl/db_toolkit.git#egg=db_toolkit',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
