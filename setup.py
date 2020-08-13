from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='finance-sdk',
    version='v0.0.0',
    description='utils for building finance tools in python',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='git@github.com:hunter-beck/finance-sdk.git',
    author='Hunter Beck',
    author_email='hunterbeck3@gmail.com',
    license='unlicense',
    packages=find_packages('finance'),
    zip_safe=False
)