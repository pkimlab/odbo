from setuptools import setup, find_packages

setup(
    name='datapkg',
    version="0.0.3",
    author='Alexey Strokach',
    author_email='alex.strokach@utoronto.ca',
    packages=find_packages(),
    entry_points={'console_scripts': ['datapkg=datapkg.__main__:main']},
)
