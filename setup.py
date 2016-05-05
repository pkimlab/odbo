from setuptools import setup

setup(
    name='datapkg',
    version="0.0.2",
    author='Alexey Strokach',
    author_email='alex.strokach@utoronto.ca',
    packages=['datapkg'],
    entry_points={'console_scripts': ['datapkg=datapkg.__main__:main']},
)
