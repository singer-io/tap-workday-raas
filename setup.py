#usr/bin/env python
from setuptools import setup

setup(
    name="tap-workday-raas",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_workday_raas"],
    install_requires=[
        'singer-python==5.9.0',
        'backoff==1.8.0',
        'requests==2.22.0',
        'ijson==3.0.4',
    ],
    extras_require={
        'dev': [
            'ipdb',
            'pylint',
            'nose'
        ]
    },
    entry_points="""
    [console_scripts]
    tap-workday-raas=tap_workday_raas:main
    """,
    packages=["tap_workday_raas"],
    package_data = {
    },
    include_package_data=True,
)
