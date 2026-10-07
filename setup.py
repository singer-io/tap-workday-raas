# usr/bin/env python
from setuptools import setup

setup(
    name="tap-workday-raas",
    version="2.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    python_requires=">=3.9",
    py_modules=["tap_workday_raas"],
    install_requires=[
        "singer-python==6.8.0",
        "backoff==2.2.1",
        "requests==2.34.2",
        "ijson==3.5.1",
        "PyJWT==2.14.0",
        # Required at runtime: PyJWT's RS256 algorithm (used to sign JWT Bearer
        # assertions) needs the cryptography backend, but PyJWT does not pull
        # it in as a hard dependency on its own.
        "cryptography==50.0.2",
    ],
    extras_require={"dev": ["ipdb", "pylint", "nose"]},
    entry_points="""
    [console_scripts]
    tap-workday-raas=tap_workday_raas:main
    """,
    packages=["tap_workday_raas"],
    package_data={},
    include_package_data=True,
)
