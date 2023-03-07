from setuptools import find_packages, setup

setup(
    name="compagnon",
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "compagnon=compagnon.entrypoints.app:app",
        ]
    },
    install_requires=[
        "datameta-client",
        "pyyaml",
        "SQLAlchemy",
        "psycopg2",
        "psycopg2-binary",
    ],
    extras_require={
        "tests": ["flake8", "pytest", "pytest-cov", "validators"],
        "KrakenExecution": ["pandas",]
    },
)
