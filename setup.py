from setuptools import find_packages, setup

setup(
    name="compagnon",
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "compagnon=compagnon.__main__:app",
        ]
    },
    install_requires=[
        "datameta-client",
        "pyyaml",
    ],
    extras_require={
        "tests": ["pytest", "pytest-cov"],
        "postgres": [
            "psycopg2-binary",
            "SQLAlchemy",
        ],
    },
)
