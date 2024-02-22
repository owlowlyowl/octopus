from setuptools import find_packages, setup

setup(
    name="octopus_dagster",
    packages=find_packages(exclude=["octopus_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "sqlalchemy",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
            "pytest",
            "pre-commit",
        ],
    },
)
