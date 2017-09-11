import versioneer

from setuptools import setup, find_packages

setup(
    name="scout2csv",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    install_requires=[
        "arrow",
        "boto3",
        "docopt",
        "keen"
    ],
    entry_points="""
        [console_scripts]
        scout2csv=scout2csv.cli:main
    """,
    author="datawire.io",
    author_email="dev@datawire.io",
    url="https://github.com/datawire/scout2csv",
    download_url="https://github.com/datawire/scout2csv/tarball/{}".format(versioneer.get_version()),
    keywords=[],
    classifiers=[],
)
