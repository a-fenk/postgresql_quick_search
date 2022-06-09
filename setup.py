import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="postgresql-quick-search",
    version="0.0.1",
    author="Artem Antonov",
    author_email="artyom.antnv@gmail.com",
    description="...",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/skipp1/S954_Skipp3_0/postgresql_quick_search",
    packages=setuptools.find_packages(),
    install_requires=[],
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.10',
)
