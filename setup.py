import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="jwt-service",
    version="0.0.1",
    author="Artem Antonov",
    author_email="artyom.antnv@gmail.com",
    description="...",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/skipp1/S954_Skipp3_0/jwt",
    packages=setuptools.find_packages(),
    install_requires=[
        'pydantic==1.9.0',
        'PyJWT==2.4.0',
        'typing_extensions==4.0.1',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.10',
)
