import setuptools

setuptools.setup(
    name='jupyter-pyfilesystem',
    version='0.0.1',
    url='https://github.com/manics/jupyter-pyfilesystem',
    author='Simon Li',
    license='BSD 3-Clause',
    description='Jupyter Notebook PyFilesystem Contents Manager',
    packages=setuptools.find_packages(),
    install_requires=[
        'notebook',
        'fs>=2'
    ],
    tests_requires=[
        'pytest',
        'nose',
    ],
    python_requires='>=3.5',
    classifiers=[
        'Framework :: Jupyter',
    ],
)
