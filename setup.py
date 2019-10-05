import setuptools

setuptools.setup(
    name='jupyter-omero-contents',
    version='0.0.1',
    url='https://github.com/manics/jupyter-omero-contents',
    author='Simon Li',
    license='BSD 3-Clause',
    description='Jupyter Notebook OMERO Contents Manager',
    packages=setuptools.find_packages(),
    install_requires=['notebook'],
    python_requires='>=3.5',
    classifiers=[
        'Framework :: Jupyter',
    ],
)
