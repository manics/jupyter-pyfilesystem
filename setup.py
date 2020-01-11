import setuptools

setuptools.setup(
    name='jupyter-pyfilesystem',
    url='https://github.com/manics/jupyter-pyfilesystem',
    author='Simon Li',
    license='MIT',
    description='Jupyter Notebook PyFilesystem Contents Manager',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    use_scm_version={
        'write_to': 'jupyter_pyfilesystem/_version.py',
    },
    packages=setuptools.find_packages(),
    setup_requires=[
        'setuptools_scm',
    ],
    install_requires=[
        'notebook',
        'fs>=2,<3',
    ],
    tests_require=[
        'pytest',
    ],
    python_requires='>=3.5',
    classifiers=[
        'Framework :: Jupyter',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Filesystems',
    ],
)
