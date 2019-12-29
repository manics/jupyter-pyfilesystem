# encoding: utf-8
"""
Utilities for testing.
"""
from __future__ import unicode_literals
from contextlib import contextmanager
from itertools import starmap
import posixpath
from unicodedata import normalize

from tornado.web import HTTPError

from nbformat.v4.nbbase import (
    new_code_cell,
    new_markdown_cell,
    new_notebook,
    new_raw_cell,
)

TEST_FS_URL = 'mem://'


def _norm_unicode(s):
    """Normalize unicode strings"""
    return normalize('NFC', s)


@contextmanager
def assertRaisesHTTPError(testcase, status, msg=None):
    msg = msg or "Should have raised HTTPError(%i)" % status
    try:
        yield
    except HTTPError as e:
        testcase.assertEqual(e.status_code, status)
    else:
        testcase.fail(msg)


# def clear_test_db():
# def remigrate_test_schema():
# def drop_testing_db_tables():
# def migrate_testing_db(revision='head'):

def get_test_notebook(name):
    """
    Make a test notebook for the given name.
    """
    nb = new_notebook()
    nb.cells.append(new_code_cell("'code_' + '{}'".format(name)))
    nb.cells.append(new_raw_cell("raw_{}".format(name)))
    nb.cells.append(new_markdown_cell('markdown_{}'.format(name)))
    return nb


def populate(contents_mgr):
    """
    Populate a test directory with a ContentsManager.
    """
    dirs_nbs = [
        ('', 'inroot.ipynb'),
        ('Directory with spaces in', 'inspace.ipynb'),
        ('unicodé', 'innonascii.ipynb'),
        ('foo', 'a.ipynb'),
        ('foo', 'name with spaces.ipynb'),
        ('foo', 'unicodé.ipynb'),
        ('foo/bar', 'baz.ipynb'),
        ('å b', 'ç d.ipynb'),
    ]

    for dirname, nbname in dirs_nbs:
        contents_mgr.save({'type': 'directory'}, path=dirname)
        contents_mgr.save(
            {'type': 'notebook', 'content': get_test_notebook(nbname)},
            path='{}/{}'.format(dirname, nbname),
        )
    return list(starmap(posixpath.join, dirs_nbs))


def _separate_dirs_files(models):
    """
    Split an iterable of models into a list of file paths and a list of
    directory paths.
    """
    dirs = []
    files = []
    for model in models:
        if model['type'] == 'directory':
            dirs.append(model['path'])
        else:
            files.append(model['path'])
    return dirs, files


def walk(mgr):
    """
    Like os.walk, but written in terms of the ContentsAPI.

    Takes a ContentsManager and returns a generator of tuples of the form:
    (directory name, [subdirectories], [files in directory])
    """
    return walk_dirs(mgr, [''])


def walk_dirs(mgr, dirs):
    """
    Recursive helper for walk.
    """
    for directory in dirs:
        children = mgr.get(
            directory,
            content=True,
            type='directory',
        )['content']
        dirs, files = map(sorted, _separate_dirs_files(children))
        yield directory, dirs, files
        if dirs:
            for entry in walk_dirs(mgr, dirs):
                yield entry


def walk_files(mgr):
    """
    Iterate over all files visible to ``mgr``.
    """
    for dir_, subdirs, files in walk_files(mgr):
        for file_ in files:
            yield file_


def walk_files_with_content(mgr):
    """
    Iterate over the contents of all files visible to ``mgr``.
    """
    for _, _, files in walk(mgr):
        for f in files:
            yield mgr.get(f, content=True)
