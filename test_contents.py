# To run these tests set environment variables
# OMERO_HOST=
# OMERO_SESSION=
# import pytest
from tornado.web import HTTPError
from jupyter_omero_contents import OmeroContentsManager


class TestContentsManager:
    @classmethod
    def setup_class(cls):
        cls.c = OmeroContentsManager()

    @classmethod
    def teardown_class(cls):
        cls.c.client.stopKeepAlive()
        cls.c.client.closeSession()

    def test_get_directory(self):
        assert self.c.dir_exists('/jupyter')
        d = self.c.get('/jupyter', content=False, type='directory')
        assert d['name'] == 'jupyter'
        assert d['path'] == '/jupyter'
        assert d['type'] == 'directory'

    def test_file(self):
        f = '/jupyter/hello.txt'
        try:
            self.c.delete_file(f)
        except HTTPError:
            pass
        assert not self.c.file_exists(f)

        model = {
            'type': 'file',
            'content': 'hello world\n',
            'format': 'text',
            'writable': True,
            'last_modified': None,
            'created': None,
            'mimetype': 'text/plain',
        }
        self.c.save(model, f)

        fetch = self.c.get(f, 'content=True', type='file')
        assert fetch['path'] == f
        assert fetch['name'] == 'hello.txt'
        assert fetch['content'] == 'hello world\n'
        assert fetch['format'] == model['format']
        assert fetch['mimetype'] == model['mimetype']
        assert fetch['size'] == 12

        model['content'] = 'Hello World!\n'
        self.c.save(model, f)
        fetch2 = self.c.get(f, 'content=True', type='file')
        assert fetch2['content'] == 'Hello World!\n'
        assert fetch2['size'] == 13

        d = self.c.get('/jupyter', content=True, type='directory')
        paths = [f['path'] for f in d['content']]
        assert f in paths
