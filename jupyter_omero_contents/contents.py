from notebook.services.contents.manager import ContentsManager
from traitlets import Unicode
from tornado.web import HTTPError
from base64 import (
    b64encode,
    b64decode,
)
from datetime import datetime
from hashlib import sha1
import mimetypes
import nbformat
import os
import omero.clients
from omero.gateway import BlitzGateway
from omero.rtypes import (
    rstring,
    unwrap,
)

# Directories don't really exist
DUMMY_CREATED_DATE = datetime.fromtimestamp(0)


# https://github.com/quantopian/pgcontents/blob/5fad3f6840d82e6acde97f8e3abe835765fa824b/pgcontents/api_utils.py#L25
def _base_model(dirname, name):
    return {
        'name': name,
        'path': dirname + '/' + name,
        'writable': True,
        'last_modified': None,
        'created': None,
        'content': None,
        'format': None,
        'mimetype': None,
        'size': 0,
        'type': None,
    }


def _normalise_path(p):
    if not p.startswith('/'):
        p = '/' + p
    return p


class OmeroContentsManager(ContentsManager):
    """
    https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html
    https://github.com/jupyter/notebook/blob/6.0.1/notebook/services/contents/manager.py
    https://github.com/jupyter/notebook/blob/6.0.1/notebook/services/contents/filemanager.py
    https://github.com/quantopian/pgcontents/blob/master/pgcontents/pgmanager.py

    A single flat directory called jupyter containing OriginalFiles

    Note checkpoints need to be either disabled or a different class configured
    https://github.com/jupyter/notebook/blob/b8b66332e2023e83d2ee04f83d8814f567e01a4e/notebook/services/contents/filecheckpoints.py
    """

    omero_host = Unicode(
        os.getenv('OMERO_HOST', ''),
        help='OMERO host or URL connection',
        config=True,
    )

    omero_user = Unicode(
        os.getenv('OMERO_USER'),
        allow_none=True,
        help='OMERO session ID',
        config=True,
    )

    omero_password = Unicode(
        os.getenv('OMERO_PASSWORD'),
        allow_none=True,
        help='OMERO session ID',
        config=True,
    )

    omero_session = Unicode(
        os.getenv('OMERO_SESSION'),
        allow_none=True,
        help='OMERO session ID',
        config=True,
    )

    def __init__(self, *args, **kwargs):
        super(OmeroContentsManager, self).__init__(*args, **kwargs)
        self.client = omero.client(self.omero_host)
        if self.omero_session:
            session = self.client.joinSession(self.omero_session)
            session.detachOnDestroy()
            self.log.info('Logged in to %s with existing session',
                          self.omero_host)
        else:
            session = self.client.createSession(
                self.omero_user, self.omero_password)
            self.log.info('Logged in to %s with new session', self.omero_host)
        # self.client.enableKeepAlive(60)
        self.conn = BlitzGateway(client_obj=self.client)

    # https://github.com/quantopian/pgcontents/blob/5fad3f6840d82e6acde97f8e3abe835765fa824b/pgcontents/pgmanager.py#L115
    def guess_type(self, path, allow_directory=True):
        """
        Guess the type of a file.
        If allow_directory is False, don't consider the possibility that the
        file is a directory.
        """
        if path.endswith('.ipynb'):
            return 'notebook'
        elif allow_directory and self.dir_exists(path):
            return 'directory'
        else:
            return 'file'

    def get(self, path, content=True, type=None, format=None):
        self.log.debug('get: %s', path)
        path = _normalise_path(path)
        if type is None:
            type = self.guess_type(path)
        try:
            fn = {
                'notebook': self._get_notebook,
                'directory': self._get_directory,
                'file': self._get_file,
            }[type]
        except KeyError:
            raise ValueError("Unknown type passed: '{}'".format(type))
        return fn(path=path, content=content, format=format)

    def _get_notebook(self, path, content, format):
        model = self._get_file(path, content, format)
        model['type'] = 'notebook'
        if content:
            nb = nbformat.reads(model['content'], as_version=4)
            self.mark_trusted_cells(nb, path)
            model['content'] = nb
            model['format'] = 'json'
            self.validate_notebook_model(model)
        return model

    def _get_directory(self, path, content, format):
        """
        Only one directory /jupyter is currently supported
        """
        if path == '/':
            return self._get_root(content, format)
        if path.strip('/') != 'jupyter':
            raise HTTPError(404, 'Directory {} not found'.format(path))

        model = _base_model(*path.rsplit('/', 1))
        model['type'] = 'directory'
        model['size'] = None
        model['format'] = 'json'
        model['created'] = model['last_modified'] = DUMMY_CREATED_DATE

        if content:
            model['content'] = []
            for f in self.conn.getObjects(
                    'OriginalFile', attributes={'path': '/jupyter'}):
                model['content'].append(self._file_model(f, False, None))

        return model

    def _get_root(self, content, format):
        model = _base_model('', '')
        model['type'] = 'directory'
        model['size'] = None
        model['format'] = 'json'
        model['created'] = model['last_modified'] = DUMMY_CREATED_DATE
        if content:
            model['content'] = [
                self._get_directory('/jupyter', 'False', format)]
        return model

    def _get_file(self, path, content, format):
        f = self._get_omero_file(path)
        return self._file_model(f, content, format)

    def _file_model(self, f, content, format):
        # TODO: AttributeError: 'str' object has no attribute 'decode'
        # model = _base_model(f.getPath(), f.getName())
        model = _base_model(unwrap(f._obj.path), f.getName())
        model['type'] = 'file'
        model['last_modified'] = model['created'] = f.getDate()
        model['size'] = f.getSize()
        if content:
            model['content'], model['format'] = self._read_file(f, format)
            # AttributeError: 'str' object has no attribute 'decode'
            # model['mimetype'] = f.getMimetype()
            model['mimetype'] = unwrap(f._obj.mimetype)
            if not model['mimetype']:
                model['mimetype'] = mimetypes.guess_type(model['path'])[0]
        return model

    def _get_omero_file(self, path):
        if '/' not in path:
            raise HTTPError(404, 'File {} not found'.format(path))
        dirname, name = path.rsplit('/', 1)
        if dirname != '/jupyter':
            raise HTTPError(404, 'File {} not found'.format(path))
        f = self.conn.getObject(
            'OriginalFile', attributes={'path': '/jupyter', 'name': name})
        if not f:
            raise HTTPError(404, 'File {} not found'.format(path))
        return f

    def _read_file(self, f, format):
        """
        :param format:
            - 'text': contents will be decoded as UTF-8.
            - 'base64': raw bytes contents will be encoded as base64.
            - None: try to decode as UTF-8, and fall back to base64
        """
        # TODO: TypeError: must be str, not bytes
        # with f.asFileObj() as fo:
        #     bcontent = fo.read()
        rfs = f._conn.c.sf.createRawFileStore()
        rfs.setFileId(f.id, f._conn.SERVICE_OPTS)
        bcontent = rfs.read(0, rfs.size())
        if format is None or format == 'text':
            try:
                return bcontent.decode('utf8'), 'text'
            except UnicodeError:
                if format == 'text':
                    raise HTTPError(
                        400,
                        "{}/{} is not UTF-8 encoded".format(f.path, f.name),
                        reason='bad format')
        return b64encode(bcontent), 'base64'

    def save(self, model, path):
        self.log.debug('save: %s', path)
        self.run_pre_save_hook(model=model, path=path)
        path = _normalise_path(path)
        if 'type' not in model or not model['type']:
            raise HTTPError(400, 'No model type provided')
        try:
            fn = {
                'notebook': self._save_notebook,
                'directory': self._save_directory,
                'file': self._save_file,
            }[model['type']]
        except KeyError:
            raise ValueError("Unknown type passed: '{}'".format(type))
        return fn(model=model, path=path)

    def _save_notebook(self, model, path):
        nb = nbformat.from_dict(model['content'])
        self.check_and_sign(nb, path)
        model['content'] = nbformat.writes(nb)
        model['format'] = 'text'
        return self._save_file(path, model)

    def _save_directory(self, model, path):
        raise HTTPError(400, 'Saving directories not supported')

    def _save_file(self, path, model):
        if 'content' not in model:
            raise HTTPError(400, 'No file content provided')
        if model.get('format') not in {'text', 'base64'}:
            raise HTTPError(
                400, "Format of file contents must be 'text' or 'base64'")
        dirname, name = path.rsplit('/', 1)
        if dirname != '/jupyter':
            raise HTTPError(400, 'Directory must be /jupyter')
        updatesrv = self.conn.getUpdateService()
        try:
            f = self._get_omero_file(path)._obj
        except HTTPError:
            f = omero.model.OriginalFileI()
            f.setName(rstring(name))
            f.setPath(rstring(dirname))
            if 'mimetype' in model:
                f.setMimetype(rstring(model['mimetype']))
            f = updatesrv.saveAndReturnObject(f)
        rfs = self.client.sf.createRawFileStore()
        rfs.setFileId(unwrap(f.id), self.conn.SERVICE_OPTS)

        try:
            if model['format'] == 'text':
                bcontent = model['content'].encode('utf8')
            else:
                bcontent = b64decode(model['content'])
        except Exception as e:
            raise HTTPError(
                400, 'Encoding error saving {}: {}'.format(model['path'], e))

        size = len(bcontent)
        rfs.write(bcontent, 0, size)
        rfs.truncate(size)
        f = rfs.save()
        # Size and hash seem to be automatically set by rfs.save()
        # f.setHash(rstring(self._sha1(bcontent)))
        # chk = omero.model.ChecksumAlgorithmI()
        # chk.setValue(rstring(omero.model.enums.ChecksumAlgorithmSHA1160))
        # f.setHasher(chk)
        # f = updatesrv.saveAndReturnObject(f)
        return self._get_file(path, False, None)

    def _sha1(self, b):
        h = sha1()
        h.update(b)
        return h.hexdigest()

    def delete_file(self, path):
        self.log.debug('delete_file: %s', path)
        path = _normalise_path(path)
        f = self._get_omero_file(path)
        self.conn.deleteObject(f._obj)

    def rename_file(self, old_path, new_path):
        self.log.debug('rename_file: %s %s', old_path, new_path)
        old_path = _normalise_path(old_path)
        new_path = _normalise_path(new_path)
        dirname, name = new_path.rsplit('/', 1)
        if dirname != '/jupyter' or old_path.rsplit('/', 1)[0] != '/jupyter':
            raise HTTPError(400, 'Directory must be /jupyter')
        try:
            self._get_omero_file(new_path)
            raise HTTPError(
                400, 'File {} exists, please delete first'.format(new_path))
        except HTTPError:
            f = self._get_omero_file(old_path)._obj
            f.setName(rstring(name))
            updatesrv = self.conn.getUpdateService()
            f = updatesrv.saveAndReturnObject(f)

    def file_exists(self, path):
        self.log.debug('file_exists: %s', path)
        path = _normalise_path(path)
        try:
            self._get_omero_file(path)
            return True
        except HTTPError:
            return False

    def dir_exists(self, path):
        self.log.debug('dir_exists: %s', path)
        path = _normalise_path(path)
        return path == '/' or path.strip('/') == 'jupyter'

    def is_hidden(self, path):
        self.log.debug('is_hidden: %s', path)
        path = _normalise_path(path)
        return path.rsplit('/', 1)[-1].startswith('.')
