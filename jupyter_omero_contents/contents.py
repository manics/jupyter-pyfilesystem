from notebook.services.contents.manager import ContentsManager
from notebook.services.contents.checkpoints import (
    Checkpoints,
    GenericCheckpointsMixin,
)
from traitlets import (
    default,
    HasTraits,
    Instance,
    Unicode,
)
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
from time import time

import omero.clients
from omero.gateway import BlitzGateway
from omero.rtypes import (
    rstring,
    rtime,
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


class OmeroManagerMixin(HasTraits):
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

    conn = Instance(
        omero.gateway.BlitzGateway,
        allow_none=False,
        help=('OMERO BlitzGateway object with active session, default is to '
              'create a new connection using provided parameters'),
        config=True,
    )

    @default('conn')
    def _conn_default(self):
        client = omero.client(self.omero_host)
        if self.omero_session:
            session = client.joinSession(self.omero_session)
            session.detachOnDestroy()
            self.log.info('Logged in to %s with existing session',
                          self.omero_host)
        else:
            session = client.createSession(
                self.omero_user, self.omero_password)
            self.log.info('Logged in to %s with new session', self.omero_host)
        # TODO: enableKeepAlive seems to prevent shutdown, try tornado
        # background loop which calls conn.c.sf.keepAlive(None) instead
        client.enableKeepAlive(60)
        return BlitzGateway(client_obj=client)

    def _get_mtime(self, f):
        return datetime.utcfromtimestamp(f.getMtime()/1000)

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
        self.log.debug('get(%s)', path)
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

    def _get_notebook(self, path, content, format, trust=True):
        model = self._get_file(path, content, format)
        model['type'] = 'notebook'
        if content:
            nb = nbformat.reads(model['content'], as_version=4)
            if trust:
                self.mark_trusted_cells(nb, path)
            model['content'] = nb
            model['format'] = 'json'
            if trust:
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
        model = _base_model(f.getPath(), f.getName())
        model['type'] = 'file'
        model['last_modified'] = model['created'] = self._get_mtime(f)
        model['size'] = f.getSize()
        if content:
            model['content'], model['format'] = self._read_file(f, format)
            model['mimetype'] = f.getMimetype()
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
        with f.asFileObj() as fo:
            bcontent = fo.read()
        if format is None or format == 'text':
            try:
                return bcontent.decode('utf8'), 'text'
            except UnicodeError:
                if format == 'text':
                    raise HTTPError(
                        400,
                        "{}/{} is not UTF-8 encoded".format(f.path, f.name),
                        reason='bad format')
        return b64encode(bcontent).decode('ascii'), 'base64'

    def save(self, model, path):
        self.log.debug('save(%s)', path)
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

    def _save_notebook(self, model, path, sign=True):
        nb = nbformat.from_dict(model['content'])
        if sign:
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
            f.setMtime(rtime(time()))
            if 'mimetype' in model:
                f.setMimetype(rstring(model['mimetype']))
            f = updatesrv.saveAndReturnObject(f)
        rfs = self.conn.c.sf.createRawFileStore()
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
        self.log.debug('delete_file(%s)', path)
        path = _normalise_path(path)
        f = self._get_omero_file(path)
        self.conn.deleteObject(f._obj)

    def rename_file(self, old_path, new_path):
        self.log.debug('rename_file(%s %s)', old_path, new_path)
        old_path = _normalise_path(old_path)
        new_path = _normalise_path(new_path)
        dirname, name = new_path.rsplit('/', 1)
        if dirname != '/jupyter' or old_path.rsplit('/', 1)[0] != '/jupyter':
            raise HTTPError(400, 'Directory must be /jupyter')
        if self.file_exists(new_path):
            raise HTTPError(
                400, 'File {} exists, please delete first'.format(new_path))
        f = self._get_omero_file(old_path)._obj
        f.setName(rstring(name))
        updatesrv = self.conn.getUpdateService()
        f = updatesrv.saveAndReturnObject(f)
        # TODO:
        # This causes an error in super().rename() due to checkpoints not being
        # implemented:
        # self.checkpoints.rename_all_checkpoints(old_path, new_path)

    def file_exists(self, path):
        self.log.debug('file_exists(%s)', path)
        path = _normalise_path(path)
        try:
            return self._get_omero_file(path)
        except HTTPError:
            return False

    def dir_exists(self, path):
        self.log.debug('dir_exists(%s)', path)
        path = _normalise_path(path)
        return path == '/' or path.strip('/') == 'jupyter'

    def is_hidden(self, path):
        self.log.debug('is_hidden(%s)', path)
        path = _normalise_path(path)
        return path.rsplit('/', 1)[-1].startswith('.')


class OmeroContentsManager(OmeroManagerMixin, ContentsManager):
    pass


class OmeroCheckpoints(
        OmeroManagerMixin, GenericCheckpointsMixin, Checkpoints):
    """
    Since we don't support directories use a flat filename instead
    """

    checkpoint_prefix = Unicode(
        '._checkpoint{id}_',
        config=True,
        help="""The prefix to add to checkpoint files. `{id}` will be replaced
        with a checkpoint id.""",
    )

    def _checkpoint_path(self, checkpoint_id, path):
        """find the path to a checkpoint"""
        path = _normalise_path(path)
        parent, name = path.rsplit('/', 1)
        prefix = self.checkpoint_prefix.format(id=checkpoint_id)
        cp_path = u"{}/{}{}".format(parent, prefix, name)
        return cp_path

    def _checkpoint_model(self, checkpoint_id, f):
        """construct the info dict for a given checkpoint"""
        info = {'id': str(checkpoint_id)}
        if isinstance(f, dict):
            info['last_modified'] = f['last_modified']
        else:
            info['last_modified'] = self._get_mtime(f)
        return info

    def create_file_checkpoint(self, content, format, path):
        self.log.debug('create_file_checkpoint(%s)', path)
        cp_path = self._checkpoint_path(0, path)
        model = _base_model(*cp_path.rsplit('/', 1))
        model['content'] = content
        model['format'] = format
        f = self._save_file(cp_path, model)
        return self._checkpoint_model(0, f)

    def create_notebook_checkpoint(self, nb, path):
        self.log.debug('create_notebook_checkpoint(%s)', path)
        cp_path = self._checkpoint_path(0, path)
        model = _base_model(*cp_path.rsplit('/', 1))
        model['content'] = nb
        f = self._save_notebook(model, cp_path, False)
        return self._checkpoint_model(0, f)

    def get_file_checkpoint(self, checkpoint_id, path):
        # -> {'type': 'file', 'content': <str>, 'format': {'text', 'base64'}}
        self.log.debug('get_file_checkpoint(%s %s)', checkpoint_id, path)
        cp_path = self._checkpoint_path(checkpoint_id, path)
        return self._get_file(cp_path, True, None)

    def get_notebook_checkpoint(self, checkpoint_id, path):
        # -> {'type': 'notebook', 'content': <output of nbformat.read>}
        self.log.debug('get_notebook_checkpoint(%s %s)', checkpoint_id, path)
        cp_path = self._checkpoint_path(checkpoint_id, path)
        return self._get_notebook(cp_path, True, 'text', False)

    def delete_checkpoint(self, checkpoint_id, path):
        self.log.debug('delete_checkpoint(%s %s)', checkpoint_id, path)
        cp_path = self._checkpoint_path(checkpoint_id, path)
        self.delete_file(cp_path)

    def list_checkpoints(self, path):
        self.log.debug('list_checkpoints(%s)', path)
        cp_path = self._checkpoint_path(0, path)
        f = self.file_exists(cp_path)
        if f:
            return [self._checkpoint_model(0, f)]
        return []

    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        self.log.debug(
            'rename_checkpoint(%s %s %s)', checkpoint_id, old_path, new_path)
        cp_path_old = self._checkpoint_path(checkpoint_id, old_path)
        cp_path_new = self._checkpoint_path(checkpoint_id, new_path)
        self.rename_file(cp_path_old, cp_path_new)

    # # Error Handling
    # def no_such_checkpoint(self, path, checkpoint_id):
    #     raise HTTPError(
    #         404,
    #         u'Checkpoint does not exist: %s@%s' % (path, checkpoint_id)
    #     )
