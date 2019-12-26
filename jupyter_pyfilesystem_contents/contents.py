from notebook.services.contents.manager import ContentsManager
from notebook.services.contents.checkpoints import (
    Checkpoints,
    GenericCheckpointsMixin,
)
from traitlets import (
    default,
    HasTraits,
    Instance,
    TraitError,
    Unicode,
    validate,
)
from traitlets.config.configurable import SingletonConfigurable
from tornado.web import HTTPError
from base64 import (
    b64encode,
    b64decode,
)
from datetime import datetime
import mimetypes
import nbformat
import re

from fs import open_fs
from fs.base import FS
from fs.errors import ResourceNotFound
import fs.path as fspath


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
    path = '/' + p.strip('/')
    print('_normalise_path', p, path)
    return path


DEFAULT_CREATED_DATE = datetime.utcfromtimestamp(0)


def _created_modified(details):
    created = details.created or details.modified or DEFAULT_CREATED_DATE
    modified = details.modified or details.created or DEFAULT_CREATED_DATE
    return created, modified


class FilesystemHandle(SingletonConfigurable):

    def __init__(self, fs_url):
        if not re.match(r'^([a-z][a-z0-9+\-.]*)://', fs_url):
            raise TraitError('Invalid fs_url: {}'.format(fs_url))
        self.fs_url = fs_url
        self.log.debug('Opening filesystem %s', fs_url)
        self.fs = open_fs(self.fs_url, writeable=True, create=True)


class FsManagerMixin(HasTraits):
    """
    https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html
    https://github.com/jupyter/notebook/blob/6.0.1/notebook/services/contents/manager.py
    https://github.com/jupyter/notebook/blob/6.0.1/notebook/services/contents/filemanager.py
    https://github.com/quantopian/pgcontents/blob/master/pgcontents/pgmanager.py

    A single flat directory called jupyter containing OriginalFiles

    Note checkpoints need to be either disabled or a different class configured
    https://github.com/jupyter/notebook/blob/b8b66332e2023e83d2ee04f83d8814f567e01a4e/notebook/services/contents/filecheckpoints.py
    """

    fs = Instance(
        FS,
        allow_none=False,
    )

    @default('fs')
    def _fs_default(self):
        instance = FilesystemHandle.instance(self.fs_url)
        assert instance.fs_url == self.fs_url
        return instance.fs

    fs_url = Unicode(
        allow_none=False,
        help='FS URL',
        config=True,
    )

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
        try:
            d = self.fs.getdetails(path)
        except ResourceNotFound as e:
            raise HTTPError(404, 'Directory not found: {}: {}'.format(path, e))
        if not d.is_dir:
            raise HTTPError(404, 'Not a directory: {}'.format(path))

        model = _base_model(*fspath.split(path))
        model['type'] = 'directory'
        model['size'] = None
        model['format'] = None
        model['created'], model['last_modified'] = _created_modified(d)

        if content:
            model['content'] = []
            model['format'] = 'json'
            for item in self.fs.scandir(path, ['basic', 'details']):
                child_path = fspath.join(path, item.name)
                if item.is_dir:
                    model['content'].append(
                        self._get_directory(child_path, False, None))
                if item.is_file:
                    model['content'].append(
                        self._get_file(child_path, content, format))
        return model

    def _get_file(self, path, content, format):
        self.log.debug('_get_file(%s)', path)
        try:
            f = self.fs.getdetails(path)
        except ResourceNotFound as e:
            raise HTTPError(404, 'File not found: {}: {}'.format(path, e))
        if not f.is_file:
            raise HTTPError(404, 'Not a file: {}'.format(path))
        return self._file_model(path, f, content, format)

    def _file_model(self, path, f, content, format):
        model = _base_model(*fspath.split(path))
        model['type'] = 'file'
        model['created'], model['last_modified'] = _created_modified(f)
        model['size'] = f.size
        if content:
            model['content'], model['format'] = self._read_file(path, format)
            model['mimetype'] = mimetypes.guess_type(model['path'])[0]
        return model

    def _read_file(self, path, format):
        """
        :param format:
            - 'text': contents will be decoded as UTF-8.
            - 'base64': raw bytes contents will be encoded as base64.
            - None: try to decode as UTF-8, and fall back to base64
        """
        with self.fs.openbin(path, 'r') as fo:
            bcontent = fo.read()
        if format is None or format == 'text':
            try:
                return bcontent.decode('utf8'), 'text'
            except UnicodeError:
                if format == 'text':
                    raise HTTPError(
                        400,
                        "{} is not UTF-8 encoded".format(path),
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
        self.fs.makedir(path, recreate=True)
        model = self._get_directory(path, False, None)
        self.log.error(model)
        return model

    def _save_file(self, path, model):
        if 'content' not in model:
            raise HTTPError(400, 'No file content provided')
        if model.get('format') not in {'text', 'base64'}:
            raise HTTPError(
                400, "Format of file contents must be 'text' or 'base64'")

        try:
            if model['format'] == 'text':
                bcontent = model['content'].encode('utf8')
            else:
                bcontent = b64decode(model['content'])
        except Exception as e:
            raise HTTPError(
                400, 'Encoding error saving {}: {}'.format(model['path'], e))

        try:
            with self.fs.openbin(path, 'w') as fo:
                fo.write(bcontent)
        except ResourceNotFound as e:
            raise HTTPError(404, 'File not found: {} :e'.format(path, e))
        return self._get_file(path, False, None)

    def delete_file(self, path):
        self.log.debug('delete_file(%s)', path)
        path = _normalise_path(path)
        self.sf.remove(path)

    def rename_file(self, old_path, new_path):
        self.log.debug('rename_file(%s %s)', old_path, new_path)
        old_path = _normalise_path(old_path)
        new_path = _normalise_path(new_path)
        if self.fs.isdir(old_path):
            self.fs.movedir(old_path, new_path, create=True)
        else:
            self.fs.move(old_path, new_path)

    def file_exists(self, path):
        self.log.debug('file_exists(%s)', path)
        path = _normalise_path(path)
        return self.fs.isfile(path)

    def dir_exists(self, path):
        self.log.debug('dir_exists(%s)', path)
        path = _normalise_path(path)
        return self.fs.isdir(path)

    def is_hidden(self, path):
        self.log.debug('is_hidden(%s)', path)
        path = _normalise_path(path)
        # return fspath.basename(path).startswith('.')
        return False

    # def _send_keep_alive(self):
    #     self.log.debug('Sending keepalive')
    #     self.conn.c.sf.keepAlive(None)


class FsContentsManager(FsManagerMixin, ContentsManager):
    pass


class FsCheckpoints(
        FsManagerMixin, GenericCheckpointsMixin, Checkpoints):

    checkpoint_dir = Unicode(
        'ipynb_checkpoints',
        config=True,
        help="""The directory name in which to keep file checkpoints
        relative to the file's own directory""",
    )

    checkpoint_template = Unicode(
        '{basename}-checkpoint{id}{ext}',
        config=True,
        help="""The prefix to add to checkpoint files.
        `{basename}` is the filename with the extension, `{ext}` is the
        extension including `.`, `{id}` will be replaced by the checkpoint id.
        """,
    )

    def _checkpoint_path(self, checkpoint_id, path):
        """find the path to a checkpoint"""
        path = _normalise_path(path)
        parent, name = fspath.split(path)
        basename, ext = fspath.splitext(name)
        cp_path = fspath.join(
            parent, self.checkpoint_dir, self.checkpoint_template.format(
                basename=basename, id=checkpoint_id, ext=ext))
        return cp_path

    def _checkpoint_model(self, checkpoint_id, f):
        """construct the info dict for a given checkpoint"""
        info = {'id': str(checkpoint_id)}
        if isinstance(f, dict):
            info['last_modified'] = f['last_modified']
        else:
            info['last_modified'] = f.modified
        return info

    def _ensure_checkpoint_dir(self, cp_path):
        self.log.debug('_ensure_checkpoint_dir(%s)', cp_path)
        dirname, basename = fspath.split(cp_path)
        self.log.debug('%s %s %s', dirname, self.dir_exists(dirname), basename)
        if not self.dir_exists(dirname):
            self._save_directory(None, dirname)

    def create_file_checkpoint(self, content, format, path):
        self.log.debug('create_file_checkpoint(%s)', path)
        cp_path = self._checkpoint_path(0, path)
        self._ensure_checkpoint_dir(cp_path)
        model = _base_model(*fspath.split(cp_path))
        model['content'] = content
        model['format'] = format
        f = self._save_file(cp_path, model)
        return self._checkpoint_model(0, f)

    def create_notebook_checkpoint(self, nb, path):
        self.log.debug('create_notebook_checkpoint(%s)', path)
        cp_path = self._checkpoint_path(0, path)
        self._ensure_checkpoint_dir(cp_path)
        model = _base_model(*fspath.split(cp_path))
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
        if self.file_exists(cp_path):
            f = self._get_file(cp_path, False, None)
            return [self._checkpoint_model(0, f)]
        return []

    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        self.log.debug(
            'rename_checkpoint(%s %s %s)', checkpoint_id, old_path, new_path)
        cp_path_old = self._checkpoint_path(checkpoint_id, old_path)
        cp_path_new = self._checkpoint_path(checkpoint_id, new_path)
        self._ensure_checkpoint_dir(cp_path_new)
        self.rename_file(cp_path_old, cp_path_new)
