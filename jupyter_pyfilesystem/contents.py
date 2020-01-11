from notebook.services.contents.manager import ContentsManager
from notebook.services.contents.checkpoints import (
    Checkpoints,
    GenericCheckpointsMixin,
)
from traitlets import (
    Bool,
    default,
    Instance,
    Int,
    TraitError,
    Unicode,
)
from traitlets.config.configurable import LoggingConfigurable
from tornado.ioloop import PeriodicCallback
from tornado.web import HTTPError
from base64 import (
    b64encode,
    b64decode,
)

import atexit
from datetime import datetime
from functools import wraps
import mimetypes
import nbformat
import re

from fs import open_fs
from fs.base import FS
from fs.errors import (
    DestinationExists,
    IllegalBackReference,
    ResourceNotFound,
    ResourceReadOnly,
)
import fs.path as fspath


# https://github.com/quantopian/pgcontents/blob/5fad3f6840d82e6acde97f8e3abe835765fa824b/pgcontents/api_utils.py#L25
def _base_model(dirname, name):
    return {
        'name': name,
        'path': (dirname + '/' + name).strip('/'),
        'writable': True,
        'last_modified': None,
        'created': None,
        'content': None,
        'format': None,
        'mimetype': None,
        'size': 0,
        'type': None,
    }


DEFAULT_CREATED_DATE = datetime.utcfromtimestamp(0)


def _created_modified(details):
    created = details.created or details.modified or DEFAULT_CREATED_DATE
    modified = details.modified or details.created or DEFAULT_CREATED_DATE
    return created, modified


def wrap_fs_errors(type=None):
    """
    Decorator to convert fs.errors into HTTPErrors.
    Wrapped method must have arguments `self` and `path`
    as the first two arguments
    """
    def wrap_fs_errors_with_type(func):
        @wraps(func)
        def check(self, path, *args, **kwargs):
            t = (type + ' ') if type else ''
            try:
                return func(self, path, *args, **kwargs)
            except (ResourceNotFound, IllegalBackReference) as e:
                self.log.debug('Caught exception: %s', e)
                raise HTTPError(404, '{}"{}" not found: {}'.format(t, path, e))
            except DestinationExists as e:
                self.log.debug('Caught exception: {}'.format(e))
                raise HTTPError(409, '{}"{}" conflicts: {}'.format(t, path, e))
            except ResourceReadOnly as e:
                self.log.debug('Caught exception: %s', e)
                raise HTTPError(409, '{}"{}" is read-only: {}'.format(
                    t, path, e))
        return check
    return wrap_fs_errors_with_type


class FilesystemHandle(LoggingConfigurable):

    def __init__(self, fs_url, *, create, writeable, closeonexit, keepalive):
        m = re.match(r'^([a-z][a-z0-9+\-.]*)://', fs_url)
        if not m:
            raise TraitError('Invalid fs_url: {}'.format(fs_url))
        self.fs_url = fs_url
        self.fsname = m.group()
        self.log.debug('Opening filesystem %s', fs_url)
        self.fs = open_fs(self.fs_url, writeable=writeable, create=create)
        self.log.info('Opened filesystem %s', self.fsname)
        self.keepalive_cb = None
        if keepalive:
            self.enable_keepalive(keepalive)
        if closeonexit:
            self.register_atexit()

    def close(self):
        self.log.debug('Closing filesystem %s', self.fs_url)
        self.enable_keepalive(0)
        self.fs.close()
        self.log.info('Closed filesystem %s', self.fsname)

    def keepalive(self):
        d = self.fs.getdetails('/')
        self.log.debug('keepalive: %s', d)

    def enable_keepalive(self, interval):
        self.log.debug('enable_keepalive(%s)', interval)
        if self.keepalive_cb:
            self.keepalive_cb.stop()
            self.keepalive_cb = None
        if interval > 0:
            self.keepalive_cb = PeriodicCallback(
                self.keepalive, interval * 1000)
            self.keepalive_cb.start()

    def register_atexit(self):
        atexit.register(self.close)


class FsContentsManager(ContentsManager):
    """
    https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html
    https://github.com/jupyter/notebook/blob/6.0.1/notebook/services/contents/manager.py
    https://github.com/jupyter/notebook/blob/6.0.1/notebook/services/contents/filemanager.py
    https://github.com/quantopian/pgcontents/blob/master/pgcontents/pgmanager.py
    """

    fs = Instance(FS)

    @default('fs')
    def _fs_default(self):
        instance = FilesystemHandle(
            self.fs_url, create=self.create, writeable=self.writeable,
            closeonexit=self.closeonexit, keepalive=self.keepalive)
        assert instance.fs_url == self.fs_url
        return instance.fs

    fs_url = Unicode(
        allow_none=False,
        help='FS URL',
        config=True,
    )

    create = Bool(
        default_value=True,
        help='Create filesystem if necessary',
        config=True,
    )

    writeable = Bool(
        default_value=True,
        help='Open filesystem for reading and writing',
        config=True,
    )

    closeonexit = Bool(
        default_value=True,
        help='Register an atexit handler to close the filesystem',
        config=True,
    )

    keepalive = Int(
        default_value=0,
        help='''Send keepalive at this interval (seconds), this might be needed
        for remote filesystems''',
        config=True,
    )

    @default('checkpoints_class')
    def _checkpoints_class_default(self):
        return FsCheckpoints

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
        self.log.debug('get(%s %s)', path, type)
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
        return fn(path=path, content=content, format=format, type=type)

    @wrap_fs_errors('notebook')
    def _get_notebook(self, path, content, format, *, type=None, trust=True):
        self.log.debug('_get_notebook(%s)', path)
        path = self.fs.validatepath(path)
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

    @wrap_fs_errors('directory')
    def _get_directory(self, path, content, format, *, type=None):
        self.log.debug('_get_directory(%s)', path)
        path = self.fs.validatepath(path)
        d = self.fs.getdetails(path)
        if not d.is_dir:
            raise HTTPError(404, '"%s" not a directory', path)

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
                        self._get_file(child_path, False, format))
        return model

    @wrap_fs_errors('file')
    def _get_file(self, path, content, format, *, type=None):
        self.log.debug('_get_file(%s)', path)
        path = self.fs.validatepath(path)
        f = self.fs.getdetails(path)
        if not f.is_file:
            raise HTTPError(404, 'Not a file: {}'.format(path))
        model = self._file_model(path, f, content, format)
        if type:
            model['type'] = type
        return model

    def _file_model(self, path, f, content, format):
        model = _base_model(*fspath.split(path))
        model['type'] = self.guess_type(path)
        model['created'], model['last_modified'] = _created_modified(f)
        model['size'] = f.size
        if content:
            model['content'], model['format'] = self._read_file(path, format)
            model['mimetype'] = mimetypes.guess_type(model['path'])[0]
        return model

    @wrap_fs_errors('file')
    def _read_file(self, path, format):
        self.log.debug('_read_file(%s)', path)
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
        self.log.debug('save(%s %s)', path, model['type'])
        self.run_pre_save_hook(model=model, path=path)
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
        return fn(path, model)

    @wrap_fs_errors('notebook')
    def _save_notebook(self, path, model, sign=True):
        self.log.debug('_save_notebook(%s)', path)
        nb = nbformat.from_dict(model['content'])
        if sign:
            self.check_and_sign(nb, path)
        model['content'] = nbformat.writes(nb)
        model['format'] = 'text'
        return self._save_file(path, model)

    @wrap_fs_errors('directory')
    def _save_directory(self, path, model):
        self.log.debug('_save_directory(%s)', path)
        self.fs.makedir(path, recreate=True)
        model = self._get_directory(path, False, None)
        return model

    @wrap_fs_errors('file')
    def _save_file(self, path, model):
        self.log.debug('_save_file(%s)', path)
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

        with self.fs.openbin(path, 'w') as fo:
            fo.write(bcontent)
        return self._get_file(path, False, None)

    @wrap_fs_errors('file')
    def delete_file(self, path):
        # TODO: This is also used to delete directories
        self.log.debug('delete_file(%s)', path)
        path = self.fs.validatepath(path)
        if self.fs.isfile(path):
            self.fs.remove(path)
        elif self.fs.isdir(path):
            self.fs.removedir(path)
        else:
            raise ResourceNotFound(path)

    @wrap_fs_errors('file')
    def rename_file(self, old_path, new_path):
        self.log.debug('rename_file(%s %s)', old_path, new_path)
        old_path = self.fs.validatepath(old_path)
        new_path = self.fs.validatepath(new_path)
        if old_path == '/':
            raise HTTPError(409, 'Unable to rename root /')
        if self.fs.isdir(old_path):
            if self.fs.exists(new_path):
                raise DestinationExists(new_path)
            self.fs.movedir(old_path, new_path, create=True)
        else:
            self.fs.move(old_path, new_path)

    @wrap_fs_errors(None)
    def file_exists(self, path):
        self.log.debug('file_exists(%s)', path)
        path = self.fs.validatepath(path)
        return self.fs.isfile(path)

    @wrap_fs_errors(None)
    def dir_exists(self, path):
        self.log.debug('dir_exists(%s)', path)
        path = self.fs.validatepath(path)
        return self.fs.isdir(path)

    @wrap_fs_errors(None)
    def is_hidden(self, path):
        self.log.debug('is_hidden(%s)', path)
        path = self.fs.validatepath(path)
        return fspath.basename(path).startswith('.')

    # def _send_keep_alive(self):
    #     self.log.debug('Sending keepalive')
    #     self.conn.c.sf.keepAlive(None)


class FsCheckpoints(GenericCheckpointsMixin, Checkpoints):

    checkpoint_dir = Unicode(
        '.ipynb_checkpoints',
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
        path = self.parent.fs.validatepath(path)
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
        dirname, basename = fspath.split(cp_path)
        if not self.parent.dir_exists(dirname):
            self.parent._save_directory(dirname, None)

    def create_file_checkpoint(self, content, format, path):
        self.log.debug('create_file_checkpoint(%s)', path)
        cp_path = self._checkpoint_path(0, path)
        self._ensure_checkpoint_dir(cp_path)
        model = _base_model(*fspath.split(cp_path))
        model['content'] = content
        model['format'] = format
        f = self.parent._save_file(cp_path, model)
        return self._checkpoint_model(0, f)

    def create_notebook_checkpoint(self, nb, path):
        self.log.debug('create_notebook_checkpoint(%s)', path)
        cp_path = self._checkpoint_path(0, path)
        self._ensure_checkpoint_dir(cp_path)
        model = _base_model(*fspath.split(cp_path))
        model['content'] = nb
        f = self.parent._save_notebook(cp_path, model, False)
        return self._checkpoint_model(0, f)

    def get_file_checkpoint(self, checkpoint_id, path):
        # -> {'type': 'file', 'content': <str>, 'format': {'text', 'base64'}}
        self.log.debug('get_file_checkpoint(%s %s)', checkpoint_id, path)
        cp_path = self._checkpoint_path(checkpoint_id, path)
        return self.parent._get_file(cp_path, True, None)

    def get_notebook_checkpoint(self, checkpoint_id, path):
        # -> {'type': 'notebook', 'content': <output of nbformat.read>}
        self.log.debug('get_notebook_checkpoint(%s %s)', checkpoint_id, path)
        cp_path = self._checkpoint_path(checkpoint_id, path)
        return self.parent._get_notebook(cp_path, True, 'text', trust=False)

    def delete_checkpoint(self, checkpoint_id, path):
        self.log.debug('delete_checkpoint(%s %s)', checkpoint_id, path)
        cp_path = self._checkpoint_path(checkpoint_id, path)
        self.parent.delete_file(cp_path)

    def list_checkpoints(self, path):
        self.log.debug('list_checkpoints(%s)', path)
        cp_path = self._checkpoint_path(0, path)
        if self.parent.file_exists(cp_path):
            f = self.parent._get_file(cp_path, False, None)
            return [self._checkpoint_model(0, f)]
        return []

    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        self.log.debug(
            'rename_checkpoint(%s %s %s)', checkpoint_id, old_path, new_path)
        cp_path_old = self._checkpoint_path(checkpoint_id, old_path)
        cp_path_new = self._checkpoint_path(checkpoint_id, new_path)
        self._ensure_checkpoint_dir(cp_path_new)
        self.parent.rename_file(cp_path_old, cp_path_new)
