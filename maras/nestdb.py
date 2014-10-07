# -*- coding: utf-8 -*-
'''
Create a stock database with a built in nesting key index
'''
# Import python libs
import os
import hashlib

# Import maras libs
import maras.database
import maras.hash_index


def key_comps(key, delim=':'):
    '''
    Return the key components
    '''
    if delim not in key:
        return '__root', key
    s_ind = key.rfind(delim)
    return key[:s_ind], key[s_ind + 1:]

# We can likely build these out as mixins, making it easy to apply high level
# constructs to multiple unerlying database implimentations
class NestDB(object):
    '''
    Create a high level database which translates entry keys into a
    higherarcical dict like structure
    '''
    def __init__(self, path, root_db='__root', index_name='pri'):
        self.path = path
        self.root_db = root_db
        self.index_name
        self.__open_dbs()

    def __open_dbs(self):
        '''
        list the databases and open them up
        '''
        self.dbs = {}
        if not os.path.isdir(self.path):
            os.makedirs(self.path)
            return
        for db in os.listdir(self.path):
            db_path = os.path.join(self.path, db)
            self.dbs[db] = maras.database.Database(db_path)
            self.dbs[db].open()
        if self.root_db not in self.dbs:
            self.create_db(self.root_db)

    def _init_db(self, name):
        '''
        Init the db, open it if it already exists, otherwise create it
        '''
        try:
            self.open()
        except maras.database.DatabasePathException:
            self.create()

    def create_db(self, db):
        '''
        Make a new db!
        '''
        db_path = os.path.join(self.path, db)
        n_db = maras.database.Database(db_path)
        try:
            n_db.open()
        except maras.database.DatabasePathException:
            n_db.create()
            n_index = Sha2Index(n_db.path, self.index_name)
            n_db.add_index(n_index)
        self.dbs[db] = n_db
        return n_db

    def _get_db(self, db):
        '''
        If the db is loaded, return it, otherwise make a new db!
        '''
        if db in self.dbs:
            return self.dbs[db]
        else:
            return self.create_db(db)

    def insert(self, key, data):
        '''
        Insert the given data into the named key
        '''
        db, key = key_comps(key)
        r_db = self._get_db(db)
        data['__key'] = key
        r_db.insert(data)

    def _db_from_key(self, key):
        db, s_key = key_comps(key)
        if db not in self.dbs:
            # TODO, match how the native db does this
            raise Exception('No matching key')
        return self.dbs[db], s_key

    def get(self, key, with_doc=False, with_storage=False):
        '''
        Get the named key
        '''
        db, s_key = self._db_from_key(key)
        return db.get(
                self.index_name,
                s_key,
                with_doc,
                with_storage)

    def get_many(
            self,
            key,
            limit=-1,
            offset=0,
            with_doc=False,
            with_storage=True,
            start=None,
            end=None,
            **kwargs):
        db, s_key = self._db_from_key(key)
        return db.get_many(
                self.index_name,
                s_key,
                limit,
                offset,
                with_doc,
                with_storage,
                start,
                end,
                **kwargs)

    def fsync(self):
        '''
        Force the kernel buffer to be written to disk
        '''
        for db in self.dbs:
            self.dbs[db].fsync()


class Sha2Index(maras.tree_index.TreeBasedIndex):
    '''
    The index to manage higherarcical keys
    '''
    custom_header = 'import hashlib'
    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '32s'
        maras.tree_index.TreeBasedIndex.__init__(self, *args, **kwargs)

    def make_key_value(self, data):
        if '__key' in data:
            return hashlib.sha256(data.pop('__key')).digest()
        return 'None'

    def make_key(self, key):
        return hashlib.sha256(key).digest()
