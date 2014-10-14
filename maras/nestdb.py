# -*- coding: utf-8 -*-
'''
Create a stock database with a built in nesting key index
'''
# Import python libs
import os
import hashlib

# Import maras libs
import maras
import maras.database
import maras.database_safe_shared
import maras.database_super_thread_safe
import maras.database_thread_safe
import maras.hash_index
import maras.tree_index


def key_comps(key, delim='/'):
    '''
    Return the key components
    '''
    if delim not in key:
        return '__root', key
    s_ind = key.rfind(delim)
    return key[:s_ind], key[s_ind + 1:]


def get_plain_db(path):
    '''
    Return a non thread safe simple database
    '''
    return maras.dtabase.Database(path)

class DBGen(object):
    def get_plain_db(self, path):
        '''
        return a plain database
        '''
        return maras.database.Database(path)

    def get_safe_shared_db(self, path):
        '''
        return a safe shared db
        '''
        return maras.safe_shared_database.Database(path)

    def get_thread_db(self, path):
        '''
        return a thread safe db
        '''
        return maras.database.thread_safe.Database(path)

    def get_super_thread_db(self, path):
        '''
        Return a super thread safe db
        '''
        return maras.database_super_thread_safe.Database(path)



class HighDB(object):
    '''
    Create a higherarchical key database. This database wraps the underlying
    database object of choice with high level convenience functions and uses
    supersymetry to maintain hiden key reference lists, this creates a logical
    reference lookup based on key "location".
    '''
    def __init__(self, path, delim='/', index_name='pri', db_type='plain'):
        self.root_path = path
        self.db_path = os.path.join(path, 'db')
        self.sym_path = os.path.join(path, 'sym')
        if not os.path.exists(self.root_path):
            os.makedirs(self.root_path)
        self.delim = delim
        self.dbgen = DBGen()
        self.index_name = index_name
        self.db = getattr(self.dbgen, 'get_{0}_db'.format(db_type))(self.root_path)
        self.sym = getattr(self.dbgen, 'get_{0}_db'.format(db_type))(self.sym_path)
        self.__open_db('db')
        self.__open_db('sym')

    def __open_db(self, form):
        '''
        If the db does not exist, create it, otherwise open it
        '''
        db_obj = getattr(self, form)
        if db_obj.exists():
            if db_obj.opened:
                return
            db_obj.open()
        else:
            db_obj.create()
        if self.index_name not in db_obj.indexes_names:
            if form == 'db':
                ind = Sha2TreeIndex(self.db.path, self.index_name)
            else:
                ind = Sha2HashIndex(self.db.path, self.index_name)
            db_obj.add_index(ind)

    def close(self):
        '''
        Close the database
        '''
        if self.db.opened:
            self.db.close()
        if self.sym.opened:
            self.sym.close()
        return True

    def destroy(self):
        '''
        Destroy the database!
        This ain't no Xen destroy, this deletes all the data!!
        '''
        if self.db.exists():
            self.db.destroy()
        if self.sym.exists():
            self.sym.destroy()
        return True

    def _update_sym(self, key):
        '''
        Write the key relational data into the sym db
        '''
        root, base = key_comps(key, self.delim)
        try:
            current = self.sym.get(self.index_name, root, True)
        except Exception:
            current = {'files': []}
        files = current.get('files')
        files.append(base)
        data = {'__key': root,
                'files': files}
        if '_rev' in current:
            data['_rev'] = current['_rev']
        if '_id' in current:
            data['_id'] = current['_rev']
        if base not in current.get('files'):
            if '_rev' in data and '_id' in data:
                self.sym.update(data)
            else:
                self.sym.insert(data)

    def insert(self, key, data):
        '''
        Insert the given data at the given key
        '''
        self._update_sym(key)
        data['__key'] = key
        return self.db.insert(data)

    def get(self, key, with_doc=False, with_storage=True):
        '''
        Get a single chunk of data from the db from the given key
        '''
        return self.db.get(self.index_name, key, with_doc, with_storage)

    def get_many(
            self,
            key=None,
            limit=-1,
            offset=0,
            with_doc=False,
            with_storage=True,
            start=None,
            end=None,
            **kwargs):
        '''
        get lots of data - this needs better docstring!!
        '''
        return self.db.get_many(
                self.index_name,
                key,
                limit,
                offset,
                with_doc,
                with_storage,
                start,
                end,
                **kwargs)


class Sha2TreeIndex(maras.tree_index.TreeBasedIndex):
    '''
    The index to manage higherarcical keys
    '''
    custom_header = 'import hashlib'
    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '32s'
        maras.tree_index.TreeBasedIndex.__init__(self, *args, **kwargs)

    def make_key_value(self, data):
        if '__key' in data:
            key = hashlib.sha256(data.pop('__key')).digest()
            return key, data
        return 'None'

    def make_key(self, key):
        return hashlib.sha256(key).digest()


class Sha2HashIndex(maras.hash_index.HashIndex):
    '''
    The index to manage higherarcical keys
    '''
    custom_header = 'import hashlib'
    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '32s'
        maras.hash_index.HashIndex.__init__(self, *args, **kwargs)

    def make_key_value(self, data):
        if '__key' in data:
            key = hashlib.sha256(data.pop('__key')).digest()
            return key, data
        return 'None'

    def make_key(self, key):
        return hashlib.sha256(key).digest()
