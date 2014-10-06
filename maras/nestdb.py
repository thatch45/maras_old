'''
Create a stock database with a built in nesting key index
'''

# Import maras libs
import maras.database
import maras.tree_index


# We can likely build these out as mixins, making it easy to apply high level
# constructs to multiple unerlying database implimentations
class NestDB(maras.database.Database):
    '''
    Create a high level database which translates entry keys into a
    higherarcical dict like structure
    '''
    def __init__(self, path):
        maras.database.Database.__init__(self, path)
        self.__init_db()

    def __init_db(self):
        '''
        Init the db, open it if it already exists, otherwise create it
        '''
        try:
            self.open()
        except maras.database.DatabasePathException:
            self.create()

    def new_index(self, name):
        '''
        Add a new named index
        '''
        new = NestIndex(self.path, name)
        self.add_index(new)


class NestIndex(maras.tree_index.TreeBasedIndex):
    '''
    The index to manage higherarcical keys
    '''
    def __init__(self, *args, **kwargs):
        kwargs['node_capacity'] = kwargs.get('node_capacity', 1000)
        maras.tree_index.TreeBasedIndex.__init__(self, *args, **kwargs)

    def make_key_value(self, data):
        return data.get('_key', 'None')

    def make_key(self, key):
        return key
