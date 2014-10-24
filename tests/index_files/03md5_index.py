# sha1_index
# sha1Index

# inserted automatically
import os
import msgpack

import struct
import shutil

from hashlib import sha1

# custom db code start


# custom index code start
# source of classes in index.classes_code
# index code start
class sha1Index(HashIndex):

    def __init__(self, *args, **kwargs):
        kwargs['entry_line_format'] = '<32s32sIIcI'
        kwargs['hash_lim'] = 4 * 1024
        super(sha1Index, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        return sha1(data['name']).hexdigest(), {}

    def make_key(self, key):
        return sha1(key).hexdigest()
