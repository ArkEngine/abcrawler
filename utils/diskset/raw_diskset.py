from pydiskset import *

class diskset(object):
    def __init__(self, data_dir, clear_flag):
        self._base = pydiskset(data_dir, clear_flag)
    def select_key(self, sign1, sign2):
        return select_key(self._base, sign1, sign2)
    def insert_key(self, sign1, sign2):
        return insert_key(self._base, sign1, sign2)
    def clear(self,):
        return clear(self._base)
