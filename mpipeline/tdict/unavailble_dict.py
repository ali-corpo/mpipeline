class UnavailableDict(dict):
    def __getitem__(self, key):
        raise ValueError("UnavailableDict in process mode")

    def get(self, key, default=None):
        raise ValueError("UnavailableDict in process mode")

    def __setitem__(self, key, value):
        raise ValueError("UnavailableDict in process mode")
