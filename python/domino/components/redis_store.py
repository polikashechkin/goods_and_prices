import redis
import pickle
from domino.core import log

class RedisStore:
    def __init__(self, db, names):
        self.master_key = '|'.join(names)
        self.redis = redis.Redis(host='localhost', port=6379, db=db)

    def __repr__(self):
        return f'RedisStore({self.master_key})'

    def get_values(self, names, default=None):
        if names:
            KEY = '|'.join((self.master_key, '|'.join(names)))
            dump = self.redis.get(KEY)
        else:
            dump = self.redis.get(self.master_key)
        return pickle.loads(dump) if dump is not None else default

    def set_values(self, names, values):
        log.debug(f'{self}.set_values({names}, {values}):')
        dump = pickle.dumps(values)
        if names:
            NAMES = '|'.join(names)
            KEY = f'{self.master_key}|{NAMES}'
            self.redis.set(KEY, dump)
        else:
            self.redis.set(self.master_key, dump)

    def all(self):
        items = []
        pattern = f'{self.master_key}|*'
        keys = self.redis.keys(pattern)
        for key in keys:
            dump = self.redis.get(key) 
            items.append(pickle.loads(dump))
        return items

