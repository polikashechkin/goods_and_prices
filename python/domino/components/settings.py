import os, sys, json
from domino.core import DOMINO_ROOT

class Settings:
    def __init__(self, account_id, module_id):
        self.account_id = account_id
        self.module_id = module_id
        self.file_name = os.path.join(DOMINO_ROOT, 'accounts', self.account_id, 'data', self.module_id, 'settings.json')
        self.js = {}
        if os.path.isfile(self.file_name):
            with open(self.file_name) as f:
                self.js = json.load(f)
    
    def save(self):
        os.makedirs(os.path.dirname(self.file_name), exist_ok=True)
        with open(self.file_name, 'w') as f:
            json.dump(self.js, f)

    def get(self, key, default=None):
        return self.js.get(key, default)

    def __getitem__(self, key):
        return self.js[key]

    def __setitem__(self, key, value):
        self.js[key] = value
