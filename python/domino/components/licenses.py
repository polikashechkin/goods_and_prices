import os, sys, json, datetime, arrow, redis, pickle
from enum import Enum
from domino.core import DOMINO_ROOT, log, RawToHex, HexToRaw

from domino.components.redis_store import RedisStore

class LicenseObjectType(Enum):
    Подразделение   = ('LOCATION',      "48431409[63111174]", HexToRaw('02E3013103C30001'), "Ключ привязки к локации (подразделению)")
    Смартфон        = ('MOBILE',        '48431409[63111172]', HexToRaw('02E3013103C30004'), "Ключ привязки к мобильному устройству (IMEI)")
    Компьютер       = ('COMPUTER',      '48431409[63111169]', HexToRaw('02E3013103C30001'), "Ключ привязки к компьютеру (v.1) (CfgKEY-1)")
    ФН              = ('FN'      ,      '48431409[63111175]', HexToRaw('02E3013103C30007'), "Ключ привязки к регистрационному номеру ФР")      
    ФСРАР           = ('FSRAR',         '63111198[63111169]', HexToRaw('03C3001E03C30001'), "ФСРАР ID")
    HASP            = ('HASP', None, None, "HASP")
    MEMOHASP        = ('MEMOHASP', None , None, "MEMOHASP")

    def __str__(self):
        return self.name
       
    @property
    def id(self):
        return self.value[0]

    @property
    def project_uid(self):
        return self.value[1]

    @property
    def uid(self):
        return self.value[2]

    @property
    def UID(self):
        return RawToHex(self.value[2])
    @property
    def description(self):
        return self.value[3]
     
    @staticmethod
    def get(id):
        if id is None: return None
        if isinstance(id, str):
            i = LicenseObjectType.__by_id__.get(id)
            if not i:
                i = LicenseObjectType.__by_project_uid__.get(id)
            return i
        else:
            return LicenseObjectType.__by_uid__.get(id)

    @staticmethod
    def items():
        items = []
        for t in LicenseObjectType:
            items.append((t.id, t))
        return items

LicenseObjectType.__by_id__ = {}
LicenseObjectType.__by_project_uid__ = {}
LicenseObjectType.__by_uid__ = {}
for license_object_type in LicenseObjectType:
    LicenseObjectType.__by_id__[license_object_type.id] = license_object_type
    LicenseObjectType.__by_project_uid__[license_object_type.project_uid] = license_object_type
    LicenseObjectType.__by_uid__[license_object_type.uid] = license_object_type

#def make_key(account_id, product_id, object_type, object_id):
#    return f'{account_id}:{product_id}:{object_type}:{object_id}'

class Licenses:
    ObjectType = LicenseObjectType
    class Store: 
        def __init__(self, account_id):
            self.store = RedisStore(1, ('license', account_id))
        def get_license(self, product_id, object_type, object_id):
            return Licenses.License(self.store.get_values((product_id, object_type, object_id), {}))
        def set_license(self, license):
            self.store.set_values((license.product_id, license.object_type, license.object_id), license.js)
        def getall(self):
            licenses = []
            for js in self.store.all():
                licenses.append(Licenses.License(js))
            return licenses

    class Sft:
        def __init__(self, account_id):
            self.file = os.path.join(DOMINO_ROOT, 'public', 'accounts', account_id, 'sft', f'{account_id}.json')
            self.js = {'account_id':account_id}
            self.licenses = []
            self.js['licenses'] = self.licenses
        @property
        def account_id(self):
            return self.js['account_id']
        def add(self, license):
            self.licenses.append(license.js)
        def getall(self):
            licenses = []
            for js in self.licenses:
                licenses.append(Licenses.License(js))
            return licenses
        def save(self):
            self.js['ctime'] = f'{datetime.datetime.now()}'
            os.makedirs(os.path.dirname(self.file), exist_ok=True)
            with open(self.file, 'w') as f:
                json.dump(self.js, f)
            self.store()
        def load(self):
            with open(self.file) as f:
                self.js = json.load(f)
            self.licenses = self.js['licenses']
        def store(self):
            store = Licenses.Store(self.account_id)
            for license in self.getall():
                store.set_license(license)
    
    class License:
        def __init__(self, js = None, product_id = None, object_type=None, object_id=None, exp_date=None):
            self.js = js if js is not None else {}
            self.product_id     = product_id
            self.object_type    = object_type
            self.object_id      = object_id
            self.exp_date       = exp_date
            
            if self.product_id is None or self.object_type is None or self.object_id is None:
                raise Exception(f'Недопустимые параметры лицензии ({self.product_id}, {self.object_type}, {self.object_id})')

        @property
        def product_id(self):
            return self.js.get('product_id')
        @product_id.setter
        def product_id(self, value):
            if value is not None:
                self.js['product_id'] = value
        
        @property
        def object_type(self):
            return self.js.get('object_type')
        @object_type.setter
        def object_type(self, value):
            if value is not None:
                self.js['object_type'] = value

        @property
        def object_id(self):
            return self.js.get('object_id')
        @object_id.setter
        def object_id(self, value):
            if value is not None:
                self.js['object_id'] = value
        @property
        def exp_date(self):
            return self.js.get('exp_date')
        @exp_date.setter
        def exp_date(self, value):
            if value:
                self.js['exp_date'] = f'{value}'


