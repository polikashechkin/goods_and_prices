from enum import Enum

class GoodType(Enum):
    ТОВАР = ('g', 'Товар')
    АЛКОГОЛЬ = ('a', 'Алкоголь')
    УСЛУГА = ('s', 'Услуга')
    ПОДПИСКА = ('sb', 'Подписка')
    ПОДПИСКА_ПП = ('sbd', 'Подписка п/п')
    
    __by_id__ =  {}

    def __str__(self):
        return self.name

    @property
    def id(self):
        return self.value[0]
    
    @property
    def description(self):
        return self.value[1]
    
    @staticmethod
    def get(id):
        return GoodType.__by_id__.get(id)

for good_type in GoodType:
    GoodType.__by_id__[good_type.id] = good_type
