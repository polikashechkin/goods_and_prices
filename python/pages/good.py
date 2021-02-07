from domino.core import log
from domino.pages import Page as BasePage
from domino.pages import Title, Table, Input, DeleteIconButton, TextWithComments, Toolbar, Select, Button, Text, IconButton
from domino.pages import FlatTable
#from domino.pages import Chip
from domino.tables.postgres.good import Good
from domino.tables.postgres.good import GoodParam
#from domino.tables.postgres.dictionary import Dictionary
#from domino.tables.postgres.user import User, UserTable

class Page(BasePage):
    def __init__(self, application, request):
        super().__init__(application, request)
        self.postgres = None
        self._good = None
        self.good_id = self.attribute('good_id')
    
    @property
    def good(self):
        if self._good is None:
            self._good = self.postgres.query(Good).get(self.good_id)
        return self._good

    def onchange_name(self):
        self.good.name = self.get('name')
        self.message(f'Наименование : "{self.good.name}"')

    def onchange(self):
        param_id = self.get('param_id')
        param = self.postgres.query(GoodParam).get(param_id)
        #column = Good.column(param.id)
        value = self.get(param.id)
        if value:
            value_name = param.get_name(self.postgres, value) if value else None
            update = {param.id:value}
            self.postgres.query(Good).filter(Good.id == self.good_id)\
                .update(update)
            self.good.update_description({param.name:value_name})
        else:
            value_name = None
            update = {param.id:None}
            self.postgres.query(Good).filter(Good.id == self.good_id)\
                .update(update)
            if self.good.description:
                if param.name in self.good.description:
                    del self.good.description[param.name]
        self.postgres.commit()
        self.message(f'{param.name} : "{value_name}" {update}')

    def __call__(self):
        Title(self, f'{self.good.t}, {self.good.code}')

        table = FlatTable(self, 'table').css('borderles')
        row = table.row()
        row.cell().text('Наименование'.upper())
        Input(row.cell(), name='name', value=self.good.name)\
            .onkeypress(13, '.onchange_name', forms=[table])
        #------------------------------------------
        for param in GoodParam.availables(self.postgres):
            row = table.row()
            row.cell().text(param.name.upper())
            #if param.id == 'unit_id':
            #    value = self.good.unit.name if self.good.unit else ''
            #elif param.id == 'country_id':
            #    value = self.good.country.name if self.good.country else ''
            #else:
            value = self.good.get_value(param.id)
            Select(row.cell(), name=param.id, value=value).options ([['','']] + param.items(self.postgres))\
                .onchange('.onchange', {'param_id':param.id}, forms = [table])
