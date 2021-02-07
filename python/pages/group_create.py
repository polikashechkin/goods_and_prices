from domino.core import log
from domino.pages import Page as BasePage
from domino.pages import Title, FlatTable, Input, DeleteIconButton, TextWithComments, Toolbar, Select, Button, Text, IconButton
from domino.pages import Chip
from domino.tables.postgres.good import Good
from domino.tables.postgres.good import GoodParam
from domino.tables.postgres.dictionary import Dictionary
from domino.tables.postgres.user import User, UserTable
from domino.enums.unit import Unit 

class Page(BasePage):
    def __init__(self, application, request):
        super().__init__(application, request)
        self.postgres = None
        self.good_type = Good.Type.get(self.attribute('good_type_id'))

    def oncreate(self):
        code = self.get('code')
        name = self.get('name')
        if not code:
            code = Good.nextval(self.postgres)
        if not name:
            self.error('Наименование должны быть задано')
            return
        good = self.postgres.query(Good).filter(Good.code == code).first()
        if good:
            self.error(f'Товар с кодом "{code}" уже существует')
            return
        #-------------------------------------------
        unit_param  = self.postgres.query(GoodParam).get('unit_id')
        unit_value = self.get(unit_param.id)
        unit_value_name = unit_param.get_name(self.postgres, unit_value)

        group_param = self.postgres.query(GoodParam).get('local_group')
        group_value = self.get(group_param.id)
        group_value_name = group_value.get_name(group_value)

        description = {
            unit_param.name : unit_value_name,
            group_param.name : group_value_name
        }
        #-------------------------------------------
        try:
            good = Good(
                t = self.good_type, code = code, name = name, 
                unit_id = unit_value, local_group = group_value,
                description = description
                )
            self.postgres.add(good)
            self.postgres.commit()
            self.message(f'Товар успешно создан : #{good.id}')
        except Exception as ex:
            self.error(ex)
 
    def row(self, table, id, name, options = None):
        row = table.row(id)
        row.cell().text(name)
        if options:
            Select(row.cell(), name=id).options(options)
        else:
            Input(row.cell(), name=id)

    def row_param(self, table, id):
        param = self.postgres.query(GoodParam).get(id)
        self.row(table, id, param.name, options=param.items(self.postgres))

    def __call__(self):
        Title(self, f'{self.good_type.description}')
        table = FlatTable(self, 'table').css('borderles')
        
        self.row(table, 'code', 'Код'.upper())
        self.row(table, 'name', 'Наименование'.upper())
        self.row_param(table, 'local_group')
        self.row_param(table, 'unit_id')

        toolbar = Toolbar(self, 'toolbar') 
        Button(toolbar.item(ml='auto'), f'Создать').style('background-color:green; color:white')\
            .onclick('.oncreate', forms=[table])

