from domino.core import log
from domino.pages import Page as BasePage
from domino.pages import Title, Table, DeleteIconButton, TextWithComments
from domino.tables.postgres.good_param import GoodParam
from domino.tables.postgres.dictionary import Dictionary

class Page(BasePage):
    def __init__(self, application, request):
        super().__init__(application, request)
        self.postgres = None
        self.column_id = self.attribute('column_id')
        self._good_param = None

    @property
    def good_param(self):
        if self._good_param is None:
            self._good_param = self.postgres.query(GoodParam).get(self.column_id)
        return self._good_param

    def print_table(self):
        table = Table(self, 'table')
        query = self.postgres.query(Dictionary)\
            .filter(Dictionary.CLASS == 'good', Dictionary.TYPE == self.good_param.column_id)
        for item in query.order_by(Dictionary.name):
            row = table.row(item.id)
            self.print_row(row, item)
    
    def print_row(self, row, item):
        row.cell().text(item.code)
        row.cell().text(item.name)

    def __call__(self):
        Title(self, f'{self.good_param.name}, {self.good_param.column_id}')
        self.print_table()
