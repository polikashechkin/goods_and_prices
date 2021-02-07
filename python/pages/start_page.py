from domino.core import log
from domino.pages.start_page import Page as BasePage
from . import Title
 
class Page(BasePage):
    def __init__(self, application, request):
        super().__init__(application, request)

    def create_menu(self, menu):
        menu.item('Товары', 'pages/goods')
        menu.item('Категории', 'pages/groups')
        menu.item('Единицы измерения', 'pages/units')
        menu.item('Страны', 'pages/countries')
        menu.item('Дополнительные параметры товара', 'pages/good_params')
        menu.item('Процедуры', 'domino/pages/procs')
        
        
