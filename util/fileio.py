import os
import datetime
import pandas as pd
import util.basic as basic

tool=basic.basic()

class file():
    def __init__(self,date=None,path=None):
        self.date=date if date else datetime.datetime.today().date()
        self.path=path if path else 'D:\\workgit\\stock\\util\\stockdata\\'