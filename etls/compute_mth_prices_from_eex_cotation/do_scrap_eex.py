# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 17:13:42 2022

@author: hermann.ngayap
"""


#==============This script is to scrap monthly, quarterly and cal cotations from
#              from eex website. It should be run every day after 9pm 
#              to scrap the most recent cotation data                         

from scrap_market_prices_eex_2022 import load_cookie, change
from scrap_market_prices_eex_2022 import scrap_eex  #This module should remain in the same folder as the one assigned to the variable ncwd
                                                    #the excel file "Futures_products_2022" as well


def do_scrap_eex(i):    
    print("futures scrapping starts")
    scrap_eex(i)
    print("futures scrapping is done")

#Change the argumet i accordingly:i.e: 0 to scrap today's prices, 1= to scrap yerterday's prices, 2=to scrap the day b4 yesterday prices....so on so forth.
#Keep in mind that there aren't cotations on week-end. Consequently, on Monday to scrap last Friday data, (i) should be equal to 3.
i=2
do_scrap_eex(i)    
