import pandas as pd


class Transform:
    
    def __init__(self, data, **args):
        self.data=data
        self.a=args[0]
        self.b=args[1]
        self.c=args[2]
        
        