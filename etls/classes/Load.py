import pandas as pd

class Load:
    
    def __init__(self, data, dest_dir, filename):
        self.data=data
        self.dest=dest_dir
        self.filename=filename
    
    def export_as_csv(self):
        self.data.to_csv(io=self.dest_dir+self.filename, index=False)
        
    def export_as_excel(self):
        self.data.to_excel(excel_writer=self.dest_dir+self.filename, index=False)