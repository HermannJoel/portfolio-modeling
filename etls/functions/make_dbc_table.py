# -*- coding: utf-8 -*-
"""
Created on Fri Jul  8 14:38:08 2022

@author: hermann.ngayap
"""
import dash_bootstrap_components as dbc
from dash import html

def make_dbc_table(df, style={}):
    """Make a dbc table from a pandas DataFrame df."""
    table_header = [html.Thead(html.Tr([html.Th(col) for col in df.columns]))]
    table_body = [
        html.Tbody(
            [
                html.Tr([html.Td(df.iloc[i][col]) for col in df.columns])
                for i in range(len(df))
            ]
        )
    ]
    return dbc.Table(
        table_header + table_body,
        bordered=False,
        responsive=True,
        hover=True,
        striped=True,
        style=style,
        class_name="table",
    )