"""
Project Constants
"""
from pathlib import Path
import os

PROJECT_ROOT = str(Path(os.path.abspath(__file__)).parents[1])
WX_DATA = PROJECT_ROOT + '/wx_data'
YLD_DATA = PROJECT_ROOT + '/yld_data'

WX_SCHEMA = 'wx_schema'
YLD_SCHEMA = 'yld_schema'
WX_TABLE = 'wx_data'
YLD_TABLE = 'yld_data'
AVG_TABLE = 'avg_table'

WX_COLS = ['DATE','MAX_TEMP_1_10_DEG_C','MIN_TEMP_1_10_DEG_C','PRECIPITATION_1_10_MM']
YLD_COLS = ['YEAR','TOTAL_CORN_GRAIN_YIELD_MT']
