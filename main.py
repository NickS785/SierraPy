from sierrapy.parser.async_scid_reader import AsyncFrontMonthScidReader
import pandas as pd
import numpy as np
from pathlib import Path
import asyncio as aio

data_path = "F:\\SierraChart\\Data"

mgr = AsyncFrontMonthScidReader(data_path)

data = aio.run(mgr.load_front_month_series("NG", volume_per_bar=100))

