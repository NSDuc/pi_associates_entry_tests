import logging
from datetime import date
from typing import List


class KisvnProcessorParams:
    def __init__(self,
                 symbols=None,
                 process_dates=None,
                 raw_datadir=None,
                 processed_datadir=None):
        self._symbols: List[str] = symbols
        self._process_dates: List[date] = process_dates
        self._raw_datadir = raw_datadir
        self._processed_datadir = processed_datadir

    def symbols(self): return self._symbols
    def process_dates(self): return self._process_dates
    def raw_datadir(self): return self._raw_datadir
    def processed_datadir(self): return self._processed_datadir


class KisvnProcessor:
    def __init__(self, params: KisvnProcessorParams):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.params = params

    def execute(self):
        raise NotImplementedError

