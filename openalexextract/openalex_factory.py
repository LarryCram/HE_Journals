from abc import ABCMeta, abstractmethod

import re
import os
import pandas as pd
import requests
import diskcache as dc
from collections import defaultdict


class IOpenalexFactory(metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def authorship_factory(cls):
        ...


class OpenalexFactory(IOpenalexFactory):

    def __init__(self):
        ...

    def authorship_factory(self):
        ...