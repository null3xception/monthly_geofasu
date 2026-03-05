# -*- coding: utf-8 -*-
def classFactory(iface):
    from .plugin import CsvToGpkgPlugin
    return CsvToGpkgPlugin(iface)
