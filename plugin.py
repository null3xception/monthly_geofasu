# -*- coding: utf-8 -*-
import os
from qgis.PyQt.QtWidgets import QAction
from qgis.PyQt.QtGui import QIcon


class CsvToGpkgPlugin:

    def __init__(self, iface):
        self.iface = iface
        self.plugin_dir = os.path.dirname(__file__)
        self.action = None
        self.toolbar = None

    def initGui(self):
        icon = QIcon(os.path.join(self.plugin_dir, "icon.png"))
        self.action = QAction(icon, "Monthly GeoFASU", self.iface.mainWindow())
        self.action.setShortcut("Ctrl+Shift+G")
        self.action.setIconVisibleInMenu(True)
        self.action.triggered.connect(self.run)

        self.iface.addPluginToMenu("&Monthly GeoFASU", self.action)

        self.toolbar = self.iface.mainWindow().addToolBar("Monthly GeoFASU")
        self.toolbar.setObjectName("MonthlyGeoFASUToolbar")
        self.toolbar.addAction(self.action)

    def unload(self):
        self.iface.removePluginMenu("&Monthly GeoFASU", self.action)
        if self.toolbar:
            self.toolbar.deleteLater()
            self.toolbar = None

    def run(self):
        from .dialog import CsvToGpkgDialog
        dlg = CsvToGpkgDialog(self.iface)
        dlg.exec_()