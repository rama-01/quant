# draw.py
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QTableView, QVBoxLayout, QWidget
from PyQt5.QtCore import Qt
import pandas as pd
from PyQt5.QtGui import QStandardItemModel, QStandardItem


def display_dataframe_in_window(df):
    class DataFrameViewer(QMainWindow):
        def __init__(self, data):
            super().__init__()
            self.setWindowTitle("上证所股票市场总览数据")
            self.resize(800, 600)

            # 创建数据模型
            model = QStandardItemModel()
            model.setHorizontalHeaderLabels(data.columns.tolist())

            # 填充数据
            for _, row in data.iterrows():
                items = [QStandardItem(str(x)) for x in row]
                model.appendRow(items)

            # 创建表格视图
            self.table = QTableView()
            self.table.setModel(model)
            self.table.setEditTriggers(QTableView.NoEditTriggers)
            self.table.horizontalHeader().setStretchLastSection(True)

            # 布局
            layout = QVBoxLayout()
            layout.addWidget(self.table)

            container = QWidget()
            container.setLayout(layout)
            self.setCentralWidget(container)

    app = QApplication(sys.argv)
    window = DataFrameViewer(df)
    window.show()
    sys.exit(app.exec_())
