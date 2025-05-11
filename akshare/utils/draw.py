import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QTableView, QVBoxLayout, QWidget
from PyQt5.QtCore import Qt
import pandas as pd
from PyQt5.QtGui import QStandardItemModel, QStandardItem


class DataFrameViewer(QMainWindow):
    def __init__(self, data):
        super().__init__()
        self.setWindowTitle("股票市场总览数据")
        self.resize(800, 600)

        # 启用表格排序功能（新增）
        self.table = QTableView()
        self.table.setSortingEnabled(True)
        # 设置选择模式为多选
        self.table.setSelectionMode(QTableView.ContiguousSelection)
        # 创建数据模型
        self.table.horizontalHeader().setDefaultAlignment(Qt.AlignLeft)
        numeric_cols = data.select_dtypes(include="number").columns.tolist()
        model = QStandardItemModel()
        model.setHorizontalHeaderLabels(data.columns.tolist())

        # 填充数据
        for _, row in data.iterrows():
            items = []
            for col_idx, value in enumerate(row):
                item = QStandardItem()
                # 如果是数字列则保留数值类型，否则转为字符串
                if data.columns[col_idx] in numeric_cols:
                    item.setData(float(value), Qt.DisplayRole)
                else:
                    item.setText(str(value))
                items.append(item)
            model.appendRow(items)

        # 创建表格视图
        self.table.setModel(model)
        self.table.setEditTriggers(QTableView.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)

        # 布局
        layout = QVBoxLayout()
        layout.addWidget(self.table)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

    def keyPressEvent(self, event):
        # 监听 Ctrl+C 快捷键
        if event.key() == Qt.Key_C and event.modifiers() == Qt.ControlModifier:
            self.copy_selection_to_clipboard()
        else:
            super().keyPressEvent(event)

    def copy_selection_to_clipboard(self):
        # 获取选中的单元格
        selection = self.table.selectedIndexes()
        if selection:
            # 获取选中的行和列
            rows = sorted(index.row() for index in selection)
            columns = sorted(index.column() for index in selection)
            # 获取选中的数据
            data = []
            for row in range(rows[0], rows[-1] + 1):
                row_data = []
                for col in range(columns[0], columns[-1] + 1):
                    index = self.table.model().index(row, col)
                    row_data.append(str(index.data()))
                data.append("\t".join(row_data))
            # 将数据复制到剪贴板
            QApplication.clipboard().setText("\n".join(data))


def display_dataframe_in_window(df):
    app = QApplication(sys.argv)
    window = DataFrameViewer(df)
    window.show()
    sys.exit(app.exec_())
