import tkinter as tk
from tkinter import ttk


def display_dataframe_in_window(df):
    # 创建主窗口
    root = tk.Tk()
    root.title("上证所股票市场总览数据")
    root.geometry("400x400")  # 设置初始窗口大小

    # 创建容器框架
    container = ttk.Frame(root)
    container.pack(expand=True, fill="both")

    # 创建 Treeview 控件
    tree = ttk.Treeview(container, columns=list(df.columns), show="headings")

    # 添加垂直滚动条
    vsb = ttk.Scrollbar(container, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=vsb.set)

    # 添加水平滚动条
    hsb = ttk.Scrollbar(container, orient="horizontal", command=tree.xview)
    tree.configure(xscrollcommand=hsb.set)

    # 设置列标题和宽度
    for col in df.columns:
        tree.heading(col, text=col)
        tree.column(col, width=100, stretch=False)  # 固定列宽

    # 插入数据
    for index, row in df.iterrows():
        tree.insert("", "end", values=list(row))

    # 布局控件
    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")

    # 配置行列权重
    container.grid_rowconfigure(0, weight=1)
    container.grid_columnconfigure(0, weight=1)

    # 窗口关闭处理
    def on_closing():
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()
