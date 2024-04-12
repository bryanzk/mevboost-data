import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as ticker

def adjust_plot_x_axis(ax):
    ax.xaxis.set_major_locator(ticker.MultipleLocator(base=10))  # 设置主要刻度为1秒
    ax.xaxis.set_minor_locator(ticker.MultipleLocator(base=1))   # 设置次要刻度为0.1秒
    # 逆转x轴   
    ax.invert_xaxis()
    return ax