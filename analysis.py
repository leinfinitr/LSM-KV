# 用于对 level 配置分析的绘图
# 横轴为数据大小 1000：10000：1000
# 纵轴为单位时间操作的吞吐量
# 三种操作分别为 PUT, GET, DEL
# 三种 level 配置分别为 2，3, 4

import matplotlib.pyplot as plt
import numpy as np

PUT = [
    [12361, 9325, 8305, 6241, 4993, 5206, 5347, 3901, 4159],
    [16416, 8289, 8305, 7281, 6241, 4165, 3862, 5201, 4621],
    [10301, 7253, 8997, 7801, 6241, 6594, 5941, 5201, 3466],
]
GET = [
    [6936, 6874, 6748, 6752, 6545, 6493, 6450, 6580, 6161],
    [5375, 6381, 7215, 7170, 6966, 6402, 4110, 5770, 6450],
    [4645, 3399, 6586, 6179, 6616, 6597, 6564, 5390, 5517],
]

data_size = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000]

# PUT 使用横线
# GET 使用虚线
# 相同 level 的操作使用相同颜色
plt.figure(figsize=(10, 5))
plt.plot(data_size, PUT[0], color="red", linestyle="-", label="PUT level 2")
plt.plot(data_size, PUT[1], color="blue", linestyle="-", label="PUT level 3")
plt.plot(data_size, PUT[2], color="green", linestyle="-", label="PUT level 4")
plt.plot(data_size, GET[0], color="red", linestyle="--", label="GET level 2")
plt.plot(data_size, GET[1], color="blue", linestyle="--", label="GET level 3")
plt.plot(data_size, GET[2], color="green", linestyle="--", label="GET level 4")
plt.xlabel("Data Size")
plt.ylabel("Throughput")
plt.legend()
plt.show()

# 绘制不断插入一个长度为1000的字符串，统计每秒钟PUT操作的吞吐量
put = [
    6115,
    3229,
    2546,
    1324,
    2179,
    1542,
    1412,
    1625,
    1212,
    1422,
    811,
    1039,
    997,
    723,
    703,
    685,
    1105,
    854,
    830,
    809,
    787,
    768,
    378,
    741,
    726,
    534,
    526,
    348,
    513,
    674,
    662,
    650,
    480,
    632,
    622,
    459,
    755,
    692,
    490,
    720,
    708,
    698,
    657,
    437,
    405,
    531,
    528,
    390,
    516,
    512,
    378,
    378,
    374,
    123,
    123,
    492,
    368,
    360,
    360,
    480,
    469,
    234,
    117,
    234,
    464,
    342,
    228,
    456,
    339,
    444,
    444,
    333,
    220,
    432,
    432,
    324,
    429,
    525,
    315,
    420,
    416,
    306,
    306,
    306,
    408,
    404,
    396,
    396,
    396,
    396,
    389,
    384,
    288,
    288,
    288,
    288,
    192,
    280,
    279,
    279,
]
Time = np.arange(1, 101)
plt.figure(figsize=(10, 5))
plt.plot(Time, put, color="red", linestyle="-", label="PUT")
plt.xlabel("Time")
plt.ylabel("Throughput")
plt.legend()
plt.show()
