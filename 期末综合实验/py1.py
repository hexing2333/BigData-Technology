from matplotlib import lines
from pyhive import hive
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
plt.rcParams['font.sans-serif'] = ['SimHei'] # 步骤一（替换 sans-serif 字体）
plt.rcParams['axes.unicode_minus'] = False # 步骤二（解决坐标轴负数的负号显示问题）
conn = hive.Connection(host='119.3.212.133',port=10000,auth='NOSASL',username='root')
cursor = conn.cursor()
cursor.execute('select rank,count(*) as cnt from sogou_100w.sogou_ext_20111230 group by rank order by rank limit 30')
rank = []
clicks = []
for result in cursor.fetchall():
    rank.append(result[0])
    clicks.append(result[1])
cursor.close()
conn.close()
plt.plot(rank,clicks,marker='D')
plt.title('rank与点击次数折线图-2018211582')
plt.xlabel("rank")
plt.ylabel("点击次数")
plt.legend(['rank-点击次数'],loc=3)
x_major_locator=MultipleLocator(1)
#把x轴的刻度间隔设置为1，并存在变量里
ax=plt.gca()
ax.xaxis.set_major_locator(x_major_locator)
for i,j in zip(rank,clicks):
    plt.text(i,j+2,"%d"%j,horizontalalignment='center')
plt.show()