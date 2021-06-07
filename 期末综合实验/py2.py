from matplotlib import lines
from pyhive import hive
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
plt.rcParams['font.sans-serif'] = ['SimHei'] # 步骤一（替换 sans-serif 字体）
plt.rcParams['axes.unicode_minus'] = False # 步骤二（解决坐标轴负数的负号显示问题）
conn = hive.Connection(host='119.3.212.133',port=10000,auth='NOSASL',username='root')
cursor = conn.cursor()
cursor.execute('SELECT uid,COUNT(*) AS cnt FROM sogou_100w.sogou_ext_20111230 GROUP BY uid ORDER BY cnt DESC LIMIT 20')
uid = []
clicks = []
for result in cursor.fetchall():
    uid.append(result[0])
    clicks.append(result[1])
cursor.close()
conn.close()
plt.bar(uid,clicks)
plt.title('点击次数最多的前50个用户-2018211582')
plt.xlabel("uid",fontsize=4)
plt.xticks(rotation = 10) 
plt.ylabel("点击次数")
plt.legend(['uid-点击次数'],loc=1)
for i,j in zip(uid,clicks):
    plt.text(i,j+2,"%d"%j,horizontalalignment='center')
plt.show()