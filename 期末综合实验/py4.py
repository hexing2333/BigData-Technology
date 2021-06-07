from matplotlib import lines
from pyhive import hive
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
plt.rcParams['font.sans-serif'] = ['SimHei'] # 步骤一（替换 sans-serif 字体）
plt.rcParams['axes.unicode_minus'] = False # 步骤二（解决坐标轴负数的负号显示问题）
conn = hive.Connection(host='119.3.212.133',port=10000,auth='NOSASL',username='root')
cursor = conn.cursor()
cursor.execute("select hour from sogou_100w.sogou_ext_20111230")
hours = []
morning = []
midnight =[]
afternoon = []
night = []
for result in cursor.fetchall():
    hours.append(result[0])

for i in range(len(hours)):
    if(int(hours[i])>8 and int(hours[i])<=12):
        morning.append(int(hours[i]))
    if(int(hours[i])>12 and int(hours[i]) <=18):
        afternoon.append(int(hours[i]))
    if(int(hours[i])>18 and int(hours[i]) <=23):
        night.append(int(hours[i]))
    if(int(hours[i])>0 and int(hours[i]) <= 8):
        midnight.append(int(hours[i]))
    #hours.append(times[i][8:10])

x=[]
x.append(len(morning))
x.append(len(afternoon))
x.append(len(night))
x.append(len(midnight))
cursor.close()
conn.close()
labels=['morning','afternoon','night','midnight']
plt.pie(x,labels=labels,autopct="%0.2f%%",shadow=True)
plt.title('时间分布分析-2018211582')
plt.legend(loc='upper right')
plt.show()