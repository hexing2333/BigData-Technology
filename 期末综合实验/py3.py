from matplotlib import lines
from pyhive import hive
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
plt.rcParams['font.sans-serif'] = ['SimHei'] # 步骤一（替换 sans-serif 字体）
plt.rcParams['axes.unicode_minus'] = False # 步骤二（解决坐标轴负数的负号显示问题）
conn = hive.Connection(host='119.3.212.133',port=10000,auth='NOSASL',username='root')
cursor = conn.cursor()
cursor.execute("SELECT SUM(IF(instr(url,keyword)<=0,1,0)),SUM(IF(instr(url,keyword)>0,1,0)) FROM (SELECT * FROM sogou_100w.sogou_ext_20111230 WHERE keyword LIKE '%www%') a")
sum = []
for result in cursor.fetchall():
    sum.append(result[0])
    sum.append(result[1])
cursor.close()
conn.close()
labels=['不包含','包含']
exp = [0, 0.1]
plt.pie(x=sum,labels=labels,explode=exp,autopct="%0.2f%%",shadow=True)
plt.legend()
plt.title('准确率分析-2018211582')
plt.show()