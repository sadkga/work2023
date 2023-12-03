from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np

# TODO :准备数据集
data = np.array([[80, 2, 1], [75, 3, 0], [85, 1, 1], [78, 2, 0], [90, 1, 1], [80, 3, 0], [70, 2, 0], [85, 1, 1], [75, 2, 1]])
X = data[:, :-1]
y = data[:, -1]

# 将数据集划分为训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 创建线性回归模型并进行训练
model = LinearRegression()
model.fit(X_train, y_train)

# 使用训练好的模型进行预测,并计算预测误差
y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)

# 输出预测结果和误差
print("预测结果:", y_pred)
print("误差:", mse)

# BUG

