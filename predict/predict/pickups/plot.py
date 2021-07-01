import matplotlib.pyplot as plt
import pandas as pd

predictions = pd.read_csv("result.csv")
for month in range(1, 13):
    pred = predictions[predictions['pumonth'] == month]
    x = range(1, pred.shape[0] + 1)
    plt.cla()
    plt.plot(x,pred['hour_count'])
    plt.plot(x,pred['predictions'])
    plt.legend(['target', 'prediction'])
    plt.xlabel('Hour')
    plt.ylabel('Orders')
    plt.title('target and prediction in month {} of 2018'.format(month))
    plt.savefig('target and prediction_{}.jpg'.format(month))