import matplotlib.pyplot as plt
import pandas as pd
from statsmodels.tsa.stattools import adfuller


### Testing For Stationarity

def rolling_statistics(ts):
    """
    :param ts: Time series to plot moving average an moving std
    :return:
    """

    rolmean = ts.rolling(12).mean()
    rolstd = ts.rolling(12).std()

    fig, ax = plt.subplots(figsize=(14,4))
    ts.plot(color='blue',label='Original', ax=ax)
    rolmean.plot(color='red', label='Rolling Mean', ax=ax)
    rolstd.plot(color='black', label = 'Rolling Std', ax=ax)
    plt.legend(loc='best')
    plt.title('Rolling Mean & Standard Deviation for {}'.format(ts.name))
    plt.show()


def adfuller_test(ts):
    """

    :param ts:
    :return:
    """

    print('Results of Dickey-Fuller Test:')
    result=adfuller(ts, autolag='AIC')
    labels = ['ADF Test Statistic','p-value','#Lags Used','Number of Observations Used']
    output = pd.Series(result[0:4], index=labels)

    if result[1] <= 0.05:
        print("Stationary - This is a stationary series")
    else:
        print("Non-Stationary - This is a non-stationary series")

    return output

