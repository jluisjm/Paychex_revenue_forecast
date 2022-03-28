import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter

import pandas as pd
from statsmodels.tsa.stattools import adfuller
from statsmodels.graphics.tsaplots import plot_acf , plot_pacf


### Testing For Stationarity

def rolling_statistics(ts, window=12):
    """
    :param ts: Time series to plot moving average an moving std
    :return:
    """

    rolmean = ts.rolling(window).mean()
    rolstd = ts.rolling(window).std()

    return rolmean, rolstd


def adfuller_test(ts, critical_value=0.05, maxlag=None, autolag='AIC'):
    """

    :param ts:
    :param critical_value:
    :param maxlag:
    :param autolag:
    :return:
    """

    result = adfuller(ts, maxlag=maxlag, autolag=autolag)
    labels = ['ADF Test Statistic', 'p-value', '#Lags Used', '# Observations Used']
    output = pd.Series(result[0:4], index=labels)

    return output, result[1]


def formatter(x, pos):
    return f'{x/1000000}'


def stationarity_test(ts, critical_value=0.05, maxlag=None, autolag='AIC'):
    """

    :param ts:
    :param critical_value:
    :param maxlag:
    :param autolag:
    :param format:
    :return:
    """
    rolmean, rolstd = rolling_statistics(ts)

    table_test, test = adfuller_test(ts, critical_value, maxlag, autolag)

    if test <= critical_value:
        test_result = "Stationary - This is a stationary series"
    else:
        test_result = "Non-Stationary - This is a non-stationary series"

    fig = plt.figure(figsize=(12, 6), constrained_layout=True)
    gs = gridspec.GridSpec(9, 1, figure=fig)
    ax1 = fig.add_subplot(gs[0:7, 0])
    ax2 = fig.add_subplot(gs[8, 0])

    ts.plot(color='blue', label='Original', ax=ax1)
    ax1.legend(loc='best')
    rolmean.plot(color='red', label='Rolling Mean', ax=ax1)
    rolstd.plot(color='black', label='Rolling Std', ax=ax1)
    ax1.set_xlabel("")
    ax1.set_ylabel("Millions")
    ax1.yaxis.set_major_formatter(FuncFormatter(formatter))

    ax2.axis('off')
    ax2.table(cellText=table_test.values.round(4).reshape(4, 1),
              rowLabels=table_test.index,
              fontsize=14)

    ax2.set_title(test_result)
    fig.suptitle('Rolling Mean & Standard Deviation for {}'.format(ts.name))

    return fig

def autocorrelation_test(ts):

    fig, (ax1, ax2) = plt.subplots(1,2, figsize=(12,4))

    plot_acf(ts, ax=ax1)
    plot_pacf(ts, ax=ax2)

    fig.suptitle('ACR and PACR for {}'.format(ts.name))

    return fig


