import pandas as pd
import warnings

warnings.filterwarnings("ignore")

cols = ['File'
    , 'Product0'
    , 'Account0'
    , 'Account Description'
    , 'Item'
    , 'FY 2015 Total'
    , 'Actual - 20140601'
    , 'Actual - 20140701'
    , 'Actual - 20140801'
    , 'Actual - 20140901'
    , 'Actual - 20141001'
    , 'Actual - 20141101'
    , 'Actual - 20141201'
    , 'Actual - 20150101'
    , 'Actual - 20150201'
    , 'Actual - 20150301'
    , 'Actual - 20150401'
    , 'Actual - 20150501'
    , 'FY 2016 Total'
    , 'Actual - 20150601'
    , 'Actual - 20150701'
    , 'Actual - 20150801'
    , 'Actual - 20150901'
    , 'Actual - 20151001'
    , 'Actual - 20151101'
    , 'Actual - 20151201'
    , 'Actual - 20160101'
    , 'Actual - 20160201'
    , 'Actual - 20160301'
    , 'Actual - 20160401'
    , 'Actual - 20160501'
    , 'FY 2017 Total'
    , 'Actual - 20160601'
    , 'Actual - 20160701'
    , 'Actual - 20160801'
    , 'Actual - 20160901'
    , 'Actual - 20161001'
    , 'Actual - 20161101'
    , 'Actual - 20161201'
    , 'Actual - 20170101'
    , 'Actual - 20170201'
    , 'Actual - 20170301'
    , 'Actual - 20170401'
    , 'Actual - 20170501'
    , 'FY 2018 Total'
    , 'Actual - 20170601'
    , 'Actual - 20170701'
    , 'Actual - 20170801'
    , 'Actual - 20170901'
    , 'Actual - 20171001'
    , 'Actual - 20171101'
    , 'Actual - 20171201'
    , 'Actual - 20180101'
    , 'Actual - 20180201'
    , 'Actual - 20180301'
    , 'Actual - 20180401'
    , 'Actual - 20180501'
    , 'FY 2019 Total'
    , 'Actual - 20180601'
    , 'Actual - 20180701'
    , 'Actual - 20180801'
    , 'Actual - 20180901'
    , 'Actual - 20181001'
    , 'Actual - 20181101'
    , 'Actual - 20181201'
    , 'Actual - 20190101'
    , 'Actual - 20190201'
    , 'Actual - 20190301'
    , 'Actual - 20190401'
    , 'Actual - 20190501'
    , 'FY 2020 Total'
    , 'Actual - 20190601'
    , 'Actual - 20190701'
    , 'Actual - 20190801'
    , 'Actual - 20190901'
    , 'Actual - 20191001'
    , 'Actual - 20191101'
    , 'Actual - 20191201'
    , 'Actual - 20200101'
    , 'Actual - 20200201'
    , 'Actual - 20200301'
    , 'Actual - 20200401'
    , 'Actual - 20200501'
    , 'FY 2021 Total'
    , 'Actual - 20200601'
    , 'Actual - 20200701'
    , 'Actual - 20200801'
    , 'Actual - 20200901'
    , 'Actual - 20201001'
    , 'Actual - 20201101'
    , 'Actual - 20201201'
    , 'Actual - 20210101'
    , 'Actual - 20210201'
    , 'Actual - 20210301'
    , 'Actual - 20210401'
    , 'Actual - 20210501'
    , 'Actual - 20210601'
    , 'Actual - 20210701'
    , 'Actual - 20210801'
    , 'Actual - 20210901'
    , 'Actual - 20211001'
    , 'Actual - 20211101'
    , 'Actual - 20211201'
    , 'Actual - 20220101'
    , 'Forecast - 20220201'
    , 'Forecast - 20220301'
    , 'Forecast - 20220401'
    , 'Forecast - 20220501']

flps = ['Actual - 20140601'
    , 'Actual - 20140701'
    , 'Actual - 20140801'
    , 'Actual - 20140901'
    , 'Actual - 20141001'
    , 'Actual - 20141101'
    , 'Actual - 20141201'
    , 'Actual - 20150101'
    , 'Actual - 20150201'
    , 'Actual - 20150301'
    , 'Actual - 20150401'
    , 'Actual - 20150501'
    , 'Actual - 20150601'
    , 'Actual - 20150701'
    , 'Actual - 20150801'
    , 'Actual - 20150901'
    , 'Actual - 20151001'
    , 'Actual - 20151101'
    , 'Actual - 20151201'
    , 'Actual - 20160101'
    , 'Actual - 20160201'
    , 'Actual - 20160301'
    , 'Actual - 20160401'
    , 'Actual - 20160501'
    , 'Actual - 20160601'
    , 'Actual - 20160701'
    , 'Actual - 20160801'
    , 'Actual - 20160901'
    , 'Actual - 20161001'
    , 'Actual - 20161101'
    , 'Actual - 20161201'
    , 'Actual - 20170101'
    , 'Actual - 20170201'
    , 'Actual - 20170301'
    , 'Actual - 20170401'
    , 'Actual - 20170501'
    , 'Actual - 20170601'
    , 'Actual - 20170701'
    , 'Actual - 20170801'
    , 'Actual - 20170901'
    , 'Actual - 20171001'
    , 'Actual - 20171101'
    , 'Actual - 20171201'
    , 'Actual - 20180101'
    , 'Actual - 20180201'
    , 'Actual - 20180301'
    , 'Actual - 20180401'
    , 'Actual - 20180501'
    , 'Actual - 20180601'
    , 'Actual - 20180701'
    , 'Actual - 20180801'
    , 'Actual - 20180901'
    , 'Actual - 20181001'
    , 'Actual - 20181101'
    , 'Actual - 20181201'
    , 'Actual - 20190101'
    , 'Actual - 20190201'
    , 'Actual - 20190301'
    , 'Actual - 20190401'
    , 'Actual - 20190501'
    , 'Actual - 20190601'
    , 'Actual - 20190701'
    , 'Actual - 20190801'
    , 'Actual - 20190901'
    , 'Actual - 20191001'
    , 'Actual - 20191101'
    , 'Actual - 20191201'
    , 'Actual - 20200101'
    , 'Actual - 20200201'
    , 'Actual - 20200301'
    , 'Actual - 20200401'
    , 'Actual - 20200501'
    , 'Actual - 20200601'
    , 'Actual - 20200701'
    , 'Actual - 20200801'
    , 'Actual - 20200901'
    , 'Actual - 20201001'
    , 'Actual - 20201101'
    , 'Actual - 20201201'
    , 'Actual - 20210101'
    , 'Actual - 20210201'
    , 'Actual - 20210301'
    , 'Actual - 20210401'
    , 'Actual - 20210501'
    , 'Actual - 20210601'
    , 'Actual - 20210701'
    , 'Actual - 20210801'
    , 'Actual - 20210901'
    , 'Actual - 20211001'
    , 'Actual - 20211101'
    , 'Actual - 20211201'
    , 'Actual - 20220101'
    , 'Forecast - 20220201'
    , 'Forecast - 20220301'
    , 'Forecast - 20220401'
    , 'Forecast - 20220501']

drops = ['FY 2015 Total'
    , 'FY 2016 Total'
    , 'FY 2017 Total'
    , 'FY 2018 Total'
    , 'FY 2019 Total'
    , 'FY 2020 Total'
    , 'FY 2021 Total']

level_0_list = ['401K Asset fee & BP Revenue',
                '401K Fee Revenue',
                'ASO Allocation',
                'ASO Revenue - Oasis',
                'Benetrac',
                'Cafeteria Plans Revenue',
                'Delivery Revenue',
                'Emerging Products',
                'ESR Revenue',
                'Full Service Unemployment Revenue',
                'Health Benefits',
                'HR Online',
                'HR Solutions (PEO)',
                'Interest on Funds Held for Clients',
                'Other Processing Revenue',
                'Payroll blended products',
                'SurePayroll.',
                'Time & Attendance',
                'Total international',
                'Total Paychex Advance',
                'Total PEO',
                'W-2 Revenue',
                'Workers Comp - Payment Services']

ext_drivers = ['401kRevenue Drivers'
    , 'Benetrac Drivers'
    , 'Cafeteria Plans Revenue Drivers'
    , 'Emerging Products Drivers'
    , 'ESR Revenue Drivers'
    , 'Full Service Unemployment Revenue Drivers'
    , 'Health Benefits Drivers'
    , 'Online Revenue Drivers'
    , 'Payroll blended products Drivers'
    , 'PEO Revenue Drivers'
    , 'SurePayroll. Drivers'
    , 'Total Paychex Advance Drivers'
    , 'Workers Comp - Payment Services Drivers']

plan_cols = ['File'
    , 'Product0'
    , 'Account0'
    , 'Account Description'
    , 'Item'
    , 'Plan - 20210601'
    , 'Plan - 20210701'
    , 'Plan - 20210801'
    , 'Plan - 20210901'
    , 'Plan - 20211001'
    , 'Plan - 20211101'
    , 'Plan - 20211201'
    , 'Plan - 20220101'
    , 'Plan - 20220201'
    , 'Plan - 20220301'
    , 'Plan - 20220401'
    , 'Plan - 20220501']

fcst_cols = ['File'
    , 'Product0'
    , 'Account0'
    , 'Account Description'
    , 'Item'
    , '2+10 - YearTotal'
    , '2+10 - 20210601'
    , '2+10 - 20210701'
    , '2+10 - 20210801'
    , '2+10 - 20210901'
    , '2+10 - 20211001'
    , '2+10 - 20211101'
    , '2+10 - 20211201'
    , '2+10 - 20220101'
    , '2+10 - 20220201'
    , '2+10 - 20220301'
    , '2+10 - 20220401'
    , '2+10 - 20220501'
    , '3+9 - YearTotal'
    , '3+9 - 20210601'
    , '3+9 - 20210701'
    , '3+9 - 20210801'
    , '3+9 - 20210901'
    , '3+9 - 20211001'
    , '3+9 - 20211101'
    , '3+9 - 20211201'
    , '3+9 - 20220101'
    , '3+9 - 20220201'
    , '3+9 - 20220301'
    , '3+9 - 20220401'
    , '3+9 - 20220501'
    , '4+8 - YearTotal'
    , '4+8 - 20210601'
    , '4+8 - 20210701'
    , '4+8 - 20210801'
    , '4+8 - 20210901'
    , '4+8 - 20211001'
    , '4+8 - 20211101'
    , '4+8 - 20211201'
    , '4+8 - 20220101'
    , '4+8 - 20220201'
    , '4+8 - 20220301'
    , '4+8 - 20220401'
    , '4+8 - 20220501'
    , '5+7 - YearTotal'
    , '5+7 - 20210601'
    , '5+7 - 20210701'
    , '5+7 - 20210801'
    , '5+7 - 20210901'
    , '5+7 - 20211001'
    , '5+7 - 20211101'
    , '5+7 - 20211201'
    , '5+7 - 20220101'
    , '5+7 - 20220201'
    , '5+7 - 20220301'
    , '5+7 - 20220401'
    , '5+7 - 20220501'
    , '6+6 - YearTotal'
    , '6+6 - 20210601'
    , '6+6 - 20210701'
    , '6+6 - 20210801'
    , '6+6 - 20210901'
    , '6+6 - 20211001'
    , '6+6 - 20211101'
    , '6+6 - 20211201'
    , '6+6 - 20220101'
    , '6+6 - 20220201'
    , '6+6 - 20220301'
    , '6+6 - 20220401'
    , '6+6 - 20220501'
    , '8+4 - YearTotal'
    , '8+4 - 20210601'
    , '8+4 - 20210701'
    , '8+4 - 20210801'
    , '8+4 - 20210901'
    , '8+4 - 20211001'
    , '8+4 - 20211101'
    , '8+4 - 20211201'
    , '8+4 - 20220101'
    , '8+4 - 20220201'
    , '8+4 - 20220301'
    , '8+4 - 20220401'
    , '8+4 - 20220501']

total_payroll_list = ['Payroll blended products'
    , 'W-2 Revenue'
    , 'Delivery Revenue'
    , 'ASO Allocation'
    , 'Other Processing Revenue'
    , 'SurePayroll.'
    , 'Total international']

total_401k_list = ['401K Asset fee & BP Revenue', '401K Fee Revenue']

total_aso_list = total_payroll_list + total_401k_list + ['HR Solutions (PEO)', 'ASO Revenue - Oasis']

total_online_svcs_list = ['HR Online', 'Time & Attendance']

mgt_sol_rev_list = ['Total Paychex Advance'
    , 'Full Service Unemployment Revenue'
    , 'ESR Revenue'
    , 'Cafeteria Plans Revenue'
    , 'Benetrac'
    , 'Emerging Products']

total_peo_list = ['Total PEO']

total_insurance_list = ['Workers Comp - Payment Se', 'Health Benefits']

funds_interest_list = ['Interest on Funds Held for Clients']

total_peo_and_insurance_list = total_peo_list + total_insurance_list
total_svc_rev_list = mgt_sol_rev_list + total_peo_and_insurance_list


def get_level_0_data(start_dt, end_dt):
    data_dir = './'
    data_file = 'Paychex Consolidated Revenue Data v1.xlsx'

    df = pd.read_excel(data_dir + data_file, sheet_name='Consolidated Data', usecols="A:CR,FV:GG", names=cols,
                       skiprows=5)

    # drop the totals columns
    df.drop(columns=drops, inplace=True)

    # drop unnecessary rows
    df = df.dropna(subset=['Item'])

    # keep level 0 rows only
    df0 = df[df['Item'].isin(level_0_list)]
    df0 = pd.melt(df0, id_vars=['File', 'Product0', 'Account0', 'Account Description', 'Item'],
                  value_vars=flps, var_name='Scenario_Date', value_name='Amount')

    df0[['Scenario', 'Calendar Date']] = df0.Scenario_Date.str.split(" - ", expand=True)
    df0.drop(columns=['Scenario_Date'], inplace=True)

    df0_acct = df0[['Item', 'Account0', 'Scenario', 'Calendar Date', 'Amount']]
    df0_prod = df0[['Item', 'Product0', 'Scenario', 'Calendar Date', 'Amount']]

    df0_group = df0.groupby(['Item', 'Scenario', 'Calendar Date'])['Amount'].sum().reset_index()
    # print(df0_group)
    item_list = df0_group['Item'].unique().tolist()

    df_piv = df0_group.pivot(index=['Calendar Date', 'Scenario'], columns='Item', values='Amount')
    df_piv = df_piv.reset_index()
    # print(df_piv)

    df_ts = df_piv[df_piv['Scenario'] == 'Actual']
    # df_ts['Series'] = np.arange(1,len(df_ts)+1)
    # df_ts['Year'] = df_ts['Calendar Date'].astype(str).str[:4]
    # df_ts['Month'] = df_ts['Calendar Date'].astype(str).str[-4:].str[:2]
    df_ts['Total Revenue'] = df_ts[item_list].sum(axis=1)
    df_ts.rename_axis(None, axis=1, inplace=True)
    df_ts = df_ts[
        (df_ts['Calendar Date'].astype(int) >= int(start_dt)) & (df_ts['Calendar Date'].astype(int) <= int(end_dt))]
    return (df_ts)


def get_clean_data(start_dt, end_dt, file_path):
    """
    Return dataframe for models from clean data format
    :param start_dt:
    :param end_dt:
    :param file_path:
    :return:
    """
    df = pd.read_csv(file_path, dtype={'Period': str, 'Calendar Date': str})

    f = ((df['Scenario'] == 'Actual') |
         ((df['Scenario'] == 'Forecast') & (df['Version'] == '8+4') & (df['Calendar Date'] <= '20220101'))) \
        & (df['Item'].isin(level_0_list))

    df = df[f] \
        .groupby(['Calendar Date', 'Item']).sum() \
        .unstack(level=1)['Value'] \
        .reset_index()

    df['Total Revenue'] = df.sum(axis=1)

    df = df[(df['Calendar Date'].astype(int) >= int(start_dt)) & (df['Calendar Date'].astype(int) <= int(end_dt))]

    return df


def get_driver_data(start_dt, end_dt, item):
    if item == '401K Asset fee & BP Revenue':
        item = '401kRevenue Drivers'
    elif item == '401K Fee Revenue':
        item = '401kRevenue Drivers'
    elif item == 'ASO Allocation':
        item = ''
    elif item == 'ASO Revenue - Oasis':
        item = ''
    elif item == 'Benetrac':
        item = 'Benetrac Drivers'
    elif item == 'Cafeteria Plans Revenue':
        item = 'Cafeteria Plans Revenue Drivers'
    elif item == 'Delivery Revenue':
        item = ''
    elif item == 'Emerging Products':
        item = 'Emerging Products Drivers'
    elif item == 'ESR Revenue':
        item = 'ESR Revenue Drivers'
    elif item == 'Full Service Unemployment Revenue':
        item = 'Full Service Unemployment Revenue Drivers'
    elif item == 'Health Benefits':
        item = 'Health Benefits Drivers'
    elif item == 'HR Online':
        item = 'Online Revenue Drivers'
    elif item == 'HR Solutions (PEO)':
        item = ''
    elif item == 'Interest on Funds Held for Clients':
        item = ''
    elif item == 'Other Processing Revenue':
        item = ''
    elif item == 'Payroll blended products':
        item = 'Payroll blended products Drivers'
    elif item == 'SurePayroll.':
        item = 'SurePayroll. Drivers'
    elif item == 'Time & Attendance':
        item = 'Online Revenue Drivers'
    elif item == 'Total international':
        item = ''
    elif item == 'Total Paychex Advance':
        item = 'Total Paychex Advance Drivers'
    elif item == 'Total PEO':
        item = 'PEO Revenue Drivers'
    elif item == 'W-2 Revenue':
        item = ''
    elif item == 'Workers Comp - Payment Se':
        item = 'Workers Comp - Payment Services Drivers'
    elif item == 'Total Revenue':
        item = ''
    else:
        item = ''
    if item == '':
        return 'ERROR - NO DATA FOR ITEM'

    data_dir = './'
    data_file = 'Paychex_data_drivers.xlsx'

    df = pd.read_excel(data_dir + data_file, sheet_name='Sheet1', usecols="A:CR,FT:GE", names=cols, skiprows=5)

    # drop the totals columns
    df.drop(columns=drops, inplace=True)
    df0 = df[df['Item'] == item]
    df0 = pd.melt(df0, id_vars=['File', 'Product0', 'Account0', 'Account Description', 'Item'],
                  value_vars=flps, var_name='Scenario_Date', value_name='Amount')

    df0[['Scenario', 'Calendar Date']] = df0.Scenario_Date.str.split(" - ", expand=True)
    df0.drop(columns=['Scenario_Date'], inplace=True)
    df0['Driver'] = df0['Product0'].astype(str) + '/' + df0['Account0'].astype(str) + '/' + df0[
        'Account Description'].astype(str)
    df0_group = df0.groupby(['Driver', 'Scenario', 'Calendar Date'])['Amount'].sum().reset_index()
    # print(df0_group)
    item_list = df0_group['Driver'].unique().tolist()

    df_piv = df0_group.pivot(index=['Calendar Date', 'Scenario'], columns='Driver', values='Amount')
    df_piv = df_piv.reset_index()
    df_piv = df_piv[
        (df_piv['Calendar Date'].astype(int) >= int(start_dt)) & (df_piv['Calendar Date'].astype(int) <= int(end_dt))]
    return df_piv


def get_clean_driver_data(start_dt, end_dt, item, file_path):
    if item == '401K Asset fee & BP Revenue':
        item = '401kRevenue Drivers'
    elif item == '401K Fee Revenue':
        item = '401kRevenue Drivers'
    elif item == 'ASO Allocation':
        item = ''
    elif item == 'ASO Revenue - Oasis':
        item = ''
    elif item == 'Benetrac':
        item = 'Benetrac Drivers'
    elif item == 'Cafeteria Plans Revenue':
        item = 'Cafeteria Plans Revenue Drivers'
    elif item == 'Delivery Revenue':
        item = ''
    elif item == 'Emerging Products':
        item = 'Emerging Products Drivers'
    elif item == 'ESR Revenue':
        item = 'ESR Revenue Drivers'
    elif item == 'Full Service Unemployment Revenue':
        item = 'Full Service Unemployment Revenue Drivers'
    elif item == 'Health Benefits':
        item = 'Health Benefits Drivers'
    elif item == 'HR Online':
        item = 'Online Revenue Drivers'
    elif item == 'HR Solutions (PEO)':
        item = ''
    elif item == 'Interest on Funds Held for Clients':
        item = ''
    elif item == 'Other Processing Revenue':
        item = ''
    elif item == 'Payroll blended products':
        item = 'Payroll blended products Drivers'
    elif item == 'SurePayroll.':
        item = 'SurePayroll. Drivers'
    elif item == 'Time & Attendance':
        item = 'Online Revenue Drivers'
    elif item == 'Total international':
        item = ''
    elif item == 'Total Paychex Advance':
        item = 'Total Paychex Advance Drivers'
    elif item == 'Total PEO':
        item = 'PEO Revenue Drivers'
    elif item == 'W-2 Revenue':
        item = ''
    elif item == 'Workers Comp - Payment Se':
        item = 'Workers Comp - Payment Services Drivers'
    elif item == 'Total Revenue':
        item = ''
    else:
        item = ''
    if item == '':
        return 'ERROR - NO DATA FOR ITEM'

    df = pd.read_csv(file_path, dtype={'Period': str, 'Calendar Date': str})

    f = ((df['Scenario'] == 'Actual') |
         ((df['Scenario'] == 'Forecast') & (df['Version'] == '8+4') & (df['Calendar Date'] <= '20220101')))

    df = df[f]

    df['driver'] = df['Product'] + '/' + df['Account'] + '/' + df['Detail']

    df = df[df['Item'] == item][['Calendar Date', 'driver', 'Value']] \
        .set_index(['Calendar Date', 'driver']) \
        .unstack(1)['Value'].reset_index()

    return df


def get_plan_data(start_dt, end_dt):
    data_dir = './'
    data_file = 'Paychex Consolidated Revenue Data v1.xlsx'

    df = pd.read_excel(data_dir + data_file, sheet_name='Consolidated Data', usecols="A:E,CU:DF", names=plan_cols,
                       skiprows=5)

    # drop unnecessary rows
    df = df.dropna(subset=['Item'])
    id_cols = ['File', 'Product0', 'Account0', 'Account Description', 'Item']
    value_cols = list(set(plan_cols) - set(id_cols))

    # keep level 0 rows only
    df0 = df[df['Item'].isin(level_0_list)]
    df0 = pd.melt(df0, id_vars=id_cols, value_vars=value_cols, var_name='Scenario_Date', value_name='Amount')
    df0[['Scenario', 'Calendar Date']] = df0.Scenario_Date.str.split(" - ", expand=True)
    df0.drop(columns=['Scenario_Date'], inplace=True)

    df0_acct = df0[['Item', 'Account0', 'Scenario', 'Calendar Date', 'Amount']]
    df0_prod = df0[['Item', 'Product0', 'Scenario', 'Calendar Date', 'Amount']]

    df0_group = df0.groupby(['Item', 'Scenario', 'Calendar Date'])['Amount'].sum().reset_index()
    # print(df0_group)
    item_list = df0_group['Item'].unique().tolist()

    df_piv = df0_group.pivot(index=['Calendar Date', 'Scenario'], columns='Item', values='Amount')
    df_piv = df_piv.reset_index()
    # print(df_piv)

    df_ts = df_piv[df_piv['Scenario'] == 'Plan']
    # df_ts['Series'] = np.arange(1,len(df_ts)+1)
    # df_ts['Year'] = df_ts['Calendar Date'].astype(str).str[:4]
    # df_ts['Month'] = df_ts['Calendar Date'].astype(str).str[-4:].str[:2]
    df_ts['Total Revenue'] = df_ts[item_list].sum(axis=1)
    df_ts.rename_axis(None, axis=1, inplace=True)
    df_ts = df_ts[
        (df_ts['Calendar Date'].astype(int) >= int(start_dt)) & (df_ts['Calendar Date'].astype(int) <= int(end_dt))]
    return df_ts


def get_forecast_data(start_dt, end_dt, forecast_type):
    data_dir = './'
    data_file = 'Paychex Consolidated Revenue Data v1.xlsx'

    df = pd.read_excel(data_dir + data_file, sheet_name='Consolidated Data', usecols="A:E,DH:GG", names=fcst_cols,
                       skiprows=5)

    # drop the totals columns
    df.drop(df.filter(regex='YearTotal').columns, axis=1, inplace=True)

    # drop unnecessary rows
    df = df.dropna(subset=['Item'])
    id_cols = ['File', 'Product0', 'Account0', 'Account Description', 'Item']
    value_cols = list(set(df.columns.tolist()) - set(id_cols))

    # keep level 0 rows only
    df0 = df[df['Item'].isin(level_0_list)]
    df0 = pd.melt(df0, id_vars=id_cols, value_vars=value_cols, var_name='Scenario_Date', value_name='Amount')
    df0[['Scenario', 'Calendar Date']] = df0.Scenario_Date.str.split(" - ", expand=True)
    df0.drop(columns=['Scenario_Date'], inplace=True)

    df0_acct = df0[['Item', 'Account0', 'Scenario', 'Calendar Date', 'Amount']]
    df0_prod = df0[['Item', 'Product0', 'Scenario', 'Calendar Date', 'Amount']]

    df0_group = df0.groupby(['Item', 'Scenario', 'Calendar Date'])['Amount'].sum().reset_index()
    # print(df0_group)
    item_list = df0_group['Item'].unique().tolist()

    df_piv = df0_group.pivot(index=['Calendar Date', 'Scenario'], columns='Item', values='Amount')
    df_piv = df_piv.reset_index()

    df_ts = df_piv[df_piv['Scenario'] == forecast_type]
    # df_ts['Series'] = np.arange(1,len(df_ts)+1)
    # df_ts['Year'] = df_ts['Calendar Date'].astype(str).str[:4]
    # df_ts['Month'] = df_ts['Calendar Date'].astype(str).str[-4:].str[:2]
    df_ts['Total Revenue'] = df_ts[item_list].sum(axis=1)
    df_ts.rename_axis(None, axis=1, inplace=True)
    df_ts = df_ts[
        (df_ts['Calendar Date'].astype(int) >= int(start_dt)) & (df_ts['Calendar Date'].astype(int) <= int(end_dt))]
    return df_ts


def get_external_data(start_dt, end_dt):
    data_dir = './'
    data_file = 'external_data.xlsx'

    df1 = pd.read_excel(data_dir + data_file, sheet_name='Nation Income & Expenditures')
    df2 = pd.read_excel(data_dir + data_file, sheet_name='Pop Employment Labor')
    df3 = pd.read_excel(data_dir + data_file, sheet_name='Prod & Bus Act')
    df4 = pd.read_excel(data_dir + data_file, sheet_name='Prices')
    df5 = pd.read_excel(data_dir + data_file, sheet_name='Money Bank Finance')

    df = pd.merge(df1, df2, on='date', how='left')
    df = pd.merge(df, df3, on='date', how='left')
    df = pd.merge(df, df4, on='date', how='left')
    df = pd.merge(df, df5, on='date', how='left')
    df = df[(df['date'].astype(int) >= int(start_dt)) & (df['date'].astype(int) <= int(end_dt))]
    df['date'] = df['date'].astype(str)
    df.rename(columns={'date': 'Calendar Date'}, inplace=True)
    return (df)
