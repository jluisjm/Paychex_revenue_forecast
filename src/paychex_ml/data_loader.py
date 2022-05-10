import pandas as pd
import warnings

warnings.filterwarnings("ignore")

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

level_1_dict = {'401K Asset fee & BP Revenue': 'Total 401k',
                '401K Fee Revenue': 'Total 401k',
                'ASO Allocation': 'Total Payroll Revenue.',
                'ASO Revenue - Oasis': 'Total ASO Revenue',
                'Benetrac': 'Other Management Solutions',
                'Cafeteria Plans Revenue': 'Other Management Solutions',
                'Delivery Revenue': 'Total Payroll Revenue.',
                'Emerging Products': 'Other Management Solutions',
                'ESR Revenue': 'Other Management Solutions',
                'Full Service Unemployment Revenue': 'Other Management Solutions',
                'Health Benefits': 'Total Insurance Services',
                'HR Online': 'Total Online Services',
                'HR Solutions (PEO)': 'Total ASO Revenue',
                'Interest on Funds Held for Clients': 'Interest on Funds Held for Clients',
                'Other Processing Revenue': 'Total Payroll Revenue.',
                'Payroll blended products': 'Total Payroll Revenue.',
                'SurePayroll.': 'Total Payroll Revenue.',
                'Time & Attendance': 'Total Online Services',
                'Total international': 'Total Payroll Revenue.',
                'Total Paychex Advance': 'Other Management Solutions',
                'Total PEO': 'Total PEO',
                'W-2 Revenue': 'Total Payroll Revenue.',
                'Workers Comp - Payment Services': 'Total Insurance Services'}


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


def get_clean_data(start_dt, end_dt, file_path, type='actual', forecast_type='10+2', level=0):
    """
    Return dataframe for models from clean data format
    :param start_dt:
    :param end_dt:
    :param file_path:
    :param level:
    :param forecast_type:
    :param type:
    :return: Dataframe
    """
    df = pd.read_csv(file_path, dtype={'Period': str, 'Calendar Date': str})

    if type == 'actual':
        f = ((df['Scenario'] == 'Actual') |
             ((df['Scenario'] == 'Forecast') & (df['Version'] == '8+4') & (df['Calendar Date'] <= '20220101'))) \
            & (df['Item'].isin(level_0_list))
    elif type == 'plan':
        f = ((df['Scenario'] == 'Plan') & (df['Item'].isin(level_0_list)))
    elif type == 'forecast':
        f = df['Version'] == forecast_type
    else:
        print("type not valid")

    df = df[f] \
        .groupby(['Calendar Date', 'Item']).sum() \
        .unstack(level=1)['Value'] \
        .reset_index()

    if level == 1:
        df = df.set_index('Calendar Date').groupby(level_1_dict, axis=1).sum().reset_index()

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

    df = pd.read_csv(file_path, dtype={'Period': str, 'Calendar Date': str})

    f = ((df['Scenario'] == 'Actual') |
         ((df['Scenario'] == 'Forecast') & (df['Version'] == '8+4') & (df['Calendar Date'] <= '20220101')))

    df = df[f]

    df['driver'] = df['Product'] + '/' + df['Account'] + '/' + df['Detail']

    if item == '401K Asset fee & BP Revenue':
        item_filter = (df['Item'] == '401kRevenue Drivers')
    elif item == '401K Fee Revenue':
        item_filter = (df['Item'] == '401kRevenue Drivers')
    elif item == 'ASO Allocation':
        item_filter = (df['Item'] == '')
    elif item == 'ASO Revenue - Oasis':
        item_filter = (df['Item'] == '')
    elif item == 'Benetrac':
        item_filter = (df['Item'] == 'Benetrac Drivers')
    elif item == 'Cafeteria Plans Revenue':
        item_filter = (df['Item'] == 'Cafeteria Plans Revenue Drivers')
    elif item == 'Delivery Revenue':
        item_filter = (df['Item'] == '')
    elif item == 'Emerging Products':
        item_filter = (df['Item'] == 'Emerging Products Drivers')
    elif item == 'ESR Revenue':
        item_filter = (df['Item'] == 'ESR Revenue Drivers')
    elif item == 'Full Service Unemployment Revenue':
        item_filter = (df['Item'] == 'Full Service Unemployment Revenue Drivers')
    elif item == 'Health Benefits':
        item_filter = (df['Item'] == 'Health Benefits Drivers')
    elif item == 'HR Online':
        item_filter = (df['Item'] == 'Online Revenue Drivers')
    elif item == 'HR Solutions (PEO)':
        item_filter = (df['Item'] == '')
    elif item == 'Interest on Funds Held for Clients':
        item_filter = (df['Item'] == '')
    elif item == 'Other Processing Revenue':
        item_filter = (df['Item'] == '')
    elif item == 'Payroll blended products':
        item_filter = (df['Item'] == 'Payroll blended products Drivers')
    elif item == 'SurePayroll.':
        item_filter = (df['Item'] == 'SurePayroll. Drivers')
    elif item == 'Time & Attendance':
        item_filter = (df['Item'] == 'Online Revenue Drivers')
    elif item == 'Total international':
        item_filter = (df['Item'] == '')
    elif item == 'Total Paychex Advance':
        item_filter = (df['Item'] == 'Total Paychex Advance Drivers')
    elif item == 'Total PEO':
        item_filter = (df['Item'] == 'PEO Revenue Drivers')
    elif item == 'W-2 Revenue':
        item_filter = (df['Item'] == '')
    elif item == 'Workers Comp - Payment Services':
        item_filter = (df['Item'] == 'Workers Comp - Payment Services Drivers')
    elif item == 'Total Payroll Revenue.':
        item_filter = (df['Item'] == 'Payroll blended products Drivers') \
                      | (df['Item'] == 'SurePayroll. Drivers')
    elif item == 'Total 401k':
        item_filter = (df['Item'] == '401kRevenue Drivers')
    elif item == 'Total ASO Revenue':
        item_filter = (df['Item'] == '')
    elif item == 'Total Online Services':
        item_filter = (df['Item'] == 'Online Revenue Drivers')
    elif item == 'Other Management Solutions':
        item_filter = (df['Item'] == 'Benetrac Drivers') \
                      | (df['Item'] == 'Cafeteria Plans Revenue Drivers') \
                      | (df['Item'] == 'Emerging Products Drivers') \
                      | (df['Item'] == 'ESR Revenue Drivers') \
                      | (df['Item'] == 'Full Service Unemployment Revenue Drivers') \
                      | (df['Item'] == 'Total Paychex Advance Drivers') \
                      | (df['Item'] == 'PEO Revenue Drivers')
    elif item == 'Total Insurance Services':
        item_filter = (df['Item'] == 'Health Benefits Drivers') \
                      | (df['Item'] == 'Workers Comp - Payment Services Drivers')
    elif item == 'Total Revenue':
        item_filter = (df['Item'] == '401kRevenue Drivers') \
        | (df['Item'] == 'Benetrac Drivers') \
        | (df['Item'] == 'Cafeteria Plans Revenue Drivers') \
        | (df['Item'] == 'Emerging Products Drivers') \
        | (df['Item'] == 'ESR Revenue Drivers') \
        | (df['Item'] == 'Full Service Unemployment Revenue Drivers') \
        | (df['Item'] == 'Health Benefits Drivers') \
        | (df['Item'] == 'Online Revenue Drivers') \
        | (df['Item'] == 'Payroll blended products Drivers') \
        | (df['Item'] == 'SurePayroll. Drivers') \
        | (df['Item'] == 'Total Paychex Advance Drivers') \
        | (df['Item'] == 'PEO Revenue Drivers') \
        | (df['Item'] == 'Workers Comp - Payment Services Drivers')
    else:
        item_filter = ''

    # if item == '':
    #     return 'ERROR - NO DATA FOR ITEM'

    df = df[item_filter][['Calendar Date', 'driver', 'Value']] \
        .drop_duplicates() \
        .set_index(['Calendar Date', 'driver']) \
        .unstack(1)['Value'].reset_index() \
        .fillna(0)

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
