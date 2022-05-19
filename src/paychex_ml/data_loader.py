import pandas as pd
import warnings

warnings.filterwarnings("ignore")

level_dict = {
    '401K Asset fee & BP Revenue': ['Total 401k', 'Management Solutions Revenue.', 'Service Revenue'],
    '401K Fee Revenue': ['Total 401k', 'Management Solutions Revenue.', 'Service Revenue'],
    'ASO Allocation': ['Total Payroll Revenue.', 'Management Solutions Revenue.', 'Service Revenue'],
    'ASO Revenue - Oasis': ['Total ASO Revenue', 'Management Solutions Revenue.', 'Service Revenue'],
    'Benetrac': ['Other Management Solutions', 'Management Solutions Revenue.', 'Service Revenue'],
    'Cafeteria Plans Revenue': ['Other Management Solutions', 'Management Solutions Revenue.', 'Service Revenue'],
    'Delivery Revenue': ['Total Payroll Revenue.', 'Management Solutions Revenue.', 'Service Revenue'],
    'Emerging Products': ['Other Management Solutions', 'Management Solutions Revenue.', 'Service Revenue'],
    'ESR Revenue': ['Other Management Solutions', 'Management Solutions Revenue.', 'Service Revenue'],
    'Full Service Unemployment Revenue': ['Other Management Solutions', 'Management Solutions Revenue.', 'Service Revenue'],
    'Health Benefits': ['Total Insurance Services', 'Total PEO and Insurance Services.', 'Service Revenue'],
    'HR Online': ['Total Online Services', 'Management Solutions Revenue.', 'Service Revenue'],
    'HR Solutions (PEO)': ['Total ASO Revenue', 'Management Solutions Revenue.', 'Service Revenue'],
    'Interest on Funds Held for Clients': ['Interest on Funds Held for Clients', '', ''],
    'Other Processing Revenue': ['Total Payroll Revenue.', 'Management Solutions Revenue.', 'Service Revenue'],
    'Payroll blended products': ['Total Payroll Revenue.', 'Management Solutions Revenue.', 'Service Revenue'],
    'SurePayroll.': ['Total Payroll Revenue.', 'Management Solutions Revenue.', 'Service Revenue'],
    'Time & Attendance': ['Total Online Services', 'Management Solutions Revenue.', 'Service Revenue'],
    'Total international': ['Total Payroll Revenue.', 'Management Solutions Revenue.', 'Service Revenue'],
    'Total Paychex Advance': ['Other Management Solutions', 'Management Solutions Revenue.', 'Service Revenue'],
    'Total PEO': ['Total PEO', 'Total PEO and Insurance Services.', 'Service Revenue'],
    'W-2 Revenue': ['Total Payroll Revenue.', 'Management Solutions Revenue.', 'Service Revenue'],
    'Workers Comp - Payment Services': ['Total Insurance Services', 'Total PEO and Insurance Services.', 'Service Revenue']
}


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
            & (df['Item'].isin(level_dict.keys()))
    elif type == 'plan':
        f = ((df['Scenario'] == 'Plan') & (df['Item'].isin(level_dict.keys())))
    elif type == 'forecast':
        f = df['Version'] == forecast_type
    else:
        print("type not valid")

    df = df[f] \
        .groupby(['Calendar Date', 'Item']).sum() \
        .unstack(level=1)['Value'] \
        .reset_index()

    if level != 0:
        df = df.set_index('Calendar Date')\
            .groupby({i: j[level-1] for (i, j) in level_dict.items()}, axis=1).sum()\
            .reset_index()

    df['Total Revenue'] = df.sum(axis=1)

    df = df[(df['Calendar Date'].astype(int) >= int(start_dt)) & (df['Calendar Date'].astype(int) <= int(end_dt))]

    return df


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
    elif item == 'Management Solutions Revenue.':
        item_filter = (df['Item'] == '401kRevenue Drivers') \
                      | (df['Item'] == 'Benetrac Drivers') \
                      | (df['Item'] == 'Cafeteria Plans Revenue Drivers') \
                      | (df['Item'] == 'Emerging Products Drivers') \
                      | (df['Item'] == 'ESR Revenue Drivers') \
                      | (df['Item'] == 'Full Service Unemployment Revenue Drivers') \
                      | (df['Item'] == 'Online Revenue Drivers') \
                      | (df['Item'] == 'Payroll blended products Drivers') \
                      | (df['Item'] == 'SurePayroll. Drivers') \
                      | (df['Item'] == 'Total Paychex Advance Drivers')
    elif item == 'Total PEO and Insurance Services.':
        item_filter = (df['Item'] == 'Health Benefits Drivers') \
                      | (df['Item'] == 'PEO Revenue Drivers') \
                      | (df['Item'] == 'Workers Comp - Payment Services Drivers')
    elif item == 'Service Revenue':
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
