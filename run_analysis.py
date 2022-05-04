import plotly.express as px
from pycaret.regression import *
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import src.paychex_ml.data_loader as dl
import src.paychex_ml.models as models

# ------------------------------------------------------------------------------------- #
# This is where we change parameters to the model
# ------------------------------------------------------------------------------------- #
train_start_dt = '20140601'
train_end_dt = '20200501'
test_start_dt = '20200601'
test_end_dt = '20210501'
pred_start_dt = '20210601'
pred_end_dt = '20220101'
ml_criteria = 'MAE'
forecast_window = 8
forecast_type = '2+10'
target_col = '401K Asset fee & BP Revenue'
has_drivers = True
has_actuals = True

ml_col = target_col+' - ML Predicted'
uts_col = target_col+' - UTS Predicted'
plan_col = target_col+' - Plan'
fcst_col = target_col+' - '+forecast_type+' Forecast'
fcst_cols = [ml_col,uts_col,plan_col,fcst_col]

file_path = "./data/clean/table_predictable.csv"
drive_path = "./data/clean/table_drivers.csv"
external_path = "./data/external/external_data_fred.csv"

if __name__=="__main__":

    all_df = dl.get_clean_data(train_start_dt, pred_end_dt, file_path)
    all_df = all_df[['Calendar Date', target_col]]
    if has_drivers:
        driv_df = dl.get_clean_driver_data(train_start_dt, pred_end_dt, target_col, drive_path)
        all_df = pd.merge(all_df, driv_df, on='Calendar Date', how='inner')

    ext_df = pd.read_csv(external_path, dtype={'date': str}) \
        .rename(columns={'date': 'Calendar Date'})

    all_df = pd.merge(all_df, ext_df, on='Calendar Date', how='inner')

    # Train df
    train_df = all_df[all_df['Calendar Date'].astype(int) <= int(train_end_dt)]
    train_df['Calendar Date'] = pd.to_datetime(train_df['Calendar Date'])
    print('Shape of the training dataframe:')
    print(train_df.shape)

    # Test df
    test_df = all_df[(all_df['Calendar Date'].astype(int) >= int(test_start_dt)) & (all_df['Calendar Date'].astype(int) <= int(test_end_dt))]
    test_df['Calendar Date'] = pd.to_datetime(test_df['Calendar Date'])
    print('Shape of the testing dataframe:')
    print(test_df.shape)

    # Combined dataframe
    comb_df = pd.concat([train_df, test_df])
    print('Shape of the combination dataframe:')
    print(comb_df.shape)

    feature_cols = comb_df.columns.to_list()
    feature_cols.remove('Calendar Date')
    feature_cols.remove(target_col)

    # Run Correlations to target
    corr_df = comb_df.corr()[[target_col]]
    #corr_df = corr_df[corr_df[target_col].abs() >= 0.75]
    plt.figure(figsize=(8, 12))
    heatmap = sns.heatmap(corr_df, vmin=-1, vmax=1, annot=True, cmap='BrBG')
    heatmap.set_title('Features Correlating with '+target_col, fontdict={'fontsize':18}, pad=16)
    plt.show()

    # run auto ml and get the most important features
    best = models.run_auto_ml(train_df, test_df, target_col, feature_cols, False, ml_criteria)