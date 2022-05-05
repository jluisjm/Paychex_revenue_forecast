import os
import plotly.express as px
from pycaret.regression import *
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
from openpyxl import load_workbook
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
correlation_threshold = 0.5
features_threshold = 10
target_col = 'Interest on Funds Held for Clients'
has_drivers = False
has_actuals = True

ml_col = target_col+' - ML Predicted'
uts_col = target_col+' - UTS Predicted'
plan_col = target_col+' - Plan'
fcst_col = target_col+' - '+forecast_type+' Forecast'
fcst_cols = [ml_col,uts_col,plan_col,fcst_col]

file_path = "./data/clean/table_predictable.csv"
drive_path = "./data/clean/table_drivers.csv"
external_path = "./data/external/external_data_fred.csv"

# Set manually date if is necessary
model_run_date = datetime.today().strftime('%Y%m%d')

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
    corr_df = corr_df[corr_df[target_col].abs() >= correlation_threshold]

    # plt.figure(figsize=(8, 12))
    # heatmap = sns.heatmap(corr_df, vmin=-1, vmax=1, annot=True, cmap='BrBG')
    # heatmap.set_title('Features Correlating with '+target_col, fontdict={'fontsize':18}, pad=16)
    # plt.show()
    # run auto ml and get the most important features
    best = models.run_auto_ml(train_df, test_df, target_col, feature_cols, False, ml_criteria)

    ml_features = models.get_important_features('xgboost', features_threshold)
    ml_feature_cols = ml_features['Feature'].tolist()

    if len(ml_features.index) != 0:
        # plot the most important features
        fig = px.bar(ml_features.sort_values('Variable Importance', ascending=True),
                     x='Variable Importance',
                     y='Feature',
                     orientation='h',
                     title='Feature Importance Plot')
        fig.show()

    # ------------------------------------------------------------------------------------- #
    # re-run the auto ml with only the important features
    # ------------------------------------------------------------------------------------- #
    feature_cols = ml_feature_cols
    keeps = ['Calendar Date', target_col]+feature_cols
    train_df = train_df[keeps]
    test_df = test_df[keeps]
    comb_df = comb_df[keeps]

    best = models.run_auto_ml(train_df, test_df, target_col, feature_cols, False, ml_criteria)

    # ------------------------------------------------------------------------------------- #
    # generate and plot predicted values on the original dataset
    # ------------------------------------------------------------------------------------- #

    predictions = predict_model(best, data=comb_df)
    predictions['Date'] = pd.date_range(start=str(train_start_dt), end = str(test_end_dt), freq = 'MS')
    predictions.rename(columns={'Label':target_col+' - ML Predicted'}, inplace=True)
    fig = px.line(predictions, x='Date', y=[target_col, target_col+' - ML Predicted'], template = 'plotly_white')
    fig.show()

    # create the future predictions dataframe
    if has_actuals:
        act_df = all_df[all_df['Calendar Date'].astype(int) >= int(pred_start_dt)]
        act_df = act_df[['Calendar Date', target_col]]
        act_df['Calendar Date'] = pd.to_datetime(act_df['Calendar Date'])
        pred_df = models.run_auto_arima(comb_df, feature_cols, pred_start_dt, forecast_window)
        final_best = finalize_model(best)
        #future_dates = pd.date_range(start = pred_start_dt, end = pred_end_dt, freq = 'MS')
        pred_df = predict_model(final_best, data=pred_df)
        #pred_df = pred_df[['Calendar Date', 'Label']]
        pred_df = pred_df.rename(columns={'Label':ml_col})[['Calendar Date', ml_col]]
        concat_df = pd.merge(act_df, pred_df ,on='Calendar Date', how='inner')

        # get plan data
        plan_df = dl.get_clean_data(train_start_dt, pred_end_dt, file_path, type='plan')
        plan_df = plan_df[['Calendar Date', target_col]]
        plan_df.rename(columns={target_col:plan_col}, inplace=True)
        plan_df['Calendar Date'] = pd.to_datetime(plan_df['Calendar Date'])
        concat_df = pd.merge(concat_df,plan_df, on='Calendar Date', how='inner')

        # get forecast data
        fcst_df = dl.get_clean_data(train_start_dt, pred_end_dt, file_path,
                                    type='forecast',
                                    forecast_type=forecast_type)
        fcst_df = fcst_df[['Calendar Date', target_col]]
        fcst_df.rename(columns={target_col:fcst_col}, inplace=True)
        fcst_df['Calendar Date'] = pd.to_datetime(fcst_df['Calendar Date'])
        concat_df = pd.merge(concat_df,fcst_df, on='Calendar Date', how='inner')

    # run UTS
    uts_df = comb_df[['Calendar Date', target_col]]
    uts_df = models.run_auto_arima(uts_df, [target_col], pred_start_dt, forecast_window)
    uts_df.rename(columns={target_col:uts_col}, inplace=True)
    concat_df = pd.merge(concat_df,uts_df, on='Calendar Date', how='inner')

    # combine all data together
    concat_df = pd.concat([comb_df[['Calendar Date', target_col]],concat_df], axis=0)

    predictions_path = "./data/predictions/"+model_run_date
    if not os.path.exists(predictions_path):
        os.makedirs(predictions_path)
        print("Directory created")

    concat_df.to_parquet(predictions_path+"/"+target_col.replace(" ","")+".parquet")

    # compute mape_df
    mape_df = concat_df[['Calendar Date', target_col, ml_col, uts_col, plan_col, fcst_col]]
    mape_df = mape_df[mape_df['Calendar Date'] >= datetime.strptime(pred_start_dt, '%Y%m%d')]
    mape_df = models.compute_apes_and_mapes(mape_df, 'Calendar Date', target_col, fcst_cols)
    mape_df.rename(index={True:'MAPE'}, inplace=True)
    #print(mape_df)

    book = load_workbook('./mapes.xlsx')
    writer = pd.ExcelWriter('./mapes.xlsx', engine = 'openpyxl')
    writer.book = book
    mape_df.to_excel(writer, sheet_name = target_col[0:31], index=False)
    book.save('./mapes.xlsx')
    book.close()

    #mape_df.to_excel('./mapes.xlsx', sheet_name=target_col[0:31])

    # show plot
    fig = px.line(concat_df, x='Calendar Date', y=[target_col, ml_col, uts_col, plan_col, fcst_col], template='plotly_white')
    fig.show()