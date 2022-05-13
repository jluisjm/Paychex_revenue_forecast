import argparse, os, pickle
import plotly.express as px
from pycaret.regression import *
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
from openpyxl import load_workbook, Workbook
import src.paychex_ml.data_loader as dl
import src.paychex_ml.models as models

items_dicctionary = {
    '10': ('Total Payroll Revenue.', True, 1),
    '11': ('Payroll blended products', True, 0),
    '12': ('W-2 Revenue', False, 0),
    '13': ('Delivery Revenue', False, 0),
    '14': ('ASO Allocation', False, 0),
    '15': ('Other Processing Revenue', False, 0),
    '16': ('SurePayroll.', True, 0),
    '17': ('Total international', False, 0),
    '20': ('Total 401k', True, 1),
    '21': ('401K Fee Revenue', True, 0),
    '22': ('401K Asset fee & BP Revenue', True, 0),
    '30': ('Total ASO Revenue', False, 1),
    '31': ('HR Solutions (PEO)', False, 0),
    '32': ('ASO Revenue - Oasis', False, 0),
    '40': ('Total Online Services', True, 1),
    '41': ('HR Online', False, 0),
    '42': ('Time & Attendance', False, 0),
    '50': ('Other Management Solutions', True, 1),
    '51': ('Total Paychex Advance', True, 0),
    '52': ('Full Service Unemployment Revenue',  True, 0),
    '53': ('ESR Revenue', True, 0),
    '54': ('Cafeteria Plans Revenue',  True, 0),
    '55': ('Benetrac', True, 0),
    '56': ('Emerging Products', True, 0),
    '61': ('Total PEO', False, 0),
    '70': ('Total Insurance Services', True, 1),
    '71': ('Workers Comp - Payment Services', True, 0),
    '72': ('Health Benefits', True, 0),
    '81': ('Interest on Funds Held for Clients', False, 0),
    '100': ('Total Revenue', True, 1)
}

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
feature_selection = 'ml_features'
has_actuals = True

#target_col_id = '13'

#target_col_id = input("Select the target column id: ")

# parser = argparse.ArgumentParser()
# parser.add_argument('--item', type=str)
# args = parser.parse_args()
# if args.item:
#     target_col_id = args.item



# ml_col = target_col+' - ML Predicted'
# uts_col = target_col+' - UTS Predicted'
# plan_col = target_col+' - Plan'
# fcst_col = target_col+' - '+forecast_type+' Forecast'
ml_col = 'ML Predicted'
uts_col = 'UTS Predicted'
plan_col = 'Plan'
fcst_col = 'Forecast'
fcst_cols = [plan_col,fcst_col,ml_col,uts_col]

file_path = "./data/clean/table_predictable.csv"
drive_path = "./data/clean/table_drivers.csv"
external_path = "./data/external/external_data_fred.csv"

# Set manually date if is necessary
model_run_date = datetime.today().strftime('%Y%m%d')

def print_menu():
    print("00", "--", "All items")
    for key in items_dicctionary.keys():
        print (key, '--', items_dicctionary[key][0])

if __name__=="__main__":

    print_menu()
    target_col_id = input("Select the target column id: ")

    if target_col_id == '00':
        it = items_dicctionary.values()
        print("Running model training all items")
    else:
        it = [items_dicctionary[target_col_id]]
        print("Running model for {}".format(items_dicctionary[target_col_id]))

    predictions_path = "./data/predictions/"+model_run_date
    if not os.path.exists(predictions_path):
        os.makedirs(predictions_path)
        print("Directory created")

    figures_path = "./data/figures/"+model_run_date
    if not os.path.exists(figures_path):
        os.makedirs(figures_path)
        print("Directory created")

    metadata_path = "./data/metadata/"
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
        print("Directory created")

    model_path = "./data/models/"+model_run_date
    if not os.path.exists(model_path):
        os.makedirs(model_path)
        print("Directory created")

    for target_col, has_drivers, level in it:

        all_df = dl.get_clean_data(train_start_dt, pred_end_dt, file_path, level=level)
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
        corr_df = corr_df[corr_df[target_col].abs() >= correlation_threshold].sort_values(target_col, ascending=False)

        plt.figure(figsize=(18, 12))
        heatmap = sns.heatmap(corr_df, vmin=-1, vmax=1, annot=True, cmap='BrBG')
        heatmap.set_title('Features Correlating with '+target_col, fontdict={'fontsize':18}, pad=16)
        plt.savefig(figures_path+"/{}_correlations.png".format(target_col))

        # run auto ml and get the most important features
        best = models.run_auto_ml(train_df, test_df, target_col, feature_cols, False, ml_criteria)

        # Here we need to figure out which set of features we want to use
        # ml_features or corr_features
        if feature_selection == 'ml_features':
            ml_features = models.get_important_features('xgboost', features_threshold)
            feature_cols = ml_features['Feature'].tolist()
        elif feature_selection == 'corr_features':
            corr_df = corr_df.rename_axis('Feature').reset_index()
            feature_cols = corr_df['Feature'].tolist()
        else:
            print("No feature selection. This could take a while!")

        if len(ml_features.index) != 0:
            # plot the most important features
            fig = px.bar(ml_features.sort_values('Variable Importance', ascending=True),
                         x='Variable Importance',
                         y='Feature',
                         orientation='h',
                         title='Feature Importance Plot',
                         width=800, height=400)
            fig.write_image(figures_path+"/{}_featureimportance.png".format(target_col))

        with open(model_path + '/{}_features'.format(target_col), "wb") as fp:   #Pickling
            pickle.dump(feature_cols, fp)
        # ------------------------------------------------------------------------------------- #
        # re-run the auto ml with only the important features
        # ------------------------------------------------------------------------------------- #
        keeps = ['Calendar Date', target_col]+feature_cols
        train_df = train_df[keeps]
        test_df = test_df[keeps]
        comb_df = comb_df[keeps]

        best = models.run_auto_ml(train_df, test_df, target_col, feature_cols, False, ml_criteria)
        dt_results = pull()

        scores_path = metadata_path + model_run_date + "_scores.xlsx"
        if os.path.exists(scores_path):
            book = load_workbook(scores_path)
        else:
            book = Workbook()
        with pd.ExcelWriter(scores_path, engine = 'openpyxl') as writer:
            writer.book = book
            dt_results.to_excel(writer, sheet_name = target_col[0:30], index=False)

        # ------------------------------------------------------------------------------------- #
        # generate and plot predicted values on the original dataset
        # ------------------------------------------------------------------------------------- #

        predictions = predict_model(best, data=comb_df)
        predictions['Date'] = pd.date_range(start=str(train_start_dt), end = str(test_end_dt), freq = 'MS')
        predictions.rename(columns={'Label':target_col+' - ML Predicted'}, inplace=True)
        fig = px.line(predictions, x='Date', y=[target_col, target_col+' - ML Predicted'],
                      template = 'plotly_white',
                      width=800, height=400)
        fig.write_image(figures_path+"/{}_prediction.png".format(target_col))

        pipeline, name = save_model(best, model_path + '/{}_model'.format(target_col))

        # create the future predictions dataframe
        if has_actuals:
            act_df = all_df[all_df['Calendar Date'].astype(int) >= int(pred_start_dt)]
            act_df = act_df[['Calendar Date', target_col]]
            act_df['Calendar Date'] = pd.to_datetime(act_df['Calendar Date'])
            pred_df, _ = models.run_auto_arima(comb_df, feature_cols, pred_start_dt, forecast_window, ci=False)
            final_best = finalize_model(best)
            pred_df = predict_model(final_best, data=pred_df)
            pred_df = pred_df.rename(columns={'Label':ml_col})[['Calendar Date', ml_col]]
            concat_df = pd.merge(act_df, pred_df ,on='Calendar Date', how='inner')

            # get plan data
            plan_df = dl.get_clean_data(train_start_dt, pred_end_dt, file_path, type='plan', level=level)
            plan_df = plan_df[['Calendar Date', target_col]]
            plan_df.rename(columns={target_col:plan_col}, inplace=True)
            plan_df['Calendar Date'] = pd.to_datetime(plan_df['Calendar Date'])
            concat_df = pd.merge(concat_df,plan_df, on='Calendar Date', how='inner')

            # get forecast data
            fcst_df = dl.get_clean_data(train_start_dt, pred_end_dt, file_path,
                                        type='forecast',
                                        forecast_type=forecast_type,
                                        level=level)
            fcst_df = fcst_df[['Calendar Date', target_col]]
            fcst_df.rename(columns={target_col:fcst_col}, inplace=True)
            fcst_df['Calendar Date'] = pd.to_datetime(fcst_df['Calendar Date'])
            concat_df = pd.merge(concat_df,fcst_df, on='Calendar Date', how='inner')

        # run UTS
        uts_df = comb_df[['Calendar Date', target_col]]
        uts_df, uts_model = models.run_auto_arima(uts_df, [target_col], pred_start_dt, forecast_window, ci=True)
        uts_df.rename(columns={target_col:uts_col}, inplace=True)
        concat_df = pd.merge(concat_df,uts_df, on='Calendar Date', how='inner')

        with open(model_path + '/{}_uts_model.pkl'.format(target_col), 'wb') as pkl:
            pickle.dump(uts_model, pkl)

        # combine all data together
        concat_df = pd.concat([comb_df[['Calendar Date', target_col]],concat_df], axis=0)

        # compute mape_df
        mape_df = concat_df[['Calendar Date', target_col, ml_col, uts_col, plan_col, fcst_col]]
        mape_df = mape_df[mape_df['Calendar Date'] >= datetime.strptime(pred_start_dt, '%Y%m%d')]
        mape_df = models.compute_apes_and_mapes(mape_df, 'Calendar Date', target_col, fcst_cols)
        mape_df = mape_df.rename(index={True:'MAPE'})
        mape_df = pd.concat([mape_df.reset_index(drop=True), uts_df], axis=1)

        mape_path = metadata_path + model_run_date + "_mape.xlsx"
        if os.path.exists(mape_path):
            book = load_workbook(mape_path)
        else:
            book = Workbook()
        with pd.ExcelWriter(mape_path, engine = 'openpyxl') as writer:
            writer.book = book
            mape_df.to_excel(writer, sheet_name = target_col[0:30], index=False)

        concat_df['Item'] = target_col
        concat_df = concat_df.rename(columns={target_col: 'Actual'}).reset_index(drop=True)

        # show plot
        fig = px.line(concat_df, x='Calendar Date', y=['Actual', ml_col, uts_col, plan_col, fcst_col],
                      template='plotly_white',
                      width=800, height=400)
        fig.write_image(figures_path+"/{}_prediction_impfeat.png".format(target_col))

        # concat_df = concat_df.rename(columns={
        #     target_col: 'Actual',
        #     ml_col: 'ML Predicted',
        #     uts_col: 'UTS Predicted',
        #     plan_col: 'Plan',
        #     fcst_col: 'Forecast'})
        concat_df = concat_df[['Calendar Date', 'Item','Actual', 'ML Predicted', 'UTS Predicted', 'Plan', 'Forecast',
                               'Lower CI', 'Upper CI']]
        concat_df = concat_df[concat_df['Calendar Date'] >= pred_start_dt]
        concat_df.to_parquet(predictions_path+"/"+target_col.replace(" ","")+".parquet", index=False)
