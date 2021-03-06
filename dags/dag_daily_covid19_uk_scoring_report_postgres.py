import datetime as dt
import subprocess
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Tamer',
    'start_date': dt.datetime(2021, 5, 27),
    'retries': 5,
    'retry_delay': dt.timedelta(minutes=1)
}
 
with DAG('dag_daily_covid19_uk_scoring_report_postgres', default_args=default_args, schedule_interval=timedelta(days=1), catchup=False) as dag:
    def _install_packages():
        try:
            import psycopg2 
        except:
            subprocess.check_call(['pip' ,'install', 'psycopg2-binary' ])
            import psycopg2

        try:
            from sqlalchemy import create_engine
        except:
            subprocess.check_call(['pip' ,'install', 'sqlalchemy' ])
            from sqlalchemy import create_engine

        try:
            import pandas as pd 
        except:
            subprocess.check_call(['pip' ,'install', 'pandas' ])
            import pandas as pd

        try:
            import matplotlib.pyplot as plt
        except:
            subprocess.check_call(['pip' ,'install', 'matplotlib' ])
            import matplotlib.pyplot as plt

        try:
            from sklearn.preprocessing import MinMaxScaler
        except:
            subprocess.check_call(['pip' ,'install', 'sklearn' ])
            from sklearn.preprocessing import MinMaxScaler
        

    def _get_list_of_days():
        list_of_days = []
            
        for year in range(2020, 2022):
            for month in range(1, 13):
                for day in range(1, 32):
                    month = int(month)

                    if day <= 9:
                        day = f'0{day}'
                    if month <= 9 :
                        month =f'0{month}'
                    
                    list_of_days.append(f'{month}-{day}-{year}')

        return list_of_days


    def _get_df_i(day):
        # I had to reimport the packages
        import pandas as pd

        df_i = None
        try: 
            url_day = f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{day}.csv'

            df_day = pd.read_csv(url_day)
            df_day['Day'] = day
            
            condition = (df_day.Country_Region == 'United Kingdom') & (df_day.Province_State == 'England')
            
            selected_columns = [
                'Day',
                'Country_Region',
                'Last_Update',
                'Lat',
                'Long_',
                'Confirmed',
                'Deaths',
                'Recovered',
                'Active',
                'Combined_Key',
                'Incident_Rate',
                'Case_Fatality_Ratio'
            ]

            df_i = df_day[condition][selected_columns].reset_index(drop=True)

        except:
            pass

        return df_i


    def _get_uk_covid19_daily_reports():
        # I had to reimport the packages
        import pandas as pd

        df_all = []

        list_of_days = _get_list_of_days()
        for day in list_of_days:
            df_all.append(_get_df_i(day))

        df_UK = pd.concat(df_all).reset_index(drop=True)

        # Create DateTime for Last_Update
        df_UK['Last_Update'] = pd.to_datetime(df_UK.Last_Update, infer_datetime_format=True)  
        df_UK['Day'] = pd.to_datetime(df_UK.Day, infer_datetime_format=True)  
        df_UK['Case_Fatality_Ratio'] = df_UK['Case_Fatality_Ratio'].astype(float)

        date_today = dt.datetime.today().strftime('%Y-%m-%d')
        df_UK.to_csv(f"/opt/airflow/data/uk_covid19_{date_today}.csv")

    
    def _plot_and_save_uk_covid19_scoring_report_to_csv():
        # I had to reimport the packages
        import pandas as pd
        from sklearn.preprocessing import MinMaxScaler
        import matplotlib.pyplot as plt

        date_today = dt.datetime.today().strftime('%Y-%m-%d')
        df_UK = pd.read_csv(f"/opt/airflow/data/uk_covid19_{date_today}.csv")

        # For scoring plot to include dates properly in the x-axis
        df_UK_reindexed_by_day = df_UK.copy()
        df_UK_reindexed_by_day.index = df_UK.Day

        selected_columns=['Confirmed', 'Deaths', 'Recovered', 'Active', 'Incident_Rate', 'Case_Fatality_Ratio']
        df_UK_selected = df_UK_reindexed_by_day[selected_columns]

        min_max_scaler = MinMaxScaler()
        df_UK_selected_minmax_scaled = pd.DataFrame(min_max_scaler.fit_transform(df_UK_selected[selected_columns]), columns=selected_columns)
        df_UK_selected_minmax_scaled.index = df_UK_selected.index
        df_UK_selected_minmax_scaled['Day'] = df_UK_reindexed_by_day.Day
        
        df_UK_selected_minmax_scaled[selected_columns].plot(figsize=(20, 10))
        plt.savefig(f"/opt/airflow/data/uk_scoring_report_{date_today}.png")

        df_UK_selected_minmax_scaled.to_csv(f"/opt/airflow/data/uk_scoring_report_{date_today}.csv")


    def _insert_uk_covid19_scoring_report_to_postgres_table():
        # I had to reimport the packages
        import pandas as pd
        from sqlalchemy import create_engine

        date_today = dt.datetime.today().strftime('%Y-%m-%d')
        df_UK = pd.read_csv(f"/opt/airflow/data/uk_covid19_{date_today}.csv")

        host = "postgres"
        database = "testDB"
        user = "me"
        password = "1234"
        port = '5432'
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

        df_UK.to_sql(f'uk_scoring_report_{date_today}', engine, if_exists='replace', index=False)

        engine.dispose()

    # Airflow DAG Operators
    install_packages = PythonOperator(
        task_id="install_packages",
         python_callable=_install_packages
    )
    
    get_uk_covid19_daily_reports = PythonOperator(
        task_id="get_uk_covid19_daily_reports",
         python_callable=_get_uk_covid19_daily_reports
    )
    
    plot_and_save_uk_covid19_scoring_report_to_csv = PythonOperator(
        task_id="plot_and_save_uk_covid19_scoring_report_to_csv",
        python_callable=_plot_and_save_uk_covid19_scoring_report_to_csv
    )

    insert_uk_covid19_scoring_report_to_postgres_table = PythonOperator(
        task_id="insert_uk_covid19_scoring_report_to_postgres_table",
        python_callable=_insert_uk_covid19_scoring_report_to_postgres_table
    )
    
    # Dag pipeline
    install_packages >> get_uk_covid19_daily_reports >> plot_and_save_uk_covid19_scoring_report_to_csv >> insert_uk_covid19_scoring_report_to_postgres_table
