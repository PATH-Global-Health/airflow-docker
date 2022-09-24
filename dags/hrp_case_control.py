from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.models import Variable
from macepa_plugin import MultiEndpointOperator, HTTPPayloadSplitOperator
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['bserda@path.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('hrp_case_control', default_args=default_args,
          description='Read HRP case control data from ODK Central, process and push it to DHIS2',
          schedule_interval='0 19 * * *')

var_conf = Variable.get("hrp_odk_dhis2_vars", deserialize_json=True)
ODK_CENTRAL_ENDPOINTS = var_conf["ODK_CENTRAL_ENDPOINT"]
DHIS2_DATA_VALUES_ENDPOINT = var_conf["DHIS2_DATA_VALUES_ENDPOINT"]
dataelements = var_conf["dataelements"]
attribute_option_combo = var_conf["attribute_option_combo"]
orgunits = var_conf["orgunits"]

read_hrp_case_control_task = MultiEndpointOperator(
    task_id="read_hrp_case_control",
    method='GET',
    endpoints=ODK_CENTRAL_ENDPOINTS.keys(),
    headers={"Content-Type": "application/json"},
    http_conn_id='odk_central',
    endpoints_from_upstream=False,
    dag=dag
)

def hrp_case_control_to_json(**kwargs):
    import json

    data = kwargs['task_instance'].xcom_pull(task_ids='read_hrp_case_control')

    final_data = []
    for d in data:
        for k, v in d.items():
            for row in json.loads(v.decode())['value']:
                # add the source form where we downloaded the data from and spread the sections
                r={'SOURCE_FORM': k, **row['s1'], **row['s2'], **row['s3'], **row['s4'], **row['s6'], **row['s7'] }
                if 's5' in row:
                    r.update(**row['s5'])
                final_data.append(r)

    return final_data


hrp_case_control_to_json_task = PythonOperator(
    task_id='hrp_case_control_to_json',
    dag=dag,
    python_callable=hrp_case_control_to_json,
    provide_context=True,
)

def hrp_case_control_to_pandas(**kwargs):
    import pandas as pd
    data = kwargs['task_instance'].xcom_pull(task_ids='hrp_case_control_to_json')
    return pd.DataFrame.from_dict(data)


hrp_case_control_to_pandas_task = PythonOperator(
    task_id='hrp_case_control_to_pandas',
    dag=dag,
    python_callable=hrp_case_control_to_pandas,
    provide_context=True,
)

def generate_report(**kwargs):
    import pandas as pd
    import numpy as np
    import json
    import re

    case_control_df = kwargs['task_instance'].xcom_pull(task_ids='hrp_case_control_to_pandas')
    

    if case_control_df.empty:  # if there is no data, return empty data value
        return json.dumps({"dataValues": []})
    
    # fill NaN with 0 and change data type to int for aggregation
    case_control_df["consent_granted"] = case_control_df["consent_granted"].fillna(0).astype(int)
    case_control_df["participant_age"] = case_control_df["participant_age"].fillna(0).astype(int)
    case_control_df["participant_gender"] = case_control_df["participant_gender"].fillna(0).astype(int)
    case_control_df["case_control"] = case_control_df["case_control"].fillna(0).astype(int)
    
    data_values = []

    # generate case control report
    case_control_df['0-4-Male-Case'] = np.where((case_control_df['participant_age'] >= 0) & (case_control_df['participant_age'] < 5) & (case_control_df['participant_gender'] == 1) & (case_control_df['case_control'] == 1), 1, 0)
    case_control_df['0-4-Female-Case'] = np.where((case_control_df['participant_age'] >= 0) & (case_control_df['participant_age'] < 5) & (case_control_df['participant_gender'] == 2) & (case_control_df['case_control'] == 1), 1, 0)
    case_control_df['0-4-Male-Control'] = np.where((case_control_df['participant_age'] >= 0) & (case_control_df['participant_age'] < 5) & (case_control_df['participant_gender'] == 1) & (case_control_df['case_control'] == 2), 1, 0)
    case_control_df['0-4-Female-Control'] = np.where((case_control_df['participant_age'] >= 0) & (case_control_df['participant_age'] < 5) & (case_control_df['participant_gender'] == 2) & (case_control_df['case_control'] == 2), 1, 0)

    case_control_df['5-14-Male-Case'] = np.where((case_control_df['participant_age'] >= 5) & (case_control_df['participant_age'] < 15) & (case_control_df['participant_gender'] == 1) & (case_control_df['case_control'] == 1), 1, 0)
    case_control_df['5-14-Female-Case'] = np.where((case_control_df['participant_age'] >= 5) & (case_control_df['participant_age'] < 15) & (case_control_df['participant_gender'] == 2) & (case_control_df['case_control'] == 1), 1, 0)
    case_control_df['5-14-Male-Control'] = np.where((case_control_df['participant_age'] >= 5) & (case_control_df['participant_age'] < 15) & (case_control_df['participant_gender'] == 1) & (case_control_df['case_control'] == 2), 1, 0)
    case_control_df['5-14-Female-Control'] = np.where((case_control_df['participant_age'] >= 5) & (case_control_df['participant_age'] < 15) & (case_control_df['participant_gender'] == 2) & (case_control_df['case_control'] == 2), 1, 0)

    case_control_df['15-24-Male-Case'] = np.where((case_control_df['participant_age'] >= 15) & (case_control_df['participant_age'] < 25) & (case_control_df['participant_gender'] == 1) & (case_control_df['case_control'] == 1), 1, 0)
    case_control_df['15-24-Female-Case'] = np.where((case_control_df['participant_age'] >= 15) & (case_control_df['participant_age'] < 25) & (case_control_df['participant_gender'] == 2) & (case_control_df['case_control'] == 1), 1, 0)
    case_control_df['15-24-Male-Control'] = np.where((case_control_df['participant_age'] >= 15) & (case_control_df['participant_age'] < 25) & (case_control_df['participant_gender'] == 1) & (case_control_df['case_control'] == 2), 1, 0)
    case_control_df['15-24-Female-Control'] = np.where((case_control_df['participant_age'] >= 15) & (case_control_df['participant_age'] < 25) & (case_control_df['participant_gender'] == 2) & (case_control_df['case_control'] == 2), 1, 0)

    case_control_df['25-49-Male-Case'] = np.where((case_control_df['participant_age'] >= 25) & (case_control_df['participant_age'] < 50) & (case_control_df['participant_gender'] == 1) & (case_control_df['case_control'] == 1), 1, 0)
    case_control_df['25-49-Female-Case'] = np.where((case_control_df['participant_age'] >= 25) & (case_control_df['participant_age'] < 50) & (case_control_df['participant_gender'] == 2) & (case_control_df['case_control'] == 1), 1, 0)
    case_control_df['25-49-Male-Control'] = np.where((case_control_df['participant_age'] >= 25) & (case_control_df['participant_age'] < 50) & (case_control_df['participant_gender'] == 1) & (case_control_df['case_control'] == 2), 1, 0)
    case_control_df['25-49-Female-Control'] = np.where((case_control_df['participant_age'] >= 25) & (case_control_df['participant_age'] < 50) & (case_control_df['participant_gender'] == 2) & (case_control_df['case_control'] == 2), 1, 0)
    
    case_control_df['>=50-Male-Case'] = np.where((case_control_df['participant_age'] >= 50) & (case_control_df['participant_gender'] == 1) & (case_control_df['case_control'] == 1), 1, 0)
    case_control_df['>=50-Female-Case'] = np.where((case_control_df['participant_age'] >= 50) & (case_control_df['participant_gender'] == 2) & (case_control_df['case_control'] == 1), 1, 0)
    case_control_df['>=50-Male-Control'] = np.where((case_control_df['participant_age'] >= 50) & (case_control_df['participant_gender'] == 1) & (case_control_df['case_control'] == 2), 1, 0)
    case_control_df['>=50-Female-Control'] = np.where((case_control_df['participant_age'] >= 50) & (case_control_df['participant_gender'] == 2) & (case_control_df['case_control'] == 2), 1, 0)
    

    # group by survey date and health facilities then aggregate the result
    case_control_sum = case_control_df.groupby(["interview_date", "health_facility"]).sum()

    for group_name in case_control_sum.index:
        for de in dataelements:
            for index,cc in de["category_option_combo"].items():
                val = {"categoryOptionCombo": cc, "attributeOptionCombo": attribute_option_combo,
                       "period": group_name[0].replace("-", ""), "orgUnit": orgunits[str(group_name[1])],
                       "dataElement": de['dataElement'], "value": int(case_control_sum.loc[group_name][index])}
                data_values.append(val)

    return {"dataValues": data_values}


generate_report_task = PythonOperator(
    task_id='generate_report',
    dag=dag,
    python_callable=generate_report,
    provide_context=True,
)

sync_to_dhis2_task = HTTPPayloadSplitOperator(
    task_id='sync_to_dhis2',
    http_conn_id='hrp_dhis2',
    method='POST',
    endpoint=DHIS2_DATA_VALUES_ENDPOINT,
    payload_size=30,
    data="{{ task_instance.xcom_pull(task_ids='generate_report') }}",
    headers={"Accept": "application/json", "Content-Type": "application/json"},
    response_check=lambda response: True if response.status_code == 200 else False,
    dag=dag)


read_hrp_case_control_task >> hrp_case_control_to_json_task >> hrp_case_control_to_pandas_task >> generate_report_task >> sync_to_dhis2_task
