import pymssql
import json
from datetime import datetime
def vir_env(host, login, password, schema, _query_string) -> None:
    
    # print('Import pymssql Done decorator')
    # print('Import pymssql Done with version:', pymssql.__version__)

    try:
        conn = pymssql.connect(
            host=host, 
            user=login, 
            password=password, 
            database=schema, 
            charset='UTF-8'
            # tds_version=r'7.0'
        )
        cursor_obj = conn.cursor()
        cursor_obj.execute(_query_string)
        data = cursor_obj.fetchall()
        cols = cursor_obj.description
        merge_data_with_column = list(
            map(
                lambda rec: {
                        v: rec[i] if not isinstance(rec[i], datetime) else rec[i].strftime('%Y-%m-%d %H:%M:%S') for i, v in enumerate(
                            list(
                                map(
                                    lambda f:f[0], cols
                                    )
                                )
                            )
                    } , data
                )
            )
        print(json.dumps(merge_data_with_column, ensure_ascii=False))
    except pymssql.OperationalError as e:
        return f"Connection failed: {e}"

vir_env(host='10.10.4.65', login='AirFlowSAPStagingPRD', password='Gzjdvj30betTrvgp', schema='spwg_staging', _query_string='''
            SELECT TOP 100 * FROM dbo.SAPCompany
        ''')