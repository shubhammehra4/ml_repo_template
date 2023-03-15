from rudderlabs.data.apps.wh import ProfilesConnector

def get_wh_connection(warehouse_creds, aws_config = {}):
    """
    Get connection to wh
    """
    return ProfilesConnector(warehouse_creds, aws_config=aws_config)

def fetch_data_from_selector_sql(wh_conn, selectorSql):
    """
    Fetch data from wh using the selector sql
    """
    if selectorSql["setup"] != "":
        wh_conn.connection.execute(selectorSql["setup"])

    df = wh_conn.run_query(selectorSql["select"])
    
    if selectorSql["cleanup"] != "":
        wh_conn.connection.execute(selectorSql["cleanup"])
 
    return df


def create_table_from_query(wh_conn, query):
    """
    Create table in wh using the query
    """
    wh_conn.run_query(query)

