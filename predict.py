import pickle
import pandas as pd
from data_loader import get_wh_connection, fetch_data_from_selector_sql

def get_unquoted_schema(df, name, schema):
    schema = pd.io.sql.get_schema(df, name, schema=schema)
    return schema.replace('"', '').replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')


def predict(creds, aws_config, model_path, inputs, output_tablename):
    print("Predicting with model: " + model_path)
    # Load inputs
    wh_conn = get_wh_connection(creds, aws_config)
    df = fetch_data_from_selector_sql(wh_conn, inputs[0])

    x = df["num"].to_numpy()
    x = x.reshape(-1, 1)
    # Load the model
    model = pickle.load(open(model_path, 'rb'))
    
    # Predict using the trained model
    prediction = model.predict(x)
    result = pd.DataFrame(prediction, columns=["Result"])

    # Create the output table
    create_statement = get_unquoted_schema(result, output_tablename, creds["schema"])

    wh_conn.connection.autocommit = True
    wh_conn.connection.execute(create_statement)
    
    # Write the prediction to a table
    wh_conn.write_to_table(result, output_tablename, creds["schema"])
