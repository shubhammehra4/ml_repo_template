from sklearn.linear_model import LinearRegression
import pickle
from data_loader import get_wh_connection, fetch_data_from_selector_sql

def train(creds, inputs, output_filename):
    print("Training model")
    # Load the data
    wh_conn = get_wh_connection(creds)
    df1 = fetch_data_from_selector_sql(wh_conn, inputs[0])
    df2 = fetch_data_from_selector_sql(wh_conn, inputs[1])

    X = df1["num"].to_numpy()
    X = X.reshape(-1, 1)
    y = df2["num"].to_numpy()
    # Create a Linear Regression model object
    model = LinearRegression()

    # Train the model on the input data
    model.fit(X, y)

    # Save the model to a file
    pickle.dump(model, open(output_filename, 'wb'))