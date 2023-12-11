import duckdb
import pandas as pd

def create_table(conn):
    conn.execute('''
        CREATE TABLE electric_cars (
            VIN STRING,
            County STRING,
            City STRING,
            State STRING,
            "Postal Code" INT,
            "Model Year" INT,
            Make STRING,
            Model STRING,
            "Electric Vehicle Type" STRING,
            "Clean Alternative Fuel Vehicle (CAFV) Eligibility" STRING,
            "Electric Range" INT,
            "Base MSRP" INT,
            "Legislative District" INT,
            "DOL Vehicle ID" INT,
            "Vehicle Location" STRING,
            "Electric Utility" STRING,
            "2020 Census Tract" STRING
        )
    ''')


def import_data(conn, file_path):
    conn.execute(f"COPY electric_cars FROM '{file_path}' (HEADER TRUE, DELIMITER ',')")

def count_cars_per_city(conn):
    result = conn.execute('SELECT City, COUNT(*) AS Car_Count FROM electric_cars GROUP BY City').fetchall()
    return pd.DataFrame(result, columns=['City', 'Car_Count'])

def top_3_models(conn):
    result = conn.execute('''
        SELECT Model, COUNT(*) AS Car_Count
        FROM electric_cars
        GROUP BY Model
        ORDER BY Car_Count DESC
        LIMIT 3
    ''').fetchall()
    return pd.DataFrame(result, columns=['Model', 'Car_Count'])

def popular_car_per_postal_code(conn):
    result = conn.execute('''
        SELECT "Postal Code", Model, COUNT(*) AS Car_Count
        FROM electric_cars
        GROUP BY "Postal Code", Model
        ORDER BY Car_Count DESC
    ''').fetchall()
    result_df = pd.DataFrame(result, columns=['Postal_Code', 'Model', 'Car_Count'])
    result_df = result_df.groupby('Postal_Code').first().reset_index()
    return result_df

def count_cars_per_year(conn):
    result = conn.execute('SELECT "Model Year", COUNT(*) AS Car_Count FROM electric_cars GROUP BY "Model Year"').fetchall()
    return pd.DataFrame(result, columns=['Model_Year', 'Car_Count'])

def write_parquet_files(result_df, output_folder):
    for index, row in result_df.iterrows():
        year = row['Model_Year']
        year_df = result_df[result_df['Model_Year'] == year]
        year_df.to_parquet(f'{output_folder}/cars_{year}.parquet', index=False)

def main():
    conn = duckdb.connect(database=':memory:', read_only=False)
    create_table(conn)
    import_data(conn, 'data/Electric_Vehicle_Population_Data.csv')

    cars_per_city = count_cars_per_city(conn)
    top_3_models_data = top_3_models(conn)
    popular_car_per_postal_code_data = popular_car_per_postal_code(conn)
    cars_per_year = count_cars_per_year(conn)
    print(cars_per_year)
    print(popular_car_per_postal_code_data)
    print(top_3_models_data)
    print(cars_per_city)
    write_parquet_files(cars_per_year, 'output')
    
if __name__ == "__main__":
    main()
