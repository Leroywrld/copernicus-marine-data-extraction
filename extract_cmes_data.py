import copernicusmarine
##import xarray as xr
##import rioxarray as rxr
import psycopg2
import os

#Database Connection
DB_NAME = "db_name"
DB_USER = "user"
DB_PASS = "db_password"
DB_HOST = "host"
DB_PORT = "port"

## a dictionary to assign each dataset's ID to a list of variables of interest
dataset_dict = {
    "cmems_mod_glo_bgc-nut_anfc_0.25deg_P1D-m": ["fe", "no3", "po4", "si"], ##nutrients
    "cmems_mod_glo_bgc-pft_anfc_0.25deg_P1D-m": ["chl", "phyc"], ##planktons
    "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m": ["so"], ##salinity
    "cmems_mod_glo_phy_anfc_0.083deg_P1D-m": ["zos"], ##sea surface height
    "cmems_mod_glo_phy_anfc_0.083deg-sst-anomaly_P1D-m": ["sea_surface_temperature_anomaly"] ##sea surface temperature
}

## a dictionary to assign each dataset ID with a user friendly name for purposes of file naming
dataset_name_dict = {
    "cmems_mod_glo_bgc-nut_anfc_0.25deg_P1D-m": "nutrients",
    "cmems_mod_glo_bgc-pft_anfc_0.25deg_P1D-m": "planktons",
    "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m": "salinity",
    "cmems_mod_glo_phy_anfc_0.083deg_P1D-m": "sea_surface_height",
    "cmems_mod_glo_phy_anfc_0.083deg-sst-anomaly_P1D-m": "sea_surface_temperature_anomaly"
}

## a dictionary containing all parameters necessary for subsetting from copernicusmarine
subsetting_dict = {
    'minimum_longitude': 39.0,
    'maximum_longitude': 45.0,
    'minimum_latitude': -5,
    'maximum_latitude': 1.25,
    'minimum_depth': 0.4940253794193268,
    'maximum_depth': 21.598816,
    'start_datetime': '2025/1/1',
    'end_datetime': '2025/1/2', ## you can change the start and end dates to subset data over long periods of time
    'output_directory": "C:\\Users\\Bill\\copernicus_data' ## output folder is specified here
}

## a function that generates informative file names necessary during file storage
def generate_filename(dataset_id):
    start_date = subsetting_dict["start_datetime"].replace("/", "")
    end_date = subsetting_dict["end_datetime"].replace("/", "")
    return f"{dataset_name_dict[dataset_id]}_kenyaeez_{start_date}_{end_date}.nc"

def download_and_insert():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()

    for dataset_id, variables in dataset_dict.items():
        filename = generate_filename(dataset_id)
        filepath = os.path.join(subsetting_dict["output_directory"], filename)

        print(f"📥 Downloading {dataset_name_dict[dataset_id]}...")
        copernicusmarine.subset(
            dataset_id=dataset_id,
            variables=variables,
            minimum_longitude=subsetting_dict["minimum_longitude"],
            maximum_longitude=subsetting_dict["maximum_longitude"],
            minimum_latitude=subsetting_dict["minimum_latitude"],
            maximum_latitude=subsetting_dict["maximum_latitude"],
            minimum_depth=subsetting_dict["minimum_depth"],
            maximum_depth=subsetting_dict["maximum_depth"],
            start_datetime=subsetting_dict["start_datetime"],
            end_datetime=subsetting_dict["end_datetime"],
            output_directory=subsetting_dict["output_directory"],
            output_filename=filename
        )

        print(f"Processing {filepath}...")
        ds = xr.open_dataset(filepath)
        df = ds.to_dataframe().reset_index()

        table_name = dataset_name_dict[dataset_id]

        for var in variables:
            for _, row in df.iterrows():
                cursor.execute(
                    f"""INSERT INTO {table_name} (time, depth, latitude, longitude, variable, value)
                        VALUES (%s, %s, %s, %s, %s, %s)""",
                    (row["time"], row.get("depth", 0), row["latitude"], row["longitude"], var, row[var])
                )

        conn.commit()
        print(f" Inserted {dataset_name_dict[dataset_id]} into {table_name} table.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    download_and_insert()
