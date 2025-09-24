import copernicusmarine
##import xarray as xr
##import rioxarray as rxr

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
    'output_directory': 'C:\\Users\\ADMIN\\marine\\cmes_extraction\\data' ## output folder is specified here
}

## a function that generates informative file names necessary during file storage
def generate_filename(dataset_name_dict:dict, dataset_id:str, start_date:str, end_date:str):
    start_date = start_date.replace('/', '')
    end_date = end_date.replace('/', '')
    filename = "{0}_kenyaeez_{1}_{2}".format(dataset_name_dict[dataset_id], start_date, end_date)
    return filename

## a loop that iterates over the keys (dataset IDs) and values (variable lists) to subset data
## from copernicusmarine accordingly for each dataset ID
for dataset_id, variable_list in dataset_dict.items():
    filename = generate_filename(dataset_name_dict=dataset_name_dict, dataset_id=dataset_id, start_date=subsetting_dict['start_datetime'], end_date=subsetting_dict['end_datetime'])
    copernicusmarine.subset(dataset_id=dataset_id,
                            variables=variable_list,
                            minimum_longitude=subsetting_dict['minimum_longitude'],
                            maximum_longitude=subsetting_dict['maximum_longitude'],
                            minimum_latitude=subsetting_dict['minimum_latitude'],
                            maximum_latitude=subsetting_dict['maximum_latitude'],
                            minimum_depth=subsetting_dict['minimum_depth'],
                            maximum_depth=subsetting_dict['maximum_depth'],
                            start_datetime=subsetting_dict['start_datetime'],
                            end_datetime=subsetting_dict['end_datetime'],
                            output_directory=subsetting_dict['output_directory'],
                            output_filename=filename,
                            dry_run=True ##if set to False the data will download to specified folder
                            )