import json
import requests
from pathlib import Path
import io
import zipfile
from datetime import datetime, timezone

# ensure in future running data feed fetches from route
HOME_PATH = Path.cwd()
DATA_PATH = HOME_PATH / "data"
TORONTO_OPEN_DATA_CKAN_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
PACKAGE_SHOW_ACTION_URL = "/api/3/action/package_show"
PACKAGE_METADATA_FETCH = TORONTO_OPEN_DATA_CKAN_URL + PACKAGE_SHOW_ACTION_URL

def parse_utc_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)


def update_ttc_routes_schedules_metadata():
    cached_metadata = None
    metadata_cache_path = DATA_PATH / "package_metadata" / "ttc_gtfs_latest.json"

    # Always fetch latest package metadata
    params = {"id": "ttc-routes-and-schedules"}
    response = requests.get(PACKAGE_METADATA_FETCH, params=params)
    api_payload = response.json()

    if not api_payload["success"]:
        raise RuntimeError("[ERROR]: Extreme failure, unable to create GTFS metadata cache")

    fetched_metadata = api_payload["result"]
    minimal_metadata = {
            "name": fetched_metadata["name"],
            "id": fetched_metadata["id"],
            "metadata_modified": fetched_metadata["metadata_modified"],
        }

    try:
        with open(metadata_cache_path, "r") as f:
            cached_metadata = json.load(f)

    # Cache does not exist — create it
    except FileNotFoundError:
        with open(metadata_cache_path, "w") as f:
            json.dump(minimal_metadata, f, indent=2)
        return

    # Cache exists — compare timestamps
    cached_modified_ts = cached_metadata["metadata_modified"]
    remote_modified_ts = fetched_metadata["metadata_modified"]

    cached_modified_time = parse_utc_iso(cached_modified_ts)
    remote_modified_time = parse_utc_iso(remote_modified_ts)

    # Data is stale update it
    if remote_modified_time > cached_modified_time:
        with open(metadata_cache_path, "w") as f:
            json.dump(minimal_metadata, f, indent=2)

def get_ttc_routes_schedules():
    resources = []
    ttc_schedules_resource = None
    params = {"id": "ttc-routes-and-schedules"}
    ttc_routes_schedules_package = requests.get(PACKAGE_METADATA_FETCH, params=params).json()
    for _, resource in enumerate(ttc_routes_schedules_package["result"]["resources"]):
           # To get metadata for non datastore_active resources:
       if not resource["datastore_active"]:
           url = TORONTO_OPEN_DATA_CKAN_URL + "/api/3/action/resource_show?id=" + resource["id"]
           resource_metadata = requests.get(url).json()
           resources.append(resource_metadata)
           # From here, you can use the "url" attribute to download this file

    for r in resources:
        data = r["result"]
        if data["id"] == 'cfb6b2b8-6191-41e3-bda1-b175c51148cb' and data["format"] == 'ZIP':
            ttc_schedules_resource = requests.get(data['url']) # get the zip file
    
    bin_buffer = io.BytesIO()
    bin_buffer.write(ttc_schedules_resource.content)

    extract_dir_path = HOME_PATH / "static_gtfs_schedule_data"
    extract_dir_path.mkdir(exist_ok=True)

    with zipfile.ZipFile(bin_buffer) as myzip:
        for gtfs_file in myzip.namelist():
            with myzip.open(gtfs_file) as myfile:
                file_path = extract_dir_path / gtfs_file
                with file_path.open("wb") as f:
                    f.write(myfile.read())
        



# auto refresh data later based on latest modified attr of pkg metadata
# Requires a CKAN package in JSOn format
def assert_latest_package_data(package):
    pass

def testing():
    # Toronto Open Data API URL
    toronto_open_data_ckan_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

    # Latest NVAS package metadata fetch, ensures using latest resources

    # If this block returns NO data, not other error, then we have a CRITICAL discrepency between the docs, perhaps id has changed?
    params = {"id": "ttc-bustime-real-time-next-vehicle-arrival-nvas"}
    metadata_show_url = toronto_open_data_ckan_url + "/api/3/action/package_show"
    package_metadata = requests.get(metadata_show_url, params=params).json()
    # End critical failure

    # if the data and docs align and this metadata DOES exist ensure our records exist
    # check that ${id}_metadata.json exists -> if not make it 

    # Why the metadata JSON? --> ensures our URLs are up to date as best as possible
        # if the meta data has been modified AFTER our current records, we need to do 
            # 1) update our URL in the JSON file
            # 2) check current endpoints to see if they still work
            # if current endpoints don't work we need to alert or log failures

    # To get resource data:
    for _, resource in enumerate(package_metadata["result"]["resources"]):

        # To get actively updated data in the resource:
        if not resource["datastore_active"]:
            url = toronto_open_data_ckan_url + "/api/3/action/resource_show?id=" + resource["id"]
            resource_metadata = requests.get(url).json()

            # Print shows available data in the resource
            print(resource_metadata)

    # Resources ordered as [data, docs]
    nvas_base_url = package_metadata["result"]["resources"][0]["url"]

    # "vehicles" endpoint gives us real-time bus information
    # comes in protobuf format, ?debug outputs in human-readable string
    bus_data_url = nvas_base_url + "/vehicles?debug"

    real_time_bus_data = requests.get(bus_data_url)
    with open("curr_nvas.proto", "w", encoding='utf-8') as f:
        f.write(real_time_bus_data.text)

        # FLOW

        # if package doesnt exist then we're screwed can't expect any data

if __name__ == '__main__':
    update_ttc_routes_schedules_metadata()