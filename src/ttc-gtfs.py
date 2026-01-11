import json
import requests
from pathlib import Path
import io
import zipfile
from datetime import datetime, timezone

# ensure in future running data feed fetches from route
HOME_PATH = Path.cwd()
DATA_PATH = HOME_PATH / "data"
METADATA_PATH = DATA_PATH / "metadata"
TORONTO_OPEN_DATA_CKAN_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
PACKAGE_SHOW_ACTION_URL = "/api/3/action/package_show"
PACKAGE_METADATA_FETCH = TORONTO_OPEN_DATA_CKAN_URL + PACKAGE_SHOW_ACTION_URL
VALID_MODES = ['package', 'resource']

# Data Service

# Why do I need this? --> staleness check!
# return type tbd? think i should return the entire object
def get_cached_metadata(mode: str, filename: str):
    if mode not in VALID_MODES:
        raise RuntimeError(f"[ERROR] Invalid mode: ${mode}")

    path = METADATA_PATH / mode / filename
    metadata = None

    try:
        with open(path, 'r') as f:
            metadata = json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] File: ${filename} not found")
    return metadata

# What scenarios do I have to fetch data?
  # staleness check, temp data to compare -> return package as a whole
  # mutator might need to overwrite:
    # overwrite package data -> return package as a whole
    # overwrite resource data -> return resource["id"] in package
    # if pkg data stale though -> overwrite both

# If persistent data is fresh, I can rely on it to fuel my fetches
# still for fetches whether we update package or resource we need the package
def fetch_remote_metadata(cached_data):
    params = {"id": cached_data["id"]}
    raw_remote_metadata = requests.get(PACKAGE_METADATA_FETCH, params=params).json()

    if not raw_remote_metadata["success"]:
        # raise some error -> orchestration can catch and alert of inaccuracy on site
        print('fetch failed smth wrong with fetch')

    sanitized_remote_metadata = raw_remote_metadata["result"]
    return sanitized_remote_metadata

# Data Mutator

# Future needs:
# Actual data mutator? (NVAS protobuf, GTFS txts) -> later

# Package OR Resource writer dependent on mode
def write_metadata(mode: str, package_data, resource_id=None):
  if mode not in VALID_MODES:
      raise ValueError(f"[ERROR] Invalid mode: ${mode}")
  if mode == 'resource' and not resource_id:
      raise ValueError(f"[ERROR] Resource mode with invalid resource_id: ${resource_id}")

  new_entry_metadata = None
  path = METADATA_PATH / mode
  id = ''
  if mode == 'resource':
      id = resource_id
      resources = None
      try:
          resources = package_data["resources"]
      except KeyError as e:
        raise KeyError("[ERROR] Package data missing 'resources' field") from e
      
      for r in resources:
          if r["id"] == resource_id:
              new_entry_metadata = r
              break
      if not new_entry_metadata:
          raise ValueError(f"[ERROR] No resource exists of ID: ${resource_id}")
  else:
      id = package_data["id"]
      new_entry_metadata = package_data
  
  filename = f"${id}.json"
  write_path = path / filename

  with open(write_path, "w") as f:
      json.dump(new_entry_metadata, f, indent=2)

  # Does orchestration need return?
  return True

def parse_utc_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)

def write_ttc_gtfs_metadata(metadata):
    metadata_cache_path = DATA_PATH / "package_metadata" / "ttc_gtfs_latest.json"
    with open(metadata_cache_path, "w") as f:
            json.dump(metadata, f, indent=2)


def get_ttc_routes_schedules_metadata():
    # Always fetch latest package metadata
    params = {"id": "ttc-routes-and-schedules"}
    response = requests.get(PACKAGE_METADATA_FETCH, params=params)
    api_payload = response.json()
    if not api_payload["success"]:
        raise RuntimeError("[ERROR]: Extreme failure, unable to create GTFS metadata cache")

    metadata = api_payload["result"]
    return metadata

def check_ttc_routes_schedules_metadata() -> dict:
    cached_metadata : dict = None
    is_stale_metadata : bool = None
    is_stale_route_data : bool = None
    metadata_cache_path = DATA_PATH / "package_metadata" / "ttc_gtfs_latest.json"
    fetched_metadata = get_ttc_routes_schedules_metadata() 

    # Slice required data
    minimal_metadata = {
        "name": fetched_metadata["name"],
        "id": fetched_metadata["id"],
        "metadata_modified": fetched_metadata["metadata_modified"],
        "last_refreshed": fetched_metadata["last_refreshed"]
    }

    try:
        with open(metadata_cache_path, "r") as f:
            cached_metadata = json.load(f)

    # Cache does not exist — create it
    except FileNotFoundError:
        write_ttc_gtfs_metadata(minimal_metadata)
        # Pass along information and stale record
        is_stale_metadata = True
        is_stale_route_data = True
        minimal_metadata["stale_metadata"] = is_stale_metadata
        minimal_metadata["stale_route_data"] = is_stale_route_data

        return minimal_metadata

    # Cache exists — compare timestamps
    cached_metadata_update_ts = cached_metadata["metadata_modified"]
    remote_metadata_update_ts = fetched_metadata["metadata_modified"]

    cached_dataset_update_ts = cached_metadata["last_refreshed"]
    remote_dataset_update_ts = fetched_metadata["last_refreshed"]

    # Time converted values for comparison
    cached_metadata_update_time = parse_utc_iso(cached_metadata_update_ts)
    remote_metadata_update_time = parse_utc_iso(remote_metadata_update_ts)

    cached_dataset_update_time = parse_utc_iso(cached_dataset_update_ts)
    remote_dataset_update_time = parse_utc_iso(remote_dataset_update_ts)

    # If package metadata OR actual dataset has been modified since last record, update record
    is_stale_metadata = remote_metadata_update_time > cached_metadata_update_time 
    is_stale_route_data = remote_dataset_update_time > cached_dataset_update_time

    # Stale metadata requires refresh of ALL data
    if is_stale_metadata:
        write_ttc_gtfs_metadata(minimal_metadata)

        is_stale_route_data = is_stale_metadata
        minimal_metadata["stale_metadata"] = is_stale_metadata
        minimal_metadata["stale_route_data"] = is_stale_route_data
        return minimal_metadata

    minimal_metadata["stale_metadata"] = is_stale_metadata
    minimal_metadata["stale_route_data"] = is_stale_route_data

    return minimal_metadata

def update_ttc_routes_schedules_data(metadata):
    is_stale_metadata = metadata["stale_metadata"]
    is_stale_route_data = metadata["stale_route_data"]
    
    should_update_data = is_stale_metadata or is_stale_route_data

    # must update data if MD is stale
    if should_update_data:
        get_ttc_routes_schedules()

def get_ttc_routes_schedules():
    resources = []
    ttc_schedules_resource = None
    params = {"id": "ttc-routes-and-schedules"}
    ttc_routes_schedules_package = requests.get(PACKAGE_METADATA_FETCH, params=params).json()
    with open('test.json', 'w') as f:
        json.dump(ttc_routes_schedules_package, f, indent=2)
    return 
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
        
def sim_cron():

    # what does the data look like?
        # not a checker its a data reader
        # Data Reader
    # is anything stale?
        # don't read decide, based on read data from above, signal staleness
        # Validity/Staleness service
    # refresh stale data
        # the refreshing service, recieve signal and act
        # Data Mutator

    is_stale_metadata : bool  = None
    try:
        is_stale_metadata = check_ttc_routes_schedules_metadata()
    except RuntimeError:
        print('do something about this, cron sleeps')
    
    # Try to save requests by pruning flow
    # if MD is stale MUST update the resource
    # if not we can just CHECK resource

    # hit this case if MD DNE or MD is stale
    if is_stale_metadata:
        get_ttc_routes_schedules()
        return # cron ends
    else:
        pass
    # also except FNF

if __name__ == '__main__':
    get_ttc_routes_schedules()