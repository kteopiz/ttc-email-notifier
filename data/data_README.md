# Overview of Data Usage

## Overview

This project uses a combination of data from **Toronto Open Data**, publically available data from the City of Toronto. This data is stored in a **CKAN instance**.

[CKAN API docs](https://docs.ckan.org/en/latest/api/)

CKAN **packages** store metadata about a specific dataset. These packages contain **resources** which is the actual data.

Currently these are the packages that are used to supply data to this application:
| Package Name | Package ID | Docs | Update Frequency |
| ----------- | ----------- | ----------- | ----------- |
| TTC BusTime Real-Time Next Vehicle Arrival (NVAS) | ttc-bustime-real-time-next-vehicle-arrival-nvas | https://open.toronto.ca/dataset/ttc-bustime-real-time-next-vehicle-arrival-nvas/ | Real-time |

---

## Data Usage Examples

### Real-time NVAS package and resource fetch

Shows the request process from package metadata to actual data

- [Shortcut to data page](https://bustime.ttc.ca/gtfsrt/)
- [Real-time vehicle position data](https://bustime.ttc.ca/gtfsrt/vehicles?debug)

```python
import json
import requests

# Toronto Open Data API URL
base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

# NVAS package metadata fetch
params = {"id": "ttc-bustime-real-time-next-vehicle-arrival-nvas"}
metadata_show_url = base_url + "/api/3/action/package_show"
package_metadata = requests.get(metadata_show_url, params=params).json()

with open("EXAMPLE_nvas-package-data.txt", "w") as json_file:
    json.dump(package_metadata, json_file, indent=4)

# To get resource data:
for idx, resource in enumerate(package_metadata["result"]["resources"]):

    # To get actively updated data in the resource:
    if not resource["datastore_active"]:
        url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
        resource_metadata = requests.get(url).json()

        # Print shows available data in the resource
        # Shows first resource is data, second is docs
        print(resource_metadata)

nvas_base_url = package_metadata["result"]["resources"][0]["url"]

# "vehicles" endpoint gives us real-time bus information
# comes in protobuf format, ?debug outputs in human-readable string
bus_data_url = nvas_base_url + "/vehicles?debug"

real_time_bus_data = requests.get(bus_data_url)
with open("EXAMPLE_nvas_rt_bus_data.txt", "w", encoding='utf-8') as f:
    f.write(real_time_bus_data.text)
```
