import requests
import json

data = requests.get(f'https://api.nhle.com/stats/rest/en/skater/bios?limit=-1&cayenneExp=seasonId=20232024')

if data.status_code == 200:
        

    f_data = json.dumps(data.json(), indent=4)
    with open("data.json", "w") as outfile:
        outfile.write(f_data)

    print("Data collected and saved successfully.")
        
else:
    print(f"There was an error with the API call, no data created (error: {data.status_code})")