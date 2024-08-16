import requests
import creds


url = "https://linkedin-data-api.p.rapidapi.com/"

querystring = {"username":"dave-birkbeck"}

headers = {
	"x-rapidapi-key": creds.api_key,
	"x-rapidapi-host": "linkedin-data-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())

# url = "https://linkedin-data-api.p.rapidapi.com/"

# querystring = {"username":"dave-birkbeck"}

# headers = {
#     "x-rapidapi-key": creds.api_key,
#     "x-rapidapi-host": "linkedin-data-api.p.rapidapi.com"
# }

# response = requests.get(url, headers=headers, params=querystring)

# print(response.json())
