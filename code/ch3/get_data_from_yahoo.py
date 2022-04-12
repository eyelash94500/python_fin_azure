import requests

address = "https://query1.finance.yahoo.com/v8/finance/chart/2317.TW?period1=0&period2=1599926400&interval=1d&events=history&=hP2rOschxO0"

response = requests.get(address, headers={"User-agent": ""})
print(response.text)
