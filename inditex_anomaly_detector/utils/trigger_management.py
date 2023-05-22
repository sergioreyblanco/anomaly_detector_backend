import requests
from inditex_anomaly_detector.utils.logging_debugging import common_print



def get_refresh_token(tenant, client_id, client_secret):

    url_reftok = "https://login.microsoftonline.com/"+tenant+"/oauth2/token"
    payload_reftok='grant_type=client_credentials&resource=https%3A%2F%2Fmanagement.azure.com%2F&client_id='+client_id+'&client_secret='+client_secret
    headers_reftok = {
    'content-Type': 'application/x-www-form-urlencoded'
    }
    response_reftok = requests.request("GET", url_reftok, headers=headers_reftok, data=payload_reftok)
    common_print("get refresh token token", response_reftok.text)
    return response_reftok.json()['access_token']



def start_trigger(token, subscription_id, resource_group, datafactory, trigger):

    url_starttrigger = "https://management.azure.com/subscriptions/"+subscription_id+"/resourceGroups/"+resource_group+"/providers/Microsoft.DataFactory/factories/"+datafactory+"/triggers/"+trigger+"/start?api-version=2018-06-01"
    payload_starttrigger={}
    headers_starttrigger = {
    'Authorization': 'Bearer '+token
    }
    response = requests.request("POST", url_starttrigger, headers=headers_starttrigger, data=payload_starttrigger)
    common_print("start trigger response", response.text)



def stop_trigger(token, subscription_id, resource_group, datafactory, trigger):

    url_starttrigger = "https://management.azure.com/subscriptions/"+subscription_id+"/resourceGroups/"+resource_group+"/providers/Microsoft.DataFactory/factories/"+datafactory+"/triggers/"+trigger+"/stop?api-version=2018-06-01"
    payload_starttrigger={}
    headers_starttrigger = {
    'Authorization': 'Bearer '+token
    }
    response = requests.request("POST", url_starttrigger, headers=headers_starttrigger, data=payload_starttrigger)
    common_print("stop trigger response", response.text)