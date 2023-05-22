
from azure.storage.blob import BlobServiceClient

from google.cloud import storage
from google.oauth2 import service_account

from inditex_anomaly_detector.utils.secret_utils import get_dbutils
from inditex_anomaly_detector.utils.logging_debugging import common_print, start_chrono, end_chrono



def fetch_config(config_file: str,
                 container: str,
                 storage_user: str,
                 storage_pass: str,
                 storage_url: str,
                 TMP_FILES_PATH: str) -> str:

    start = start_chrono()

    path = f'{TMP_FILES_PATH}{config_file}'
    path=path.replace("dev/","").replace("pro/","")
    common_print("path", path)

    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_user};AccountKey={storage_pass};EndpointSuffix={storage_url}"
    common_print("cs", connection_string)
    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string)
    common_print("ainf", blob_service_client.get_account_information())


    common_print("co", container)
    common_print("cf", config_file)
    blob_client = blob_service_client.get_blob_client(
        container=container, blob=config_file)

    with open(path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    end_chrono(config_file, start)

    return path



def upload_plot_azure( plot_file: str,
                 container: str,
                 storage_user: str,
                 storage_pass: str,
                 storage_url: str,
                 TMP_FILES_PATH: str) -> str:

    start = start_chrono()

    path = f'{TMP_FILES_PATH}{plot_file}'

    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_user};AccountKey={storage_pass};EndpointSuffix={storage_url}"
    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string)

    blob_client = blob_service_client.get_blob_client(container=container, blob=plot_file)

    with open(plot_file, "wb") as download_file:
        blob_client.upload_blob(data=plot_file, overwrite=True, encoding='utf-8')

    end_chrono('Upload plot file', start)

    return path



def upload_plot_google(  plot_file_local_system,
                         plot_file_name,
                         bucket_name,
                         project_id,
                         credentials_dict):

    start = start_chrono()


    try:
        key_dict = {
            "type": get_dbutils().secrets.get(scope=credentials_dict['keyvault'], key=credentials_dict['type']),
            "project_id": get_dbutils().secrets.get(scope=credentials_dict['keyvault'], key=credentials_dict['project_id']),
            "private_key_id": get_dbutils().secrets.get(scope=credentials_dict['keyvault'], key=credentials_dict['private_key_id']),
            "private_key": get_dbutils().secrets.get(scope=credentials_dict['keyvault'], key=credentials_dict['private_key']).replace('=', '\n'),
            "client_email": get_dbutils().secrets.get(scope=credentials_dict['keyvault'], key=credentials_dict['client_email']),
            "client_id": get_dbutils().secrets.get(scope=credentials_dict['keyvault'], key=credentials_dict['client_id']),
            "auth_uri": get_dbutils().secrets.get(scope=credentials_dict['keyvault'], key=credentials_dict['auth_uri']),
            "token_uri": get_dbutils().secrets.get(scope=credentials_dict['keyvault'], key=credentials_dict['token_uri']),
            "auth_provider_x509_cert_url": get_dbutils().secrets.get(scope=credentials_dict['keyvault'], key=credentials_dict['auth_cert']),
            "client_x509_cert_url": get_dbutils().secrets.get(scope=credentials_dict['keyvault'], key=credentials_dict['cert_url'])
        }
    except:
        key_dict = {
            "type": "service_account",
            "project_id": "zaracom-datalake",
            "private_key_id": "b9fc0e74cd27c43745c0f16b512342c17c1141d1",
            "private_key": "-----BEGIN PRIVATE KEY-----=MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0yU9UbUEFb8nv=hIgDOJKnlgUOCrKfoC7F0d/RmybcYYJbgTQdsjP3hjdCrfX8i9wJVvDyEJb50lc6=p+dyZLQsPuhxPnV+/aZ+BM+fcdEjzXTl+zJv44Hch9vxl/5AfxryizgjxjcM8kpC=rejiVQoJLVXccYz0SSkrGoz6wk1aCYe3w+y6bMJF8eiIVO5qDSx3NL4alTf5/m9d=6nFsMM5NkL+TgPJIt8mliOtirt0HpU/bhpXIvKvHbo2OySMPp9iAS57wdAib/oZv=G0mvL9yhxaJbvFpPAE2YYcTbPzZwVg8hYcbrUEd00rFdsm1kFbxo6qVDe7hnS19M=LpYDkkGnAgMBAAECggEAJEukUyFqI/LJGn7Dpc3mV6H9Ws/YA8cvjMSxiE37c9RZ=zRA4C9w6pa+7CXaOg8j6gJ+FWTvua7KIk7yg8Wz5ZUat+QpeoYPAON2zZ2H86JC3=rvKLMj59VG6sQClRQNaj7Nz5hOknJUDBmvh6YHJNRQFmkw7zCxpjGwXGFxQKXpBi=rttofmvvGuQqh25xqQMDcVON+9iOkNhY6WUIVr1zuhSDH3IuBaYtXLuh9nJ6l1nd=sip8p/TYe4ogVq9c7lPf2gSTQWTDFi8FNzA2BuR7HceQuf7fB3FGTijpUN8Bjxwx=vS/Ck5doztCAbAUpaHheiBRgavLgJGVwHPqCND5KPQKBgQD93Rrm66Xmzhv6EN/z=q2n827cuiWKKBf+oSc6fLZ5a5OmK6cc5b4ZzWw3gy2J/wG4OyGpplBIluwsqUtuM=IOeDdkopP1gsUtPffSqzb/fNtB/Step8w15v0z0g28ENp+8m4KFMXABGbrL2TDJu=qhO9CyMXMtjY4AXcY4SSQdw4+wKBgQC2TsZ+GfLeGSAe+s8SRz0eG/tXclRNXOCs=XqNhZpGzzzk+LZYqcHQ9AkSCr2p2hEV8W7Vvhs/QOBhngiwSd+8IwvsaEkZQrRfy=U1dDzDOD2Ps0uTdSP+k86f/ZQ7/99OTuMwF84McKlR003bvus5dqETW81RBucQL6=uV9drhvSRQKBgQC5mHF3i6ZhhCPuAXJJafN1uohEzIovEE2lkjguJaLVHvAi7+EH=+6IXKeWOiAL+FQghZKJvh6Jw1TtHeQYrf07izJNRbo8e8HsXtCGIx/4p9FsjqH3D=Mu4S4SOapy6PtkvgZ29lLQnCdGIAMRrZzxvZJJBGAJWKkNWYF2Wvq9i+CwKBgBfH=RpSEBe16EXp49DFKIWqf87SAMQa2KofRADZgDnkJeknl4ERAzqh2d0EkaBp1Piru=O0gEqW5bIrO9gsoV4pOd+up3n7w+F3V/8U3igIWd84X48oJD28QaMUjBUXVqT8HR=3UvQdaWntVuD4FNOEV3ASOYMVExTaTQSoaGyZ+aRAoGBANu8nbkHpVBykGj3oAjE=3CBCwnxjmBp0W25niPKdkS+w/1YVulUccPFMYqZA4bUv0EnWvT4zGHERn3RfVGz1=PuPzXk2CGInn3R0reEZNJ0FDLrlxXd4Z5Um7nx6QUDKPEXw86PZzbUQTXyK1N3Ch=a0GlhwaqYTVCVhsbSb1HcLCm=-----END PRIVATE KEY-----=".replace('=', '\n'),
            "client_email": "zaracom-stock-alerts@zaracom-datalake.iam.gserviceaccount.com",
            "client_id": "117441082680421944710",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/zaracom-stock-alerts%40zaracom-datalake.iam.gserviceaccount.com"
        }
    creds_dicti = service_account.Credentials.from_service_account_info(key_dict)
    client = storage.Client(credentials=creds_dicti)
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(plot_file_name)
    s=blob.upload_from_filename(plot_file_local_system)

    end_chrono('Upload plot file', start)