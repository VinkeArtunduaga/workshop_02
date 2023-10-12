#Conexion a drive

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

credentials = '/home/vinke1302/Apache/credenciales_drive.json'  

def login():
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials)

    if gauth.access_token_expired:
        gauth.Refresh()
        gauth.SaveCredentialsFile(credentials)
    else:
        gauth.Authorize()

    credentials_drive = GoogleDrive(gauth)
    return credentials_drive

def upload_csv(path, id_folder):
    credentials_drive = login()
    archivo = credentials_drive.CreateFile({'parents': [{'kind': 'drive#fileLink', 'id': id_folder}]})
    archivo['title'] = path.split('/')[-1]
    archivo.SetContentFile(path)
    archivo.Upload()
