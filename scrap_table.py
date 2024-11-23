import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    url = "https://ds.iris.edu/latin_am/evlist.phtml?limit=20&new=1"
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')  # Encuentra la tabla principal
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    # Procesar encabezados de la tabla
    headers = [header.text.strip() for header in table.find_all('th')]

    # Procesar filas de la tabla
    rows = []
    for row in table.find_all('tr')[1:]:  # Ignorar la fila de encabezado
        cells = row.find_all('td')
        if len(cells) == len(headers):  # Asegurarse de que la fila tenga la misma cantidad de celdas que los encabezados
            row_data = {headers[i]: cells[i].text.strip() for i in range(len(cells))}
            rows.append(row_data)

    # Conectar a DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping')

    # Limpiar tabla existente
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={'id': each['id']})

    # Insertar nuevas filas en la tabla DynamoDB
    for row in rows:
        row['id'] = str(uuid.uuid4())  # Agregar un ID único para cada fila
        table.put_item(Item=row)

    return {
        'statusCode': 200,
        'body': rows
    }
