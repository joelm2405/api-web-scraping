import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    headers = [header.text.strip() for header in table.find_all('th')]
    rows = []
    for row in table.find_all('tr')[1:11]:
        cells = row.find_all('td')
        row_data = {headers[i]: cell.text.strip() for i, cell in enumerate(cells)}
        rows.append(row_data)

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaSismos')
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={'id': each['id']})

    for row in rows:
        row['id'] = str(uuid.uuid4())
        table.put_item(Item=row)

    return {
        'statusCode': 200,
        'body': rows
    }
