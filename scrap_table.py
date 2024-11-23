import requests
from bs4 import BeautifulSoup
import uuid
import boto3

def lambda_handler(event, context):
    url = "https://ds.iris.edu/latin_am/evlist.phtml?limit=20&new=1"
    response = requests.get(url)
    
    # Verificar si la solicitud fue exitosa
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': f'Error al acceder a la página web. Código de estado: {response.status_code}'
        }

    # Parsear la página HTML
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')  # Buscar la tabla principal

    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    # Extraer encabezados de la tabla
    headers = [header.text.strip() for header in table.find_all('th')]

    # Extraer datos de las filas de la tabla
    records = []
    for row in table.find_all('tr')[1:]:  # Saltar la fila de encabezados
        cols = row.find_all('td')
        if len(cols) == len(headers):  # Asegurar que cada fila tenga el número correcto de columnas
            record = {headers[i]: cols[i].text.strip() for i in range(len(cols))}
            records.append(record)

    # Ordenar registros por fecha y hora si el formato lo permite (opcional)
    try:
        records.sort(key=lambda x: x['FECHA - HORA (UTC)'], reverse=True)
    except KeyError:
        pass  # Si el campo no está bien formado, continuar sin ordenar

    # Seleccionar los 10 registros más recientes
    recent_records = records[:10]

    # Configurar DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table_name = 'TablaWebScrappingSismosNuevo'  # Nombre de la tabla en DynamoDB
    table = dynamodb.Table(table_name)

    # Limpiar la tabla en DynamoDB
    with table.batch_writer() as batch:
        for item in table.scan()['Items']:
            batch.delete_item(Key={'id': item['id']})

    # Insertar registros recientes en DynamoDB
    for record in recent_records:
        record['id'] = str(uuid.uuid4())  # Agregar un identificador único
        table.put_item(Item=record)

    # Retornar los datos procesados
    return {
        'statusCode': 200,
        'body': recent_records
    }
