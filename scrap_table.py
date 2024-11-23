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

    # Extraer los datos de la tabla
    array = []
    for row in table.find_all('tr')[1:]:  # Saltar el encabezado de la tabla
        cols = row.find_all('td')
        if len(cols) >= 6:  # Validar que existan suficientes columnas
            # Extraer datos de las columnas
            fecha_hora = cols[0].text.strip()
            latitud = cols[1].text.strip()
            longitud = cols[2].text.strip()
            magnitud = cols[3].text.strip()
            profundidad = cols[4].text.strip()
            localidad = cols[5].text.strip()
            
            # Crear un registro y agregarlo al array
            array.append((fecha_hora, {
                'fecha_hora': fecha_hora,
                'latitud': latitud,
                'longitud': longitud,
                'magnitud': magnitud,
                'profundidad': profundidad,
                'localidad': localidad
            }))
    
    # Ordenar el array por la fecha y hora
    array.sort(key=lambda x: x[0], reverse=True)

    # Seleccionar los 10 registros más recientes
    registros_recientes = [record[1] for record in array[:10]]

    # Configuración de DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table_name = 'TablaWebScrappingSismos'
    table = dynamodb.Table(table_name)

    # Limpiar la tabla DynamoDB existente
    with table.batch_writer() as batch:
        for item in table.scan()['Items']:
            batch.delete_item(Key={'id': item['id']})

    # Insertar los registros recientes en DynamoDB
    resultado = []
    for i, registro in enumerate(registros_recientes, start=1):
        registro['#'] = i  # Agregar un índice al registro
        registro['id'] = str(uuid.uuid4())  # Generar un UUID único
        resultado.append(registro)  # Agregar a la lista de resultados
        table.put_item(Item=registro)  # Insertar en DynamoDB

    # Devolver los registros procesados
    return {
        'statusCode': 200,
        'body': resultado
    }
