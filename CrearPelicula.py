import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    try:
        # Entrada (json)
        log_entrada = {
            "tipo": "INFO",
            "log_datos": {
                "timestamp": datetime.utcnow().isoformat(),
                "evento": "entrada_recibida",
                "mensaje": "Solicitud recibida para crear película",
                "datos": event
            }
        }
        print(json.dumps(log_entrada))

        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]

        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # Salida (json)
        log_exito = {
            "tipo": "INFO",
            "log_datos": {
                "timestamp": datetime.utcnow().isoformat(),
                "evento": "pelicula_creada",
                "mensaje": "Película creada exitosamente",
                "pelicula": pelicula,
                "tenant_id": tenant_id,
                "uuid": uuidv4
            }
        }
        print(json.dumps(log_exito))

        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }

    except KeyError as e:
        # Error de campos faltantes en el evento
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "timestamp": datetime.utcnow().isoformat(),
                "evento": "error_campo_faltante",
                "mensaje": f"Campo requerido faltante: {str(e)}",
                "error_tipo": "KeyError",
                "datos_evento": event
            }
        }
        print(json.dumps(log_error))
        return {
            'statusCode': 400,
            'error': f'Campo requerido faltante: {str(e)}'
        }

    except Exception as e:
        # Error general
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "timestamp": datetime.utcnow().isoformat(),
                "evento": "error_general",
                "mensaje": f"Error al crear película: {str(e)}",
                "error_tipo": type(e).__name__,
                "datos_evento": event
            }
        }
        print(json.dumps(log_error))
        return {
            'statusCode': 500,
            'error': f'Error interno del servidor: {str(e)}'
        }
