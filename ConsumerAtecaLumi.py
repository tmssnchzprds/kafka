from hdfs import InsecureClient
from kafka import KafkaConsumer
import datetime
import time
import boto3

ruta = "/Proyecto/bronze/Ateca/luminosidad/"
topic = 'luminosidadAULAateca'
MAX_MESSAGES_PER_FILE = 1000 # máximo número de mensajes a guardar en un solo archivo

# Datos de conexión
HDFS_HOSTNAME = '172.17.10.30'
HDFSCLI_PORT = 9870
HDFSCLI_CONNECTION_STRING = f'http://{HDFS_HOSTNAME}:{HDFSCLI_PORT}'

# En nuestro caso, al no usar Kerberos, creamos una conexión no segura
hdfs_client = InsecureClient(HDFSCLI_CONNECTION_STRING)

# Consumidor Kafka
consumer = KafkaConsumer(
    topic,
    enable_auto_commit=True,
    bootstrap_servers=['172.17.10.33:9092','172.17.10.34:9092','172.17.10.35:9092'])

# Lista para almacenar los mensajes de Kafka
messages = []

for m in consumer:
    resp_json = m.value
    messages.append(resp_json)

    # Comprueba si se ha alcanzado el número máximo de mensajes
    if len(messages) >= MAX_MESSAGES_PER_FILE:
        # Crea el nombre del archivo
        fechaHora = datetime.datetime.now()
        anio = fechaHora.year
        mes = fechaHora.month
        dia = fechaHora.day
        hora = fechaHora.hour
        min = fechaHora.minute
        sec = fechaHora.second
        fechaNombre = f"{anio}{mes:02}{dia:02}{hora:02}{min:02}{sec:02}"
        counter = 1
        nom_fichero = ruta + fechaNombre + "_0.json"
        while hdfs_client.status(nom_fichero, strict=False):
            nom_fichero = ruta + fechaNombre + "_" + str(counter) + ".json"
            counter += 1
        # Convertir los elementos de messages a str
        messages_str = [msg.decode('utf-8') for msg in messages]
        
        # Escribe los mensajes en el archivo
        with hdfs_client.write(nom_fichero, overwrite=False) as writer:
            writer.write('\n'.join(messages_str).encode('utf-8'))

        # Borra los mensajes guardados de la lista
        messages.clear()