import os
from dotenv import find_dotenv, load_dotenv
from kafka import KafkaConsumer
import json
from typing import List

class KafkaConsumerRequest:
    """
    Handles consumption for requests messages.
    Uses environment variables from .env.messaging and requires no constructor parameters.
    """
    def __init__(self):
        # Cargar variables de entorno desde .env antes de inicializar el consumidor
        load_dotenv(find_dotenv())

        # 1. Obtener configuración del entorno
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", None)

        group_id = os.getenv("CONSUMER_GROUP_ID", "mcps_request_consumer_group") # Usar un default específico para requests

        if not bootstrap_servers:
            bootstrap_servers = os.getenv("bootstrap_servers")
            raise EnvironmentError("KAFKA_BOOTSTRAP_SERVERS must be set in the environment variables.")

        # 2. Determinar el topic (asumiendo que hay una variable dedicada o usando el default general)
        request_topic = os.getenv("REQUEST_TOPIC", "project.requests.messages") # Usar un tópico específico para requests, si existe
        if not request_topic:
            # Fallback seguro si no se encuentra REQUEST_TOPIC
             raise EnvironmentError("REQUEST_TOPIC must be set to specify the message topic.")

        # 3. Inicialización del Consumer
        self.consumer = KafkaConsumer(
            request_topic, # Usar el tópico leído de las variables de entorno
            bootstrap_servers=bootstrap_servers, # Usa la variable de entorno para brokers
            group_id=group_id,                   # Usa la variable de entorno para el grupo
            auto_offset_reset='earliest', 
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def consume_requests(self):
        """
        Starts consuming messages indefinitely, handling incoming requests.
        """
        print("Starting Kafka Consumer Request Listener...")
        try:
            for message in self.consumer:
                # Process the received message (assuming payload is a dictionary)
                print(f"Received REQUEST message:")
                print(f"  Topic: {message.topic}, Partition: {message.partition}")
                # Aquí se llamaría la lógica de manejo específica de peticiones, e.g., 
                # self._handle_request(message.value)
        except KeyboardInterrupt:
            print("Kafka Consumer Request Listener stopped by user.")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    try:
        # Ejemplo de uso (necesita Kafka corriendo localmente)
        consumer = KafkaConsumerRequest()
        consumer.consume_requests()
    except EnvironmentError as e:
        print(f"FATAL ERROR: {e}")