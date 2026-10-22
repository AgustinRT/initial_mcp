import logging
import os
from pathlib import Path
from typing import Any, Dict
import json

from dotenv import dotenv_values

from mcps1.domain.emun.family_resources import FamilyResource
from mcps1.infraestructure.messaging.adapters.messaging_adaptator import KafkaPortOutDependency
# from fastmcp import Context

class ResourcesAssignation():
    """
    Adapter Implementation for the Resource Management Contracts (Ports). 
    This Adapter implements both read-only (ResourcePortOut) and write/management (ResourceMaintenderPortIn) contracts.
    It acts as the primary point of interaction between the domain logic and infrastructure services.
    """
    _kafka_producer = None


    _name = "mcps1"
    _logger = logging.getLogger(_name)
    
    def __init__(self, kafka_producer: KafkaPortOutDependency, type_run: str = "dev"):
        env_path = str([f for f in (Path(os.path.abspath("")).rglob(f".env.{type_run}")) if f.is_file()][0])
        self._resource_config: Dict[str, str] = dotenv_values(env_path)

        self._logger.info("ResourceAssignments Adapter initialized. Dependencies (DB, Kafka) ready.")
        if not self._kafka_producer:
            self._kafka_producer = kafka_producer
        pass


        

    # =========================================================
    # 1. Implementation of ResourcePortOut methods (Read/Query)
    # =========================================================

    async def get_data_dictionary(self, type_data: str, value: str) -> str:
        """
        Retrieves a structured data dictionary (question or response schema).
        [Implementation logic remains the same]
        """

        resource_path = self._get_resource_path(type_data, value)
        self._logger.info(f"[ResourceAssignments] INFO: Attempting to load resource dictionary from path: {resource_path}")
        if not resource_path:
            return "unkwon resource"
        # BEST PRACTICE (File Access): Load the schema template from disk. 
        with open(resource_path, 'r') as f:
            data_dict = json.load(f)

        # === START KAFKA PUBLISHING FOR RESOURCE DEFINITION EVENT ===
        kafka_payload = {
            "source": "resources_assignation",
            "type_data": type_data,
            "value": value,
            "dictionary_content": data_dict
        }
        # The KafkaProducerService handles the 'mcps' group context. 
        await self._kafka_producer.send_message_reporter(
            destiny_message={"resource_type": type_data, "resource_value": value},
            message=kafka_payload
        )
        self._logger.info("[ResourceAssignments] Kafka: Successfully published resource dictionary definition event.")
        # === END KAFKA PUBLISHING FOR RESOURCE DEFINITION EVENT ===

        return json.dumps(data_dict)

    def _get_resource_path(self, type_data: str, value: str) -> str:
        try:
            base_path: str = f"{self._resource_config['RESOURCES_LOCATION']}/{FamilyResource[(type_data.upper())].value}"
            file_name: str = value.split("/")[-1:][0]
            resource_path = next(Path(base_path).rglob((file_name + ".*")), None)
            return resource_path
        except Exception:
            self._logger.error(f"*******************incorrent params to define resource path")
            raise Exception("incorrent params to define resource path")

    def get_contextual_values(self, context: str) -> Any:
        """
        Extracts contextual values from the provided data stream. 
        [IA Model Focus] Prepares structured input for the AI model. 
        """
        self._logger.info("[ResourceAssignments] INFO: Analyzing context from the input stream for AI model consumption.")
        # TODO: Implement robust validation and transformation logic here (Interaction with IA Service)
        return {"status": "success", "context_data": f"Context successfully extracted based on stream source: {context.source}"}

    def get_assignation_communications(self, route: str) -> Any:
        """
        Retrieves and publishes assignment communications (logs/events). 
        [Messaging Focus] Acts as both a reader of logs and a producer of events via Kafka.
        """
        self._logger.info("[ResourceAssignments] INFO: Retrieving communication records. Preparing to publish events.")
        # TODO: Implement fetching from DB and publishing to Kafka.
        return {"status": "success", "communication_log": ["[INFO] Event retrieved.", "[WARN] Event published successfully via Kafka."]}
