import json
from pathlib import Path
from typing import Any, Dict

import os
from datetime import datetime

from dotenv import dotenv_values
from mcps1.application.utils import type_file_definition
from mcps1.domain.emun.data_use import DataUse
from mcps1.domain.emun.family_resources import FamilyResource
from mcps1.infraestructure.messaging.dtos.message_resource_dto import MessageResourceDTO

# Nota: Se recomienda usar el decorador @injectable o inyectar estas dependencias 
# en lugar de pasarlas manualmente para seguir la mejor práctica de FastAPI/Fast-depends.

class ResourceManagement:
    """
    Adapter Implementation for the Resource Management Contracts (Ports). 
    Implements the write/management side (ResourceMaintenderPortIn), handling CRUD operations 
    and ensuring persistence and event publishing when resources are modified.
    """
    def __init__(self, type_run: str = "dev"):
        # Inyección de dependencia para la capa de mensajería
        env_path = str([f for f in (Path(os.path.abspath("")).rglob(f".env.{type_run}")) if f.is_file()][0])
        self._resource_config: Dict[str, str] = dotenv_values(env_path)
        messaging_path = str([f for f in (Path(os.path.abspath("")).rglob(f".env.messaging.dev")) if f.is_file()][0])
        self._messaging_config: Dict[str, str] = dotenv_values(messaging_path)
        print("ResourceManagement Adapter initialized. Dependencies (DB, Kafka) ready for write operations.")


    def _store_resource(self, payload: MessageResourceDTO) -> str:
        """
        Establece un nuevo recurso en el sistema. 
        TODO: Implementar lógica de persistencia real (Database Repository Layer).
        """
        print(f"[ResourceManagement] INFO: Attempting to store a new resource record.")

        file_path: str = self._path_recource_builder(payload)
        if not payload.content:
            raise Exception("empty data content")
        version_json = payload.content.model_dump_json(indent=4)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(version_json)
        # Lógica de negocio pendiente: Validación, Serialización y persistencia en PostgreSQL.
        # Se debería emitir un evento Kafka aquí para notificar la creación del recurso.
        return file_path # Simula éxito temporalmente

    def _delete_resource(self, file_path: str) -> bool:
        """
        Elimina o marca como eliminado (soft delete) un registro de recurso. 
        TODO: Implementar lógica de persistencia real (Database Repository Layer).
        """
        os.remove(file_path)
        # Lógica de negocio pendiente: Soft delete en la base de datos para mantener el historial.
        return True # Simula éxito temporalmente

    def message_procesate(self, topic: str, payload: MessageResourceDTO) -> str:
        """
        Procesa mensajes asincrónicos de Kafka y realiza la persistencia local (JSON file dump). 
        Esta función se llama desde el Consumer Listener.
        """
        print(f"[ResourceManagement] INFO: Processing asynchronous message from topic: {topic}.")
        topics: list[str] = self._messaging_config['DEFAULT_TOPIC'].split(",")
        current_path: str = self._get_file_path(payload)
        if topic not in topics:
            raise Exception("incorrect topic")
        if topics[0] == topic:
            deleted: bool = self._delete_resource(current_path)
            return f"{current_path} was been deleted" if deleted else f"fail at delete {current_path}"
        elif current_path and topic in topics[1:]:
            deleted: bool = self._delete_resource(current_path)
            new_file_path: str = self._store_resource(payload)
            return new_file_path if new_file_path else f"fail at update {current_path}"
        else:
            new_file_path: str = self._store_resource(payload)
            return new_file_path if new_file_path else f"fail at create {payload.name_resource}"

    def _path_recource_builder(self, payload: MessageResourceDTO) -> str:
        try:
            familly_path: str = payload.family_resource.value
            type_path: str = payload.type_resource.value
            base_path: str = f"{self._resource_config['RESOURCES_LOCATION']}/{familly_path}/{type_path}"
            raw_name: str = payload.name_resource
            path_name: str = "/".join(raw_name.split("/")[:-1])
            file_extension: str = type_file_definition(payload.type_resource)
            file_name: str = raw_name.split("/")[-1:][0] + "." + file_extension
            usable_path: str = (base_path + "/" + path_name) if path_name else base_path

            os.makedirs(usable_path, exist_ok=True)

            file_path = os.path.join(usable_path, file_name)
            return file_path
        except Exception:
            raise Exception("incorrect filename or path definition")

    def _get_file_path(self, payload: MessageResourceDTO) -> str:
        try:
            familly_resource: str = payload.family_resource.value
            base_path: str = f"{self._resource_config['RESOURCES_LOCATION']}/{familly_resource}"
            
            file_name: str = payload.name_resource.split("/")[-1:][0]
            resource_path = next(Path(base_path).rglob((file_name + ".*")), None)
            return resource_path
        except Exception:
            raise Exception("incorrect filename or path definition")

