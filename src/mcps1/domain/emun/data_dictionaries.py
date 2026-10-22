from enum import Enum

class DataDictionaries(Enum):
    """
    Defines constant keys for accessing different resource schema JSON files 
    within the asinc_messages directory. This serves as a single source of truth 
    for defining which message schemas are available to the system.
    """
    MESSAGE_QUESTION = "src/resources/datas/asinc_messages/question.json"
    MESSAGE_RESPONSE = "src/resources/datas/asinc_messages/response.json"

# Ejemplo de uso (para verificación interna):
if __name__ == "__main__":
    print(f"Question Schema Path: {DataDictionaries.MESSAGE_QUESTION.value}")
    print(f"Response Schema Path: {DataDictionaries.MESSAGE_RESPONSE.value}")