

from mcps1.domain.emun.data_use import DataUse


def type_file_definition(type_resource: DataUse) -> str:
    type_data: str = "txt"
    if DataUse.ASYNC_MESSAGE == type_resource or DataUse.REST_OBJ == type_resource:
        type_data = "json"
    elif DataUse.CONTEXT == type_resource:
        type_data = "md"
    elif DataUse.DB_TABLE == type_resource:
        type_data = "sql"
    else:
        type_data = "csv"
    return type_data