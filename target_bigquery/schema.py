import re

# import logging
from google.cloud.bigquery import SchemaField

JSON_SCHEMA_LITERALS = {"boolean", "number", "integer", "string"}
METADATA_FIELDS = {
    "_sdc_extracted_at": {"type": ["null", "string"], "format": "date-time", "bq_type": "timestamp"},
    "_sdc_batched_at": {"type": ["null", "string"], "format": "date-time", "bq_type": "timestamp"},
    "_sdc_received_at": {"type": ["null", "string"], "format": "date-time", "bq_type": "timestamp"},
    "_sdc_sequence": {"type": ["null", "integer"], "format": "integer", "bq_type": "integer"},
}


def get_type(property):
    nullable = False
    prop_type = None

    if "anyOf" in property:
        return "anyOf", nullable
    elif "type" in property:
        prop_type = property["type"]
    else:
        raise ValueError(
            f"'type' or 'anyOf' are required fields in property: {property}"
        )

    if isinstance(prop_type, str):
        return prop_type, nullable

    field_type = None

    if isinstance(prop_type, list):
        for t in prop_type:
            if t.lower() == "null":
                nullable = True
            else:
                field_type = t

    return field_type, nullable


def filter(schema, record):
    if not record:
        return record
    field_type, _ = get_type(schema)

    # return literals without checking
    if field_type in JSON_SCHEMA_LITERALS:
        # logging.error(f"JSON_SCHEMA_LETERALS record: {record}")
        return record

    elif field_type == "anyOf":
        for prop in schema["anyOf"]:
            prop_type, _ = get_type(prop)

            if prop_type == "null":
                continue

            # anyOf can be an array of properties, the choice here
            # is to ignore the case where anyOf is two types (not including "null")
            # and simply choose the first one. This might bite us.
            # adswerve recommendation: implement prioritization of string, then float, then int
            # logging.error(f"anyof prop: {prop}, anyof record: record")
            return filter(prop, record)

    elif field_type == "object":
        props = {**schema.get("properties", {}), **METADATA_FIELDS}
        obj_results = {}
        # logging.error(f"object props: {props}")
        for key, prop_schema in props.items():
            if key not in record:
                # logging.error(f"KEY ({key}) NOT IN OBJECT RECORD ({record})!!!!")
                continue

            obj_results[bigquery_transformed_key(key)] = filter(prop_schema,
                                                                record[key])  # adswerve fix to match schema field name
        #     logging.error(f"object record key {key} appended")
        # logging.error(f"filtered object: {obj_results}")
        return obj_results

    elif field_type == "array":
        props = schema.get("items", {})
        # logging.error(f"array props: {props}")

        prop_type, _ = get_type(props)

        # array can contain either an object or literals
        # - if it contains literals, simply return those
        if prop_type != "object":
            # logging.error(f"prop type is not object, returning record {record}")
            return record

        arr_result = []
        for obj in record:
            arr_result.append(filter(props, obj))
        # logging.error(f"filtered array: {arr_result}")
        return arr_result
    else:
        raise ValueError(f"type {field_type} is unknown")


def define_schema(field, name, required_fields=None):
    field_type, _ = get_type(field)
    nullable = True
    if required_fields and name in required_fields:
        nullable = False

    if field_type == "anyOf":
        props = field["anyOf"]
        # select first non-null property
        for prop in props:
            prop_type, _ = get_type(prop)
            if not prop_type:
                continue

            # take the first property that is not None
            # of the possible types
            if field_type == "anyOf" and prop_type:
                field_type = prop_type
                field = prop

    schema_description = None
    schema_name = name
    schema_mode = "NULLABLE" if nullable else "required"

    if field_type == "object":
        schema_type = "RECORD"
        schema_fields = tuple(build_schema(field, add_metadata=False))

        # logging.info(f"schema name: {bigquery_transformed_key(schema_name)}, type: {schema_type}, mode: {schema_mode},  fields: {schema_fields}")
        return SchemaField(
            bigquery_transformed_key(schema_name), schema_type, schema_mode, schema_description, schema_fields,
            # adswerve fix
        )
    elif field_type == "array":
        # objects in arrays cannot be nullable
        # - but nested fields in RECORDS can be nullable
        props = field.get("items")
        props_type, _ = get_type(props)

        if props_type == "object":
            schema_type = "RECORD"
            schema_fields = tuple(build_schema(props, add_metadata=False))
        else:
            schema_type = props_type if props_type.lower() != 'array' else get_type(props.get('items'))[0]  # adswerve fix
            schema_fields = ()

        schema_mode = "REPEATED"

        # logging.info(f"schema name: {bigquery_transformed_key(schema_name)}, type: {schema_type}, mode: {schema_mode},  fields: {schema_fields}")
        return SchemaField(
            bigquery_transformed_key(schema_name), schema_type, schema_mode, schema_description, schema_fields,
            # adswerve fix
        )

    if field_type not in JSON_SCHEMA_LITERALS:
        raise ValueError(f"unknown type: {field_type}")

    if field_type == "string" and "format" in field:
        format = field["format"]
        if format == "date-time":
            schema_type = "timestamp"
        elif format == "date":
            schema_type = "date"
        elif format == "time":
            schema_type = "time"
        else:
            schema_type = field_type
    elif field_type == "number":
        schema_type = "FLOAT"
    else:
        schema_type = field_type

        # always make a field nullable
    # logging.info(f"schema name: {bigquery_transformed_key(schema_name)}, type: {schema_type}, mode: {schema_mode}")
    return SchemaField(bigquery_transformed_key(schema_name), schema_type, schema_mode, schema_description,
                       ())  # adswerve fix


def bigquery_transformed_key(key):
    for pattern, repl in [(r"-", "_"), (r"\.", "_")]:
        key = re.sub(pattern, repl, key)
    if re.match(r"^\d", key): key = "_" + key  # adswerve fix
    return key


def build_schema(schema, key_properties=None, add_metadata=True, force_fields={}):
    SCHEMA = []

    required_fields = set(key_properties) if key_properties else set()
    if "required" in schema:
        required_fields.update(schema["required"])

    for key, props in schema["properties"].items():

        if key in force_fields:
            SCHEMA.append(
                SchemaField(key, force_fields[key]["type"], force_fields[key].get("mode", "nullable"), force_fields[key].get("description", None), ())
            )
        elif not props:
            # if we end up with an empty record.
            continue

        else:
            SCHEMA.append(
            define_schema(
                props, bigquery_transformed_key(key), required_fields=required_fields
            )
        )

    if add_metadata:
        for field in METADATA_FIELDS:
            SCHEMA.append(
                SchemaField(field, METADATA_FIELDS[field]["bq_type"], "nullable", None, ())
            )

    return SCHEMA
