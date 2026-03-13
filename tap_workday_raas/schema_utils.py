def infer_schema_from_value(value):
    """Infer JSON schema type from a sample Python value.

    Used when a column is found in data but not in the XSD schema,
    both during discovery enrichment and sync-time schema expansion.
    Defaults to nullable string for None values.
    """
    if value is None:
        return {"type": ["string", "null"]}
    elif isinstance(value, bool):
        return {"type": ["boolean", "null"]}
    elif isinstance(value, (int, float)):
        return {"type": ["number", "null"]}
    elif isinstance(value, dict):
        properties = {}
        for k, v in value.items():
            properties[k] = infer_schema_from_value(v)
        return {"type": "object", "properties": properties}
    elif isinstance(value, list):
        if value:
            items_schema = infer_schema_from_value(value[0])
        else:
            items_schema = {"type": ["string", "null"]}
        return {"type": "array", "items": items_schema}
    else:
        return {"type": ["string", "null"]}
