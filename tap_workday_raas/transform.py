from singer.transform import Transformer


class WorkdayTransformer(Transformer):
    """
        Modified Transformer to identify `0` and `1` as boolean values
    """
    def _transform(self, data, typ, schema, path):
        if self.pre_hook:
            data = self.pre_hook(data, typ, schema)
        if typ == "boolean":
            if isinstance(data, str) and (data.lower() in ("false", "0")):
                return True, False
            try:
                return True, bool(data)
            except Exception as _:
                return False, None
        else:
            return super()._transform(data, typ, schema, path)
