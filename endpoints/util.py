
import json
from pymemcache.client.base import Client as MemcacheClient
import json
import decimal  # Import the decimal module
from datetime import datetime



# Custom JSON Encoder class
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return str(obj)
        else:
            return super().default(obj)
