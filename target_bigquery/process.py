import copy
import json
import re
import pytz
from datetime import datetime
from tempfile import TemporaryFile

import singer
from jsonschema import validate

from target_bigquery.encoders import DecimalEncoder
from target_bigquery.schema import build_schema, filter
from target_bigquery.processhandler import BaseProcessHandler

logger = singer.get_logger()


def process(
        ProcessHandler,
        tap_stream,
        **kwargs
):
    handler = ProcessHandler(logger, **kwargs)
    assert isinstance(handler, BaseProcessHandler)

    if handler.emit_initial_state():
        s = kwargs.get("initial_state", {})
        assert isinstance(s, dict)
        logger.info(f"Pushing state: {s}")
        yield s  # yield init state, so even if there is an exception right after we get proper state emitted

    update_fields = kwargs.get("update_fields", False)

    for line in tap_stream:

        if update_fields:
            obj = json.loads(line.strip())
            msg_type = obj['type']
            new_obj = dict()
            # only deal with the first depth of fields, for Google Analytics schemas and records
            if msg_type == 'RECORD':
                for key, value in obj.items():
                    if key == 'record':
                        new_obj[key] = dict()
                        for k, v in obj[key].items():
                            new_obj[key][k.replace(':', '_')] = v
                    else:
                        new_obj[key] = value
                line = json.dumps(new_obj)
            elif msg_type == 'SCHEMA':
                for key, value in obj.items():
                    if key == 'schema':
                        new_obj[key] = dict()
                        for schema_key, schema_value in obj[key].items():
                            if schema_key == 'properties':
                                new_obj[key][schema_key] = dict()
                                for k, v in obj[key][schema_key].items():
                                    new_obj[key][schema_key][k.replace(':', '_')] = v
                            else:
                                new_obj[key][schema_key] = schema_value
                    else:
                        new_obj[key] = value
                line = json.dumps(new_obj)

        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            for s in handler.handle_record_message(msg):
                logger.info(f"Pushing state: {s}")
                yield s

        elif isinstance(msg, singer.StateMessage):
            logger.info("Updating state with {}".format(msg.value))
            for s in handler.handle_state_message(msg):
                logger.info(f"Pushing state: {s}")
                yield s

        elif isinstance(msg, singer.SchemaMessage):
            logger.info("{} schema: {}".format(msg.stream, msg.schema))
            for s in handler.handle_schema_message(msg):
                logger.info(f"Pushing state: {s}")
                yield s

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    for s in handler.on_stream_end():
        logger.info(f"Pushing state: {s}")
        yield s
