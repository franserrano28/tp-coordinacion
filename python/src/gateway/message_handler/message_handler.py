import uuid

from common import message_protocol

internal = message_protocol.internal


class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().hex
        self.records_sent = 0

    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.records_sent += 1
        return internal.serialize([internal.DATA, self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return internal.serialize([internal.EOF, self.client_id, self.records_sent])

    def deserialize_result_message(self, message):
        client_id, fruit_top = internal.deserialize(message)
        if client_id != self.client_id:
            return None
        return fruit_top