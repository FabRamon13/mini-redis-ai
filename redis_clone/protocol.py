from collections import namedtuple
from io import BytesIO

from redis_clone.exceptions import CommandError, Disconnect, ProtocolError


Error = namedtuple("Error", ("message",))

MAX_BULK_BYTES = 1024 * 1024
MAX_ARRAY_LENGTH = 1024
MAX_HEADER_BYTES = 1024


class ProtocolHandler:
    def __init__(self):
        ## Map of Redis protocol type indicators to their corresponding handler methods
        self.handlers = {
            b"+": self.handle_simple_string,
            b"-": self.handle_error,
            b":": self.handle_integer,
            b"$": self.handle_string,
            b"*": self.handle_array,
            b"%": self.handle_dict,
        }
    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)

        if not first_byte:
            raise Disconnect()

        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise ProtocolError(f"Unknown protocol type: {first_byte}")

    def encode(self, data):
        """Serialize one RESP value without writing to a buffered socket file."""
        buf = BytesIO()
        self._write(buf, data)
        return buf.getvalue()

    def write_response(self, socket_file, data):
        socket_file.write(self.encode(data))
        ##sends through TCP conn
        socket_file.flush()
        
    def _write(self, buf, data):
        if isinstance(data, str):
            data = data.encode("utf-8")

        if isinstance(data, bytes):
            buf.write(
                b"$" + str(len(data)).encode("utf-8") +
                b"\r\n" + data + b"\r\n"
            )

        elif isinstance(data, int):
            buf.write(
                b":" + str(data).encode("utf-8") + b"\r\n"
            )

        elif isinstance(data, Error):
            message = data.message
            if isinstance(message, str):
                message = message.encode("utf-8")

            buf.write(b"-" + message + b"\r\n")

        elif isinstance(data, (list, tuple)):
            buf.write(
                b"*" + str(len(data)).encode("utf-8") + b"\r\n"
            )

            for item in data:
                self._write(buf, item)

        elif isinstance(data, dict):
            buf.write(
                b"%" + str(len(data)).encode("utf-8") + b"\r\n"
            )

            for key, value in data.items():
                self._write(buf, key)
                self._write(buf, value)

        elif data is None:
            buf.write(b"$-1\r\n")

        else:
            raise CommandError(f"unrecognized type: {type(data)}")
    
    def _read_line(self, socket_file, description):
        line = socket_file.readline(MAX_HEADER_BYTES + 1)
        if not line:
            raise ProtocolError(f"Missing {description}")
        if len(line) > MAX_HEADER_BYTES:
            raise ProtocolError(f"{description.capitalize()} too large")
        if not line.endswith(b"\r\n"):
            raise ProtocolError(f"Truncated {description}")
        return line[:-2]

    ##parse to handle simple strings
    def handle_simple_string(self, socket_file):
        return self._read_line(socket_file, "simple string")

    ##handling errors 
    def handle_error(self, socket_file):
        return Error(self._read_line(socket_file, "error response"))

    ##handling integers 
    def handle_integer(self, socket_file):
        raw = self._read_line(socket_file, "integer")
        try:
            return int(raw)
        except ValueError as exc:
            raise ProtocolError("Invalid integer") from exc

    def handle_string(self, socket_file):
        length_line = self._read_line(socket_file, "bulk string length")
        try:
            length = int(length_line)
        except ValueError:
            raise ProtocolError("Invalid bulk string length")

        if length == -1:
            return None

        if length < -1:
            raise ProtocolError("Invalid bulk string length")

        if length > MAX_BULK_BYTES:
            raise ProtocolError("Bulk string too large")

        payload = socket_file.read(length)
        terminator = socket_file.read(2)

        if len(payload) != length or terminator != b"\r\n":
            raise ProtocolError("Truncated bulk string")

        return payload

    def handle_array(self, socket_file):
        length_line = self._read_line(socket_file, "array length")
        try:
            num_elements = int(length_line)
        except ValueError:
            raise ProtocolError("Invalid array length")

        if num_elements < 0:
            raise ProtocolError("Invalid array length")

        if num_elements > MAX_ARRAY_LENGTH:
            raise ProtocolError("Array too large")

        return [
            self.handle_request(socket_file)
            for _ in range(num_elements)
        ]

    def handle_dict(self, socket_file):
        length_line = self._read_line(socket_file, "map length")
        try:
            num_items = int(length_line)
        except ValueError as exc:
            raise ProtocolError("Invalid map length") from exc

        if num_items < 0:
            raise ProtocolError("Invalid map length")

        if num_items > MAX_ARRAY_LENGTH:
            raise ProtocolError("Map too large")

        elements = [
            self.handle_request(socket_file)
            for _ in range(num_items * 2)
        ]

        return dict(zip(elements[::2], elements[1::2]))
