class CommandError(Exception):
    pass


class Disconnect(Exception):
    pass


class ProtocolError(CommandError):
    """The peer returned malformed or truncated RESP data."""


class DatastoreConnectionError(OSError):
    """Base class for bounded datastore transport failures."""

    def __init__(self, message, *, command=None, phase=None, cause=None):
        super().__init__(message)
        self.command = command
        self.phase = phase
        self.cause = cause


class CommandNotSent(DatastoreConnectionError):
    """A command failed before any request bytes could be transmitted."""

    may_have_committed = False


class AmbiguousCommandResult(DatastoreConnectionError):
    """A transmitted command may have committed without an acknowledgement."""

    may_have_committed = True


class ClientClosedError(DatastoreConnectionError):
    """The caller attempted to use a permanently closed client."""

    may_have_committed = False
