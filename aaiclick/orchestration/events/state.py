"""Transport lifecycle states (see ``SignalTransport.state``)."""

from typing import Literal

STATE_IDLE = "idle"
STATE_LISTENING = "listening"
STATE_RECONNECTING = "reconnecting"
TransportState = Literal["idle", "listening", "reconnecting"]
