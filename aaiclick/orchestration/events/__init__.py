"""UI change signals: a bus, the session hooks that feed it, and one transport per backend.

Any transaction that writes ``jobs``, ``tasks`` or ``groups`` publishes one
payload-less signal when it commits (:mod:`.hooks`). How the signal reaches
the server's bus is the transport's job — :mod:`.local` publishes directly
in the single local-mode process, :mod:`.postgres` rides ``NOTIFY`` /
``LISTEN`` across hosts — chosen by :func:`get_transport`. The browser
invalidates its query cache on each signal and REST supplies authoritative
state, so a burst of writes collapses into one pending signal per
subscriber and nothing tenant-specific ever crosses the channel.

Importing this package registers the session listeners.
"""

from . import hooks  # noqa: F401 — registers the Session listeners
from .bus import EventBus, event_bus, get_event_bus
from .hooks import WATCHED_TABLES, statement_touches_watched
from .state import STATE_IDLE, STATE_LISTENING, STATE_RECONNECTING, TransportState
from .transport import SignalTransport, get_transport
