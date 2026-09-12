"""UI change signals: a bus, the session hooks that feed it, and one transport per backend.

Any transaction that writes ``jobs``, ``tasks`` or ``groups`` publishes one
payload-less signal when it commits (:mod:`.hooks`). How the signal reaches
the server's bus is the transport's job — :mod:`.local` publishes directly
in the single local-mode process, :mod:`.postgres` rides ``NOTIFY`` /
``LISTEN`` across hosts — chosen by :func:`get_transport`. The browser
invalidates its query cache on each signal and REST supplies authoritative
state, so a burst of writes collapses into one pending signal per
subscriber and nothing tenant-specific ever crosses the channel.
"""

from .bus import EventBus, Subscription, event_bus, get_event_bus
from .hooks import register_session_hooks, unregister_session_hooks
from .state import STATE_LISTENING
from .transport import SignalTransport, get_transport, signal_transport
