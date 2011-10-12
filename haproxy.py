import csv
import socket
from collections import defaultdict


HAPROXY_STAT_SOCKET = '/var/run/haproxy.sock'


class HaproxyStatusEntry(object):
    """
    Encapsulates some basic logic re: Haproxy status CSV lines.

    E.g. whether a line refers to a backend/frontend, one of the actual backend
    servers being monitored, etc. Many fields' meanings then derive from that
    root observation.

    Behaves like the wrapped dict except for the extra methods/attributes.
    """
    def __init__(self, mapping):
        self.mapping = mapping

    NAME_MAP = {
        'name': 'svname',
        'proxy': 'pxname',
        'active': 'act',
    }

    def __getattr__(self, name):
        # Prefer, in order:
        # * NAME_MAP => mapping keys
        # * Actual mapping keys
        # * Mapping object attributes
        try:
            return self.mapping[self.NAME_MAP.get(name, name)]
        except KeyError:
            return getattr(self.mapping, name)

    def __getitem__(self, key):
        return self.mapping[key]

    def __repr__(self):
        return repr(self.mapping)

    def __str__(self):
        return str(self.mapping)

    @property
    def is_server(self):
        """
        Is this object representing a proxied server?
        """
        return self.name not in ('BACKEND', 'FRONTEND')

    @property
    def is_active(self):
        """
        Object is a server and is active ('act' == '1').
        """
        return self.is_server and self.active == '1'


def get_entries(socket_path):
    """
    Obtain status CSV from Haproxy socket, return HaproxyStatusEntry objects.
    """
    # Obtain CSV data from UNIX socket
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(socket_path)
    sock.sendall("show stat\n")
    # Why the fuck doesn't python stdlib have something for this bullshit.
    # At least Ruby sockets let you just do .read() and BAM done.
    result = ""
    while True:
        data = sock.recv(1024)
        if not data:
            break
        result += data

    # Nuke header line's comment
    result = result.partition("# ")[2]

    # DictReader => uses header to make handy dicts
    dictified = csv.DictReader(result.splitlines())

    # Clean trailing empty-field pair (good job haproxy? kind of strange.)
    # and turn into custom object
    cleaned = []
    for entry in dictified:
        del entry['']
        cleaned.append(HaproxyStatusEntry(entry))

    return cleaned


def statuses(entries):
    """
    Return mapping of the form $map['proxy']['server'] = 'STATUS'.

    E.g. {'redis_cache_19201': {'cache10': 'UP', 'cache11': 'MAINT'}}
    """
    # 2-level dict
    processes = defaultdict(dict)
    for e in entries:
        processes[e.proxy][e.name] = e.status
    # Cast back to vanilla dict for shits, giggles and better pprinting.
    # (Not expecting clients to modify.)
    return dict(processes)


if __name__ == "__main__":
    import pprint
    entries = get_entries(HAPROXY_STAT_SOCKET)
    pprint.pprint(statuses(entries))
    print "Active statuses:"
    for entry in filter(lambda x: x.is_server, entries):
        print "%s => %s: %s" % (entry.proxy, entry.name, entry.is_active)
