import csv
import socket
from collections import defaultdict


HAPROXY_STAT_SOCKET = '/var/run/haproxy.sock'


def socket_to_dicts(socket_path):
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
    cleaned = []
    for entry in dictified:
        del entry['']
        cleaned.append(entry)

    return cleaned


def statuses(dicts):
    """
    Return mapping of the form $map['process']['server'] = 'STATUS'.

    E.g. {'redis_cache_19201': {'cache10': 'UP', 'cache11': 'MAINT'}}
    """
    # 2-level dict
    processes = defaultdict(dict)
    for d in dicts:
        proc = d['pxname']
        server = d['svname']
        status = d['status']
        processes[proc][server] = status
    return processes


if __name__ == "__main__":
    import pprint
    dicts = socket_to_dicts(HAPROXY_STAT_SOCKET)
    pprint.pprint(dicts)
    pprint.pprint(dict(statuses(dicts)))
