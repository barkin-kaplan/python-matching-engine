import socket

import psutil


def get_open_sockets_current_process():
    current_pid = psutil.Process().pid
    current_process = psutil.Process(current_pid)

    open_sockets = []
    try:
        connections = current_process.connections()
        for conn in connections:
            # Here we filter out non-socket connections (e.g., pipes, UNIX sockets)
            if conn.type == socket.SOCK_STREAM and conn.status == psutil.CONN_ESTABLISHED:
                open_sockets.append(conn)
    except psutil.NoSuchProcess:
        pass

    return open_sockets

def get_close_wait_sockets_of_current_process_for_ubuntu():
    current_pid = psutil.Process().pid
    connections = psutil.net_connections(kind='tcp')
    close_wait_connections = []

    for conn in connections:
        if conn.status == psutil.CONN_CLOSE_WAIT and (conn.pid == current_pid or conn.pid is None):
            close_wait_connections.append(conn)

    return close_wait_connections

def get_close_wait_sockets_of_current_process_for_mac():
    current_pid = psutil.Process().pid
    current_process = psutil.Process(current_pid)

    close_wait_sockets = []
    try:
        connections = current_process.connections()
        for conn in connections:
            # Here we filter out non-socket connections (e.g., pipes, UNIX sockets)
            if conn.type == socket.SOCK_STREAM and conn.status == psutil.CONN_CLOSE_WAIT:
                close_wait_sockets.append(conn)
    except psutil.NoSuchProcess:
        pass

    return close_wait_sockets


def shutdown_connections(connections: list):
    for conn in connections:
        sock = socket.fromfd(conn.fd, socket.AF_INET, socket.SOCK_STREAM)
        sock.shutdown(1)


def get_all_close_wait_socket_connections():
    connections = psutil.net_connections(kind='tcp')
    close_wait_connections = []

    for conn in connections:
        if conn.status == psutil.CONN_CLOSE_WAIT:
            close_wait_connections.append(conn)

    return close_wait_connections


