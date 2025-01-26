import time
import json
import socket
import struct
import threading
import logging
import concurrent.futures
from json import JSONDecodeError
import os
import psutil

# Function to fetch the network IP address
def fetch_network_ip():
    try:
        os_type = os.name
        iface = "Ethernet" if os_type == "nt" else "en0"
        idx = 1 if os_type == "nt" else 0

        for intf, addr_list in psutil.net_if_addrs().items():
            if (intf == iface):
                return addr_list[idx][1]
    except:
        return None

# Constants
IP_ADDRESS = fetch_network_ip()
MULTICAST_BUFFER_SIZE = 20480
BUFFER_SIZE = 2048

# Logging configuration
logging.basicConfig(level=logging.INFO)
Logger_1 = logging.getLogger(__name__)

# Time if Leader not reachable
LEADER_DEATH_TIME = 10
MLTC_SERVER_PORT = 60003
MLTC_CLIENT_PORT = 60002
MLTC_TTL = 2
MLTC_GROUP_ADDRESS = '225.0.0.4'
TCP_PORT_CONSTANT_CLIENT = 60001
BRDC_IP_ADDRESS = '255.255.255.255'
BRDC_PORT_SERVER = 60004
BRDC_PORT_CLIENT = 60000
BULLY_PORT = 60005
HEARTBEAT_PORT_SERVER = 60006

class Server:
    def __init__(self):
        # Initializes the server attributes
        self.election_in_progress = False
        self.leader_ip = ''
        self.shutdown_event = threading.Event()
        self.threads = []
        self.bully_in_progress = False
        self.known_servers = []
        self.chat_members = []
        self.is_leader = False
        self.last_leader_message_time = time.time()
        self.lock = threading.Lock()

    def start_server(self):
        # Starts the server and initializes threads for various tasks
        def create_task(executor, task):
            future = executor.submit(self.run_with_exception_handling, task)
            self.threads.append(future)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            tasks = [
                self.listen_for_server_broadcasts,
                self.send_server_broadcast,
                self.bully_algorithm,
                self.update_leader_info,
                self.listen_for_heartbeat,
                self.detect_leader_failure,
                self.listen_for_client_broadcasts,
                self.handle_client_messages,
                self.send_leader_update
            ]

            for task in tasks:
                create_task(executor, task)

            try:
                while not self.shutdown_event.is_set():
                    time.sleep(1)
            except KeyboardInterrupt:
                Logger_1.info("Server shutdown initiated. Time to say goodbye!")
                self.shutdown_event.set()
            finally:
                for thread in self.threads:
                    thread.cancel()
                executor.shutdown(wait=True)

    def send_server_broadcast(self):
        # Sends periodic broadcast messages to discover other servers
        def setup_socket():
            try:
                broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                return broadcast_socket
            except socket.error as e:
                Logger_1.error(f"Yikes! Couldn't create broadcast socket: {e}")
                return None

        def send_broadcast_message(broadcast_socket):
            message = 'server_discovery'.encode()
            try:
                broadcast_socket.sendto(message, (BRDC_IP_ADDRESS, BRDC_PORT_SERVER))
                Logger_1.debug(f'Broadcast message sent: {message}')
            except socket.error as send_error:
                Logger_1.error(f"Oh no! Failed to send broadcast message: {send_error}")
            except Exception as send_exception:
                Logger_1.error(f"Unexpected twist during broadcast send: {send_exception}")

        broadcast_socket = setup_socket()
        if not broadcast_socket:
            return

        try:
            while not self.shutdown_event.is_set():
                send_broadcast_message(broadcast_socket)
                time.sleep(5)  # Broadcast interval
        finally:
            broadcast_socket.close()

    def run_with_exception_handling(self, target):
        # Executes a function with exception handling
        try:
            target()
        except Exception as e:
            Logger_1.error(f"Oops! Error in execution thread {target.__name__}: {e}")

    def listen_for_server_broadcasts(self):
        # Listens for broadcast messages from other servers
        Logger_1.info('Standing by for incoming server broadcasts...')

        def setup_socket():
            try:
                server_listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                server_listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_listener_socket.bind(('', BRDC_PORT_SERVER))
                server_listener_socket.settimeout(1)
                return server_listener_socket
            except socket.error as e:
                Logger_1.error(f'Error setting up listener socket: {e}')
                return None

        def receive_broadcast_message(server_listener_socket):
            try:
                msg, addr = server_listener_socket.recvfrom(BUFFER_SIZE)
                Logger_1.info('Received broadcast message for server discovery. New friends, perhaps?')

                if addr[0] not in self.known_servers:
                    Logger_1.info(f"New server on the block: {addr}")
                    self.known_servers.append(addr[0])

                response_message = 'hello'.encode()
                try:
                    server_listener_socket.sendto(response_message, addr)
                    Logger_1.debug(f'Sent hello message to {addr}')
                except socket.error as send_error:
                    Logger_1.error(f"Oops! Error sending hello message: {send_error}")
                except Exception as send_exception:
                    Logger_1.error(f"Unexpected issue during hello message send: {send_exception}")

            except socket.timeout:
                pass
            except socket.error as recv_error:
                Logger_1.error(f'Whoops! Socket error while receiving: {recv_error}')
            except Exception as recv_exception:
                Logger_1.error(f'Unexpected error while receiving: {recv_exception}')

        server_listener_socket = setup_socket()
        if not server_listener_socket:
            return

        try:
            while not self.shutdown_event.is_set():
                receive_broadcast_message(server_listener_socket)
        finally:
            server_listener_socket.close()

    def detect_leader_failure(self):
        # Monitors heartbeat messages from the leader and detects leader failures
        Logger_1.info('Initiating leader failure detection protocol...')
        self.last_leader_message_time = time.time()  # Initialize at start
        while not self.shutdown_event.is_set():
            time.sleep(3)
            current_time = time.time()
            time_since_last_heartbeat = current_time - self.last_leader_message_time
            Logger_1.info(f'Time since last heartbeat: {time_since_last_heartbeat} seconds')

        
            if not self.is_leader and not self.bully_in_progress:
                if time_since_last_heartbeat >= LEADER_DEATH_TIME:
                    Logger_1.info(f'No leader detected! Time since last heartbeat: {time_since_last_heartbeat} seconds')
                    self.initiate_bully_algorithm()
                else:
                    Logger_1.debug(f'Leader is alive and kicking. Time since last heartbeat: {time_since_last_heartbeat} seconds')

    def handle_heartbeat(self, message):
        # Processes incoming heartbeat messages
        if message['type'] == 'heartbeat':
            with self.lock:  # Thread-safe update
                leader_id = message["mid"]
                self.last_leader_message_time = time.time()
                Logger_1.debug(f'Heartbeat received from {leader_id} at {self.last_leader_message_time}')
            
                # Check if the leader has a lower ID
                if leader_id < IP_ADDRESS:
                    Logger_1.warning(f"Alert! Invalid leader with lower ID detected: {leader_id}. Initiating Bully Algorithm.")
                    time.sleep(5)
                    self.initiate_bully_algorithm()

    def initiate_bully_algorithm(self):
        # Initiates the Bully algorithm to elect a new leader
        if self.bully_in_progress:
            return  # Prevent multiple initiations
        Logger_1.info('Bully Algorithm is now in motion!')
        self.bully_in_progress = True
        higher_servers = [server for server in self.known_servers if server > IP_ADDRESS]

        if not higher_servers:
            self.become_leader()
        else:
            for server in higher_servers:
                self.send_election_message(server)

            time.sleep(5)
            if not self.is_leader:
                self.become_leader()

    def send_election_message(self, server):
        # Sends an election message to another server
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as election_socket:
                election_message = {"mid": IP_ADDRESS, "type": "election"}
                message = json.dumps(election_message).encode()
                election_socket.sendto(message, (server, BULLY_PORT))
                Logger_1.info(f'Election message sent to server {server}')
        except socket.error as e:
            Logger_1.error(f'Oh dear! Socket error sending election message: {e}')

    def become_leader(self):
        # Sets the current server as the new leader
        Logger_1.info(f'{IP_ADDRESS} is now the leader! All hail the new leader!')
        self.is_leader = True
        self.bully_in_progress = False
        self.announce_victory()

    def announce_victory(self):
        # Announces victory and informs all known servers
        Logger_1.info('Victory! Announcing leadership to all known servers.')
        for server in self.known_servers:
                self.send_victory_message(server)

    def send_victory_message(self, server):
        # Sends a victory message to another server
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as victory_socket:
                victory_message = {"mid": IP_ADDRESS, "type": "victory"}
                message = json.dumps(victory_message).encode()
                victory_socket.sendto(message, (server, BULLY_PORT))
                Logger_1.info(f'Victory message sent to server {server}')
        except socket.error as e:
            Logger_1.error(f'Oh no! Socket error sending victory message: {e}')

    def bully_algorithm(self):
        # Implements the Bully algorithm to elect a new leader
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as bully_listener_socket:
            bully_listener_socket.bind((IP_ADDRESS, BULLY_PORT))
            while not self.shutdown_event.is_set():
                try:
                    data, address = bully_listener_socket.recvfrom(BUFFER_SIZE)
                    with self.lock:
                        message = json.loads(data.decode())
                        if message['type'] == 'election':
                            if not self.bully_in_progress and not self.is_leader:
                                self.bully_in_progress = True
                                if message['mid'] < IP_ADDRESS:
                                    self.send_election_message(message['mid'])
                                else:
                                    self.initiate_bully_algorithm()
                        elif message['type'] == 'victory':
                            self.leader_ip = message['mid']
                            self.is_leader = (self.leader_ip == IP_ADDRESS)
                            self.bully_in_progress = False
                            self.last_leader_message_time = time.time()  # Reset timer on victory
                            Logger_1.info(f'{IP_ADDRESS}: New leader is {self.leader_ip}')
                        elif message['type'] == 'heartbeat':
                            self.last_leader_message_time = time.time()
                            Logger_1.info(f'Heartbeat received from {message["mid"]} at {self.last_leader_message_time}')
                            # Check if the leader has a lower ID
                            if message['mid'] < IP_ADDRESS:
                                Logger_1.warning(f"Alert! Invalid leader with lower ID detected: {message['mid']}. Initiating Bully Algorithm.")
                                self.initiate_bully_algorithm()
                except Exception as e:
                    Logger_1.error(f'Error in bully_algorithm: {e}')

    def update_leader_info(self):
        # Updates the information about the current leader
        def setup_socket():
            try:
                leader_info_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                leader_info_socket.bind(('', MLTC_SERVER_PORT))
                multicast_group = socket.inet_aton(MLTC_GROUP_ADDRESS)
                multicast_request = struct.pack('4sL', multicast_group, socket.INADDR_ANY)
                leader_info_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, multicast_request)
                leader_info_socket.settimeout(1)
                return leader_info_socket
            except socket.error as e:
                Logger_1.error(f"Error setting up multicast socket in update_leader_info: {e}")
                return None

        def receive_leader_info(leader_info_socket):
            try:
                data, addr = leader_info_socket.recvfrom(MULTICAST_BUFFER_SIZE)
                Logger_1.debug(f'Received multicast data from {addr}: {data}')
                if addr[0] == addr[0]:
                    data = json.loads(data.decode())
                    if isinstance(data, dict):  # Ensure data is a dictionary
                        self.chat_members = data.get("chat_members", [])  # Correct assignment
                        Logger_1.info(f'Updated chat rooms according to leader server: {self.chat_members}')
                    else:
                        Logger_1.warning(f'Uh-oh! Received data is not a dictionary: {data}')
            except socket.timeout:
                pass
            except JSONDecodeError as e:
                Logger_1.error(f"Yikes! JSON decoding error: {e}")
            except Exception as e:
                Logger_1.error(f"Unexpected error in update_leader_info: {e}")

        leader_info_socket = setup_socket()
        if not leader_info_socket:
            return

        try:
            while not self.shutdown_event.is_set():
                if not self.is_leader:
                    receive_leader_info(leader_info_socket)
        finally:
            leader_info_socket.close()

    def send_leader_update(self):
        # Sendet regelmäßig Updates über den aktuellen Leader
        def setup_socket():
            try:
                leader_update_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                leader_update_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MLTC_TTL)
                return leader_update_socket
            except socket.error as e:
                Logger_1.error('Oh no! Socket error setting up leader update socket: %s', e)
                return None

        def send_update_message(leader_update_socket):
            try:
                message = json.dumps({"chat_members": self.chat_members}).encode()  # Correct structure
                leader_update_socket.sendto(message, (MLTC_GROUP_ADDRESS, MLTC_SERVER_PORT))
                Logger_1.info(f'Sent leader update for chat members: {self.chat_members}')
            except socket.error as e:
                Logger_1.error('Oh no! Socket error sending leader update: %s', e)
            except Exception as e:
                Logger_1.error('An unexpected error occurred: %s', e)

        while not self.shutdown_event.is_set():
            if self.is_leader:
                leader_update_socket = setup_socket()
                if not leader_update_socket:
                    return

                try:
                    send_update_message(leader_update_socket)
                finally:
                    leader_update_socket.close()
            time.sleep(5)

    def listen_for_heartbeat(self):
        # Hört auf Herzschlagnachrichten von anderen Servern
        Logger_1.info('Listening for heartbeat messages...')
        
        def setup_socket():
            try:
                heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                heartbeat_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                heartbeat_socket.bind(('', HEARTBEAT_PORT_SERVER))
                multicast_group = socket.inet_aton(MLTC_GROUP_ADDRESS)
                multicast_request = struct.pack('4sL', multicast_group, socket.INADDR_ANY)
                heartbeat_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, multicast_request)
                heartbeat_socket.settimeout(2)
                return heartbeat_socket
            except socket.error as e:
                Logger_1.error(f'Error setting up heartbeat listener socket: {e}')
                return None

        def receive_heartbeat(heartbeat_socket):
            try:
                data, addr = heartbeat_socket.recvfrom(MULTICAST_BUFFER_SIZE)
                message = json.loads(data.decode())
                if message['type'] == 'heartbeat':
                    self.handle_heartbeat(message)
            except socket.timeout:
                pass
            except socket.error as e:
                Logger_1.error(f'Socket error while receiving heartbeat: {e}')
            except Exception as e:
                Logger_1.error(f'Unexpected error while receiving heartbeat: {e}')

        heartbeat_socket = setup_socket()
        if not heartbeat_socket:
            return

        try:
            while not self.shutdown_event.is_set():
                if self.is_leader:
                    self.send_heartbeat()
                else:
                    receive_heartbeat(heartbeat_socket)
        finally:
            heartbeat_socket.close()

    def send_heartbeat(self):
        # Sendet regelmäßig Herzschlagnachrichten, wenn der Server der Leader ist
        if not self.is_leader:
            return  # Stop sending heartbeats if not the leader

        def setup_socket():
            try:
                heartbeat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                heartbeat_client_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MLTC_TTL)
                heartbeat_client_socket.settimeout(1)
                return heartbeat_client_socket
            except socket.error as e:
                Logger_1.error(f'Error setting up heartbeat client socket: {e}')
                return None

        def send_heartbeat_message(heartbeat_client_socket):
            try:
                Logger_1.debug('Sending heartbeat')
                message = json.dumps({"mid": IP_ADDRESS, "type": "heartbeat"}).encode()
                heartbeat_client_socket.sendto(message, (MLTC_GROUP_ADDRESS, HEARTBEAT_PORT_SERVER))
                Logger_1.info('Heartbeat sent successfully! Thump-thump!')
                time.sleep(2)
            except socket.error as e:
                Logger_1.error(f"Yikes! Socket error while sending heartbeat: {e}")
            except Exception as e:
                Logger_1.error(f"Unexpected error while sending heartbeat: {e}")

        heartbeat_client_socket = setup_socket()
        if not heartbeat_client_socket:
            return

        try:
            send_heartbeat_message(heartbeat_client_socket)
        finally:
            heartbeat_client_socket.close()

    def listen_for_client_broadcasts(self):
        # Hört auf Broadcast-Nachrichten von Clients
        def setup_socket():
            try:
                client_listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                client_listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                client_listener_socket.bind(('', BRDC_PORT_CLIENT))
                client_listener_socket.settimeout(1)
                return client_listener_socket
            except socket.error as e:
                Logger_1.error(f"Yikes! Error opening socket for client broadcast requests: {e}")
                return None

        def receive_client_broadcast(client_listener_socket):
            try:
                msg, client_address = client_listener_socket.recvfrom(BUFFER_SIZE)
                Logger_1.debug(f"Incoming server discovery request from client {client_address}")

                response_message = 'hello'.encode()
                try:
                    client_listener_socket.sendto(response_message, client_address)
                    Logger_1.debug(f'Sent hello message to client {client_address}')
                except socket.error as send_error:
                    Logger_1.error(f"Oops! Error sending hello message to client: {send_error}")
                except Exception as send_exception:
                    Logger_1.error(f"Unexpected issue during hello message send to client: {send_exception}")

            except socket.timeout:
                pass
            except socket.error as recv_error:
                Logger_1.error(f'Oh no! Socket error while receiving from client: {recv_error}')
            except Exception as recv_exception:
                Logger_1.error(f'Unexpected error while receiving from client: {recv_exception}')

        client_listener_socket = setup_socket()
        if not client_listener_socket:
            return

        try:
            while not self.shutdown_event.is_set():
                if self.is_leader:
                    receive_client_broadcast(client_listener_socket)
        finally:
            client_listener_socket.close()

    def handle_client_messages(self):
        # Verarbeitet eingehende Nachrichten von Clients
        def setup_socket():
            try:
                server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_socket.bind((IP_ADDRESS, TCP_PORT_CONSTANT_CLIENT))
                server_socket.listen()
                return server_socket
            except socket.error as e:
                Logger_1.error(f"Error setting up server socket: {e}")
                return None

        def handle_client_connection(self, server_socket):
            # Verarbeitet eingehende Nachrichten von Clients
            def accept_client_connection():
                try:
                    client_socket, addr = server_socket.accept()
                    return client_socket, addr[0]
                except socket.error as e:
                    Logger_1.error(f"Error accepting client connection: {e}")
                    return None, None
                except Exception as e:
                    Logger_1.error(f"Unexpected issue accepting client connection: {e}")
                    return None, None

            def process_join(client_addr):
                if client_addr not in self.chat_members:
                    self.chat_members.append(client_addr)
                    join_message = f'New participant {client_addr} joined the chat room'
                    self.forward_message_to_chat_members(join_message, "SYSTEM")
                    response = f"Welcome aboard! You've successfully joined the chat room."
                    Logger_1.info(f"Client {client_addr} joined the chat room")
                else:
                    response = "You're already in this chat room!"
                    Logger_1.warning(f"Client {client_addr} is already in the chat room")
                return response

            def process_leave(client_addr):
                if client_addr in self.chat_members:
                    self.chat_members.remove(client_addr)
                    leave_message = f'Participant {client_addr} left the chat room'
                    self.forward_message_to_chat_members(leave_message, "SYSTEM")
                    response = "You've successfully left the chat room. Goodbye!"
                    Logger_1.info(f"Client {client_addr} left the chat room")
                else:
                    response = "You're not in any chat room!"
                    Logger_1.warning(f"Client {client_addr} is not in any chat room")
                return response

            def process_chat(client_addr, msg):
                if client_addr in self.chat_members:
                    self.forward_message_to_chat_members(msg, client_addr)
                    Logger_1.info(f"Message from {client_addr} sent to chat members")
                    return 'Message sent successfully!'
                Logger_1.warning(f"Client {client_addr} tried to send a message without joining a chat room")
                return "Nobody here to listen - join a chat room first!"

            def process_client_message(client_socket, client_addr):
                try:
                    data = client_socket.recv(BUFFER_SIZE)
                    client_response_msg = ''
                    if data:
                        json_data = json.loads(data.decode('UTF-8'))
                        Logger_1.info(f"Message received from client: {json_data}")
                        if json_data['function'] == 'join':
                            client_response_msg = process_join(client_addr)
                        elif json_data['function'] == 'chat':
                            client_response_msg = process_chat(client_addr, json_data.get('msg'))
                        elif json_data['function'] == 'leave':
                            client_response_msg = process_leave(client_addr)
                        else:
                            client_response_msg = "Received invalid data object"
                        try:
                            client_socket.sendall(client_response_msg.encode('UTF-8', errors='replace'))
                        except socket.error as send_error:
                            Logger_1.error(f"Oops! Error sending response to client: {send_error}")
                        except Exception as send_exception:
                            Logger_1.error(f"Unexpected issue during response send to client: {send_exception}")
                except socket.error as recv_error:
                    Logger_1.error(f"Yikes! Error receiving data from client: {recv_error}")
                except Exception as recv_exception:
                    Logger_1.error(f"Unexpected issue receiving data from client: {recv_exception}")
                finally:
                    client_socket.close()
                    Logger_1.info(f"Connection with client {client_addr} closed")

            while not self.shutdown_event.is_set():
                if self.is_leader:
                    client_socket, client_addr = accept_client_connection()
                    if client_socket and client_addr:
                        process_client_message(client_socket, client_addr)

        server_socket = setup_socket()
        if not server_socket:
            return

        try:
            while not self.shutdown_event.is_set():
                if self.is_leader:
                    handle_client_connection(self, server_socket)
        finally:
            server_socket.close()

    def join_chat_room(self, client_addr):
        # Fügt einen Client dem Chat-Raum hinzu
        if client_addr not in self.chat_members:
            self.chat_members.append(client_addr)
            join_message = f'New participant {client_addr} joined the chat room'
            self.forward_message_to_chat_members(join_message, "SYSTEM")
            response = f"Welcome aboard! You've successfully joined the chat room."
            Logger_1.info(f"Client {client_addr} joined the chat room")
        else:
            response = "You're already in this chat room!"
            Logger_1.warning(f"Client {client_addr} is already in the chat room")

        return response

    def leave_chat_room(self, client_addr):
        # Entfernt einen Client aus dem Chat-Raum
        if client_addr in self.chat_members:
            self.chat_members.remove(client_addr)
            leave_message = f'Participant {client_addr} left the chat room'
            self.forward_message_to_chat_members(leave_message, "SYSTEM")
            response = "You've successfully left the chat room. Goodbye!"
            Logger_1.info(f"Client {client_addr} left the chat room")
        else:
            response = "You're not in any chat room!"
            Logger_1.warning(f"Client {client_addr} is not in any chat room")

        return response

    def send_message(self, client_addr, message):
        # Sendet eine Nachricht von einem Client an alle Chat-Mitglieder
        if client_addr in self.chat_members:
            self.forward_message_to_chat_members(message, client_addr)
            Logger_1.info(f"Message from {client_addr} sent to chat members")
            return 'Message sent successfully!'

        Logger_1.warning(f"Client {client_addr} tried to send a message without joining a chat room")
        return "Nobody here to listen - join a chat room first!"

    def forward_message_to_chat_members(self, msg, sender):
        # Leitet eine Nachricht an alle Chat-Mitglieder weiter
        def setup_socket():
            try:
                multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MLTC_TTL)
                return multicast_socket
            except socket.error as e:
                Logger_1.error(f"Error setting up multicast socket: {e}")
                return None

        def send_message_to_members(multicast_socket, message):
            for client_addr in self.chat_members:
                try:
                    multicast_socket.sendto(message, (client_addr, MLTC_CLIENT_PORT))
                except socket.error as send_error:
                    Logger_1.error(f"Oops! Error sending message to {client_addr}: {send_error}")
                except Exception as send_exception:
                    Logger_1.error(f"Unexpected issue during message send to {client_addr}: {send_exception}")

        multicast_socket = setup_socket()
        if not multicast_socket:
            return

        send_message = f'{sender}: {msg}'.encode('UTF-8')
        try:
            send_message_to_members(multicast_socket, send_message)
            Logger_1.info(f"Message from {sender} forwarded to chat members")
        except Exception as e:
            Logger_1.error(f"Error forwarding message to chat members: {e}")
        finally:
            multicast_socket.close()

def main():
    # Hauptfunktion zum Starten des Servers
    server = Server()
    server.start_server()

if __name__ == "__main__":
    main()
