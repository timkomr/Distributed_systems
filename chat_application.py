import concurrent.futures
import threading
import os
import psutil
import json
import logging
import socket
import struct


BUF_SIZE = 2048
MULTICAST_BUF_SIZE = 20480
MULTICAST_PORT = 60002 
MULTICAST_GROUP = '225.0.0.3'
BROADCAST_PORT = 60000  
BROADCAST_ADDR = '255.255.255.255'
TCP_PORT = 60001
TCP_TIMEOUT = 4
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def get_network_ip():
    """Get the network IP address of the current machine."""
    try:
        os_type = os.name
        iface = "Ethernet" if os_type == "nt" else "en0"
        idx = 1 if os_type == "nt" else 0

        for intf, addr_list in psutil.net_if_addrs().items():
            if intf == iface:
                return addr_list[idx][1]
    except:
        return None

IP_ADDR = get_network_ip()
        
class ChatApp:
    def __init__(self):
        """Initialize the ChatApp with necessary attributes."""
        self.stop_event = threading.Event()
        self.workers = []

    # Section: Application Lifecycle
    def launch_app(self):
        """Launch the chat application and start necessary threads."""
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as thread_pool:
            self.workers.append(thread_pool.submit(self.user_interface))
            self.workers.append(thread_pool.submit(self.receive_messages))
            log.info('Client has started!')

            try:
                while not self.stop_event.is_set():
                    self.stop_event.wait(1)
            except KeyboardInterrupt:
                log.info("Client shutdown initiated. Goodbye!")
                self.stop_event.set()
            finally:
                for worker in self.workers:
                    worker.cancel()
                thread_pool.shutdown(wait=True)

    def user_interface(self):
        """Provide a user interface for interacting with the chat application."""
        menu_options = {
            "1": ("Join chat to talk to your friends", self.join_room),
            "2": ("Send a chat message", self.send_chat),
            "3": ("Leave chat and say goodbye to your friends", self.exit_room)
        }

        while not self.stop_event.is_set():
            print("\n--- Chat Application Menu ---")
            for key, (description, _) in menu_options.items():
                print(f"{key} - {description}")
            print("q - Quit the application")

            user_input = input("Select an option: ").strip().lower()
            if user_input == "q":
                log.info("Exiting the chat application. Goodbye!")
                self.stop_event.set()
            elif user_input in menu_options:
                _, action = menu_options[user_input]
                action()
            else:
                print("Invalid option. Please try again.")

    # Section: Message Handling
    def receive_messages(self):
        """Receive messages from the multicast group and display them in a formatted manner."""
        log.info('Opening socket for incoming chat messages...')
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
                udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                udp_socket.bind(('', MULTICAST_PORT))
                mreq = struct.pack('4sL', socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
                udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

                while not self.stop_event.is_set():
                    try:
                        message_data, message_addr = udp_socket.recvfrom(BUF_SIZE)
                        self.display_message(message_addr, message_data)
                    except socket.timeout:
                        continue
                    except socket.error as socket_err:
                        log.error(f'Socket error: {socket_err}')
                    except Exception as general_err:
                        log.error(f'Unexpected error: {general_err}')
        except Exception as receive_err:
            log.error(f'Error in receive_messages: {receive_err}')

    def display_message(self, address, data):
        """Format and display the received message."""
        try:
            message = data.decode("utf-8")
            print(f"\n--- New Message ---\nFrom: {address}\nMessage: {message}\n-------------------")
        except Exception as display_err:
            log.error(f'Error displaying message: {display_err}')

    def send_to_server(self, msg):
        """Send a message to the server using TCP with retries and error handling."""
        max_attempts = 3
        for attempt in range(max_attempts):
            if self.stop_event.is_set():
                log.info('Operation cancelled.')
                return

            server_ip = self.discover_server()
            if server_ip:
                log.info(f'Attempting to send message to server {server_ip} (Attempt {attempt + 1}/{max_attempts})')
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_socket:
                        tcp_socket.settimeout(TCP_TIMEOUT)
                        tcp_socket.connect((server_ip, TCP_PORT))
                        tcp_socket.sendall(msg.encode('utf-8'))
                        server_response = tcp_socket.recv(BUF_SIZE)
                        log.info(f'Server response: {server_response.decode("utf-8")}')
                        return
                except socket.error as tcp_err:
                    log.error(f'Socket error: {tcp_err}')
                except Exception as send_err:
                    log.error(f'Error in send_to_server: {send_err}')
            else:
                log.warning(f'Failed to discover server (Attempt {attempt + 1}/{max_attempts})')

        log.error('Unable to connect to server after multiple attempts. Please try again later.')

    # Section: Server Discovery
    def discover_server(self):
        """Broadcast a message to find the leading server and return its IP address."""
        log.info('Starting server discovery...')
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as broadcast_socket:
                broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                broadcast_socket.settimeout(3)
                broadcast_socket.sendto(IP_ADDR.encode(), (BROADCAST_ADDR, BROADCAST_PORT))
                log.debug('Broadcast message for server discovery sent.')

                while True:
                    try:
                        server_response, server_addr = broadcast_socket.recvfrom(BUF_SIZE)
                        log.info(f'Server discovered at {server_addr[0]}')
                        return server_addr[0]
                    except socket.timeout:
                        log.warning('Server discovery timed out. Retrying...')
                        break
        except Exception as discover_err:
            log.error(f'Error during server discovery: {discover_err}')
        return None

    # Section: Chat Room Actions
    def join_room(self):
        """Join the chat room and notify the server."""
        log.info("Attempting to join the chat room...")
        try:
            join_message = json.dumps({"function": "join"})
            self.send_to_server(join_message)
            print("Successfully joined the chat room!")
        except Exception as join_err:
            log.error(f'Error joining the chat room: {join_err}')

    def send_chat(self):
        """Prompt the user to enter a chat message and send it to the server."""
        log.info("Preparing to send a chat message...")
        try:
            user_msg = input("Enter your message: ")
            chat_message = json.dumps({"function": "chat", "msg": user_msg})
            self.send_to_server(chat_message)
            print("Message sent successfully!")
        except Exception as chat_err:
            log.error(f'Error sending chat message: {chat_err}')

    def exit_room(self):
        """Leave the chat room and notify the server."""
        log.info("Attempting to leave the chat room...")
        try:
            leave_message = json.dumps({"function": "leave"})
            self.send_to_server(leave_message)
            print("You have left the chat room.")
        except Exception as exit_err:
            log.error(f'Error leaving the chat room: {exit_err}')

def main():
    """Main function to start the chat application."""
    chat_app = ChatApp()
    chat_app.launch_app()

if __name__ == "__main__":
    main()
