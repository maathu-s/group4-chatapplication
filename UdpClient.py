import os
import sys
import time
import socket
import struct
import threading
import queue

MULTICAST_IP = '224.1.1.1'

MULTICAST_PORT = 10001
UNICAST_PORT   = 50001

host_name = None
host_ip = None

uid = None # the clients UID
server_ip = None # the server's ip address
shutdown = False


def send_mc(msg):

	mc_addr = (MULTICAST_IP, MULTICAST_PORT)

	with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
		sock.settimeout(0.3)

		ttl = struct.pack('b', 1)  # '1': do not pass the network
		sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		print(f'send multicast msg: {msg}', flush=True)
		sock.sendto(msg.encode(), mc_addr)  # encoding defaults to 'utf-8'

		try:
			msg, addr = sock.recvfrom(128)
		except socket.timeout:
			return None
		else:
			answer = msg.decode()
			print(f"received from {addr}: {answer}")
			if ':' in answer:
				srv_msg, srv_ip = answer.split(':')
				return srv_ip if srv_msg == 'Server IP' else None
			return None	



def unicast_socket(q):
	global server_ip
	
	with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

		server_addr = (server_ip, UNICAST_PORT)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.settimeout(0.5)

		print(f"unicast socket started")
		while True:

			# anything to send?
			if not q.empty():
				msg = q.get()
				try:
					sock.sendto(msg.encode(), server_addr)
				except:
					pass

			# anything received?
			try:
				data, addr = sock.recvfrom(1024)  # byte data

			except socket.timeout:
				if shutdown:
					break
			else:
				msg = data.decode()
				if msg.startswith('Server IP:'):
					server_ip = msg.split(':')[1]
					print(f' [server leader has changed!]')
				else:
					print(msg)
					print('>', end='', flush=True)

	print('unicast socket closed')


def main():
	global shutdown
	global uid
	global server_ip
	global host_name
	global host_ip

	host_name = socket.gethostname()
	host_ip = socket.gethostbyname(host_name)
	uid = os.getpid()
	print(f'My Client Host: {host_name}, IP: {host_ip}, UID: {uid}')

	server_ip = send_mc(f'service discovery over multicast by {uid}')

	q = queue.Queue()

	if server_ip is None:
		print('please start a server first!')
		return 1
	else:
		print('current server ip address:', server_ip)


	t1 = threading.Thread(target=unicast_socket, args=(q,))
	t1.start()

	time.sleep(0.2)

	intro = '''\
----------------------------------------------------------------
ready to enter commands and messages.
commands:
  join <name> : join the chat
  leave       : leave the chat
  <message>   : send a chat message
  end         : terminate client appl  
----------------------------------------------------------------\
	'''
	print(intro)
	while True:
		msg = input('>') # block!
		if msg == 'end':
			shutdown = True
			break
		else:
			q.put(msg)

	t1.join() 

	return 0



if __name__ == '__main__':
	main()
