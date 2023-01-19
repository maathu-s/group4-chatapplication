import os
import sys
import socket
import struct
import time
import threading
import queue

MULTICAST_IP = '224.1.1.1'

MULTICAST_PORT = 10001
UNICAST_PORT   = 50001

T_HEARTBEAT = 3 # heartbeat period in seconds
T_WATCHDOG = 5  # heartbeat failure alert after seconds

host_name = None
host_ip = None

shutdown = False
leader = False
my_uid = None
server_ip = None


chatroom = dict() # client address -> user name
srv_group = dict() # server address

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
			else:
				return None	


def multicast_listener():

	srv_addr = (host_ip, MULTICAST_PORT)

	with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind(srv_addr)
		sock.settimeout(1.0)

		mreq = struct.pack('=4sL', socket.inet_aton(MULTICAST_IP), socket.INADDR_ANY)
		sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

		print(f'multicast receiver ready for input at {srv_addr}')
		while True:
			try:
				msg, addr = sock.recvfrom(1024) # block until timeout!
			except socket.timeout:
				if shutdown:
					break
			else:
				if leader:
					print(f"multicast received from {addr}: {msg.decode()}")
					sock.sendto(f"Server IP:{host_ip}".encode(), addr)

	print(f'multicast receiver closed')


def unicast_socket(que, hb_evt, wd_evt):
	global leader
	global server_ip
	
	# internal function for server leader to update the status info for each server member
	def update_status(sock):
		for srv_addr in srv_group:
			sock.sendto('clear status'.encode(), srv_addr) # clear old status informations
			for addr, name in chatroom.items():
				ip, port = addr
				sock.sendto(f'client member:{ip}:{port}:{name}'.encode(), srv_addr)
			for addr, uid in srv_group.items():
				ip, port = addr
				sock.sendto(f'server member:{ip}:{port}:{uid}'.encode(), srv_addr)
	
	
	# outer loop to change the server role on demand
	while True:
		
		# leader server
		if leader:

			with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

				my_addr = (host_ip, UNICAST_PORT)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				sock.bind(my_addr) 
				sock.settimeout(0.3)

				print(f'unicast socket for server leader started')
				while True:

					try:
						data, addr = sock.recvfrom(1024) # data = encoded message, addr = (ip, port) of sender
					except socket.timeout:
						if shutdown:
							break
					else:
						msg = data.decode()
						print(f"received from {addr}: {msg}")

						#-- wanna join to server group?
						if msg.startswith('join server:'):
							# add the new member to the group
							srv_msg, srv_uid = msg.split(':')
							srv_group[addr]=int(srv_uid)
							print(f'UID={srv_uid} joins to server group')
							print('  current members:', srv_group)

							# update all status information to all servers
							# remark: for simplicity and reliability the complete data will be updated!
							update_status(sock)

							# acknowledge membership to enable watchdog
							sock.sendto('ack join'.encode(), addr)

						#-- wanna leave server group?
						elif msg.startswith('leave server:'):
							if addr in srv_group:
								# remove this server from the group
								uid = srv_group[addr]
								del srv_group[addr]
								print(f'UID={uid} leaves the server group')
								print('  current members:', srv_group)

								# update all status information to all servers
								# remark: for simplicity and reliability the complete data will be updated!
								update_status(sock)
								
								# acknowledge membership end to disable watchdog
								sock.sendto('ack leave'.encode(), addr)

						#-- wanna join the chat?
						elif msg.startswith('join'):
							if addr not in chatroom:
								# extract name from message
								name = msg.replace('join', '').strip() # remove 'join' and whitespace
								# valid name?
								if name:
									msg = f'info: {name} joins the chat'
									
									# inform all other clients
									for client_addr, client_name in chatroom.items():
										sock.sendto(msg.encode(), client_addr)	

									# add this client to chat
									chatroom[addr] = name
									print(msg, ' current members:', chatroom)

									# inform server group about new client
									update_status(sock)

									# acknowledge joining
									sock.sendto('info: you join the chat!]'.encode(), addr)
								else:
									sock.sendto('info: please join with your name!'.encode(), addr)
								
						#-- wanna leave the chat?
						elif msg == 'leave':
							if addr in chatroom:
								name = chatroom[addr]
								del chatroom[addr]
								msg = f'info: {name} leaves the chat'
								print(msg, ' current members:', chatroom)
								
								# inform all other clients
								for client_addr, client_name in chatroom.items():
									sock.sendto(msg.encode(), client_addr)

								# inform server group
								update_status(sock)

								# acknowledge joining
								sock.sendto('info: you leave the chat!'.encode(), addr)

						#-- seems to be a chat message
						else:
							if addr in chatroom:
								name = chatroom[addr]
								for client_addr, client_name in chatroom.items():
									if client_addr != addr:	# skip the message to itself
										print(f'send chat message from {name} to {client_name} [{client_addr}]')
										sock.sendto(f'[{name}] {msg}'.encode(), client_addr)
							else: 
								sock.sendto('info: you are not in a chatroom'.encode(), addr)  # echo

				print('unicast socket for server leader stopped')

		# monitoring server
		else:
			with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

				server_addr = (server_ip, UNICAST_PORT)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				sock.settimeout(0.3)

				print("unicast socket for monitoring server started")
				while True:
					
					# anything to send?
					if not que.empty():
						msg = que.get()
						if not leader:						# skip message from leader to itself
							if msg in ['join', 'leave']: 	# valid command?
								msg += f' server:{my_uid}'		# append 'server' and UID to message to join/leave server group
								try:
									sock.sendto(msg.encode(), server_addr)
								except:
									pass

					# anything received?
					try:
						data, addr = sock.recvfrom(1024) # byte data, addr = (ip, port) of sender
					except socket.timeout:
						if leader:
							break
						if shutdown:
							break
					else:
						msg = data.decode()
						#print(f"received from {addr}: {msg}")
						
						if msg == 'heartbeat':
							hb_evt.set()   				# set flag for each arrival of a heartbeat message

						elif msg == 'ack join':
							wd_evt.set()   				# set flag to enable the watchdog

						elif msg == 'ack leave':
							wd_evt.clear() 				# clear flag to disable the watchdog
							chatroom.clear() 			# destroy chatroom info
							srv_group.clear() 			# destroy server group info
							
						elif msg == 'clear status':
							print('clear status information')
							chatroom.clear()
							srv_group.clear()
							
						elif msg.startswith('client member:'):
							msg, ip, port, name = msg.split(':')
							addr = (ip, int(port))
							print(f'add client member {addr}, {name}')
							chatroom[addr]=name

						elif msg.startswith('server member:'):
							msg, ip, port, uid = msg.split(':')
							addr = (ip, int(port))
							print(f'add server member {addr}, {uid}')
							srv_group[addr]=int(uid)
							
						elif msg.startswith('Server IP:'):
							server_ip = msg.split(':')[1]
							print(f'[server leader has changed!]')							
				
				print("unicast socket for monitoring server stopped")
				
		if shutdown:
			break

	print('unicast socket terminated')


def heartbeat_watchdog(hb_evt, wd_evt):
	global leader
	'''
	check server role. 
	- if server has a leader role, start 'heartbeat' system
	- if server isn't the leader, start monitoring system 
	'''
	def leader_election():
		'''
		in srv_group are all candidates for next leadership and the UIDs
		- return False if any other conditate has a higher UID
		- return True if the own UID is the highest.
		'''
		print('candidates:')
		uid_max = 0
		for addr, uid in srv_group.items():
			print('  ', addr, uid)
			if uid > uid_max: uid_max = uid
		
		return False if uid_max > my_uid else True
		
	with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		# outer loop to change the server role on demand
		while True:
		
			if leader:

				# inner loop to send heartbeat. Leave if shutdown
				print('start heartbeat system')
				while True:

					if shutdown:
						break

					# send heartbeat if at least one server monitor present
					if srv_group:  
					
						# send heartbeat to each server group member (monitor)
						print('.', end='', flush=True)
						for member_addr in srv_group:
							sock.sendto('heartbeat'.encode(), member_addr)

					time.sleep(T_HEARTBEAT)

			else:
				# inner loop to detect heartbeat. Leave if shutdown or role has changed after leader election
				print('start monitor system')
				while True:

					if shutdown:
						break

					# monitoring system starts after leader (server) acknowledged the membership to server group
					if wd_evt.is_set():
						
						# wait for the leader's heartbeat (alive=true) or a timeout (alive=false)
						alive = hb_evt.wait(T_WATCHDOG) 
						
						if not wd_evt.is_set(): # check flag again; may have changed by now
							print('watchdog disabled')
							hb_evt.clear()
						elif alive:
							print('watchdog: leader alive')
							# reset the event flag
							hb_evt.clear()
						else:
							# no heartbeat within watchdog period. 
							print('start leader election')
							i_am_new_leader = leader_election()
							if i_am_new_leader:
								# stop watchdog!
								wd_evt.clear() 
							
								# remove own address from monitoring (server) group
								for member_addr, member_uid in srv_group.items():
									if member_uid == my_uid:
										del srv_group[member_addr]
										break
								# inform chat members about new situation
								for server_addr in srv_group:
									print(f'inform server {server_addr} about new leader')
									sock.sendto(f'Server IP:{host_ip}'.encode(), server_addr)

								for client_addr in chatroom:
									print(f'inform client {client_addr} about new leader')
									sock.sendto(f'Server IP:{host_ip}'.encode(), client_addr)
								leader = True
								print("=== I am the server leader ===")
								print('leave monitoring system')
								break

					else:
						time.sleep(1) # do nothing, check again after delay

			if shutdown:
				break

	print('heartbeat/watchdog terminated')
			

def main():
	global shutdown
	global my_uid
	global server_ip
	global host_name
	global host_ip
	global leader
	
	host_name = socket.gethostname()
	host_ip = socket.gethostbyname(host_name)
	my_uid = os.getpid()
	print(f'My Server Host: {host_name}, IP: {host_ip}, UID: {my_uid}')

	server_ip = send_mc(f'service discovery over multicast by {my_uid}')

	if server_ip is None:
		print("=== I am the server leader ===")
		leader = True
	else:
		print("--- I am a server monitor ---")
		print("current server ip address:", server_ip)


	# use a queue object to push new messages to the unicast_socket thread
	# use event objects to communicate between threads (unicast_socket -> haearbeat_watchdog)
	#  - the unicast_socket thread listens the heartbeat and sets the hb_evt
	#  - the watchdog thread is waiting for the hb_evt in time or initiates an alert
	q = queue.Queue() # message queue
	hb_evt = threading.Event() # heartbeat event, initially false
	wd_evt = threading.Event() # enable watchdog event, initially false

	t1 = threading.Thread(target=unicast_socket, args=(q, hb_evt, wd_evt))
	t1.start()
	t2 = threading.Thread(target=heartbeat_watchdog, args=(hb_evt, wd_evt) )
	t2.start()
	t3 = threading.Thread(target=multicast_listener)
	t3.start()

	
	time.sleep(0.2)

	intro = '''\
----------------------------------------------------------------
ready to enter commands and messages.
commands:
  join      : join to server group
  leave     : leave the server group
  end       : terminate server app
----------------------------------------------------------------\
	'''
	print(intro)
	while True:
		msg = input('')
		if msg == 'end':
			shutdown = True
			break
		else:
			q.put(msg)

	# wait until all threads terminate
	t1.join() 
	t2.join()
	t3.join()

	return 0


if __name__ == '__main__':
	sys.exit( main() )
