import socket
import sys
import os
import time
import datetime
import random
import math
from datetime import datetime, timedelta

def reg2bs(bs_address):							#Function for Registration
        regcmd = "REG" + ' ' + myip + ' '  + str(myport) + ' ' + myname
        reglen = len(regcmd) + 5
        regcmd = "00" + str(reglen) + ' ' + regcmd
        sock.sendto(regcmd, bs_address)
        data, bs_address = sock.recvfrom(4096)
        data = data[5:]            
        if (data == 'BS REQ -9999'):					#Displaying all the possible error commands
                print 'Unknown command, undefined characters to BS'
        elif (data == 'REGOK KA -1'):
                print 'unknown REG command'
        elif (data == 'REGOK KA 9999'):
                print 'error in registering'
        elif (data == 'REGOK KA 9998'):
                print 'already registered with bs'
        else:
                print 'REGISTERED TO BS'
		log.write("\nREGISTERED TO BS: ")
		log.write(myip)
		log.write(" at time: ")
		log.write(str(datetime.now()))
                splitfinal = data.split(" ")
                splitfinal[2] = int(splitfinal[2])
                ip1 = '0'
                ip2 = '0'
                ip3 = '0'
                port1 = '0'
                port2 = '0'
                port3 = '0'
                num = str(splitfinal[2])
                if (splitfinal[2] == 3):
                        ip3 = str(splitfinal[7])
                        port3 = str(splitfinal[8])
                        splitfinal[2] -= 1
                        print 'Received Node3 '
                if (splitfinal[2] == 2):
                        ip2 = str(splitfinal[5])
                        port2 = str(splitfinal[6])
                        splitfinal[2] -= 1
                        print 'Received Node2 '
                if (splitfinal[2] == 1):
                        ip1 = str(splitfinal[3])
                        port1 = str(splitfinal[4])
                        print 'Received Node1 '	    
                if (splitfinal[2] == 0):
                        print 'This is the first node'

        listpass = [num, ip1, port1, ip2, port2, ip3, port3]
        return(listpass)

def unreg2bs(bs_address):					#Function for Unregistration
        unregcmd = "DEL IPADDRESS" + ' ' + myip + ' '  + str(myport) + ' ' + myname
        unreglen = len(unregcmd) + 5
        unregcmd = "00" + str(unreglen) + ' ' + unregcmd
        print 'UNREGISTERING TO BS'
	log.write("\nUNREGISTERED TO BS")
	log.write(myip)
	log.write(" at time: ")
	log.write(str(datetime.now()))
        sock.sendto(unregcmd, bs_address)
        data, bs_address = sock.recvfrom(4096)
        data = data[5:]
        if (data == 'BS REQ -9999'):
                print 'Unknown command, undefined characters to BS'
                return
        elif (data == ('DEL IPADDRESS OK -1' or 'DEL OK -1')):
                print 'Error in DEL command'
                return
        elif (data == 'DEL IPADDRESS OK ' + myname + ' 9998'):
                print '(IPAddress + Port not registered for username'
                return
        else:
                print 'UNREGISTER Successful'
        return

def unregname2bs(bs_address):					#Function for Deleting Username
	unregnamecmd = "DEL UNAME" + ' ' + myname			
	unregnamelen = len(unregnamecmd) + 5
	unregnamecmd = "00" + str(unregnamelen) + ' ' + unregnamecmd
	sock.sendto(unregnamecmd, bs_address)
	
	data, bs_address = sock.recvfrom(4096)				
	data = data[5:]           
	if (data == 'BS REQ -9999'):
		print 'Unknown command, undefined characters to BS'
		return
	elif (data == ('DEL UNAME OK ' + myname + ' -1' or 'DEL OK ' + myname + ' -1')):
		print 'Error in DEL command'
		return
	elif (data == 'DEL UNAME OK 9999'):
		print '(IPAddress + Port not registered for username'
		return
	else:
		print 'USER NAME unregister successful'
		log.write("\nUSER NAME unregister successful")
		log.write(myip)
		log.write(" at time: ")
		log.write(str(datetime.now()))
        return

def join_ds(listpass, dict):
        num = int(listpass[0])
        ip = [0, 0, 0, 0, 0]
        port = [0, 0, 0, 0, 0]
        node = [(0, 0), (0, 0), (0, 0), (0, 0)]
	
	def join(node, dict):					#Function within a function
                joincmd = "JOIN" + ' ' + myip + ' '  + str(myport)		
                joinlen = len(joincmd) + 5
                joincmd = "00" + str(joinlen) + ' ' + joincmd
                sock.sendto(joincmd, node)
                data, node = sock.recvfrom(4096)
                data = data[5:]        
                if (data == 'BS REQ -9999'):
                        print 'Unknown command, undefined characters to BS'
                        return
                else:
                        print 'Node Join Successful: ', node
			log.write("\nNode Join Successful. Routing table : ")
			log.write(str(dict))
			log.write(" at time: ")
			log.write(str(datetime.now()))
                        ip, port = node
                        if not dict:
                                dict = {ip : port}
                        else:
                                dict[ip] = port
                        print "Current entries in Routing Table: ", dict
                return dict                       
        if (num == 0):
                print "No other nodes to join"
                return
        else:
                for i in range (1, num+1):
                        if (i == 1):
                                j = 1
                        elif (i == 2):
                                j = 3
                        elif (i == 3):
                                j = 5
                        else:
                                print "Please try again"
                        ip[i] = listpass[j]
                        port[i] = int(listpass[j+1])
                        node[i] = (ip[i], port[i])
                for i in range (1, num+1):
                        dict = join(node[i], dict)			
        return dict

def search():							#Function for Search
	def nsq(searchkey):
		print searchkey
		targent = open('entries.txt', 'r')
		temp = targent.read().splitlines()
		for line in temp:
			if (searchkey == line):
				print "File found at my node, Hopcount is 0 ", searchkey
				log.write("\nFile found at my node, Hopcount is 0  : ")
				log.write(searchkey)
				log.write(" at time: ")
				log.write(str(datetime.now()))
				dag = "File found at my node, Hopcount is 0 " + searchkey
				slogf.write(dag) 
				slogf.write("\n") 
				return 
		targent.close()
		scmd = "SER" + ' ' + myip + ' ' + str(myport) + ' ' + searchkey + ' ' + str('0')
		scmd = "00" + str(len(scmd)) + ' ' + scmd
		stl = time.time()
		for key, value in dict.iteritems():
			outad = (key, value)
		        sock.sendto(scmd, (key, int(value)))
			log.write("\nSearch query generated : ")
			log.write(str(outad))
			log.write(" at time: ")
			log.write(str(datetime.now()))
		h = 15						#Maximum hop count
		print "Waiting for SEARCH OK"
		data, address = sock.recvfrom(4096)	
		data = data[5:]
		splitfinal = data.split(" ")
		if (splitfinal[0] == "SER"):
			print "Packet drop"
		elif(splitfinal[0] == "SEROK"):
			if (int(splitfinal[4]) < h):
				print "Search successful: ", data
				slogf.write(data) 
				slogf.write("\n")
				endl = time.time()
				lat = endl - stl
				llog.write(str(lat))
				llog.write("\n")
		else:
			print "NOT FOUND"
		if (h > (int(splitfinal[4]) + 1)):
			h = int(splitfinal[4])	
		return
	tresource = open('resources.txt', 'r')
    	tmed = open('med.txt', 'w')
    	for line in tresource:
        	if (line[0] == '#'):
            		print ""
       		else:
        		tmed.write(line)
    	tresource.close()
    	tmed.close()
    
    	tresource = open('med.txt', 'r')
    	tmed = open('rank.txt', 'w')
    	n = 1
   	for line in tresource:
        	if (n < 10):
            		var = "00" + str(n) + line
            		tmed.write(var)
        	elif ((n<100) and (n>9)):
           		var = "0" + str(n) + line
            		tmed.write(var)
        	else:
            		var = str(n) + line
            		tmed.write(var)
        	n = n+1
    	tresource.close()
   	tmed.close()
	os.remove('med.txt')
								#Taking input from users
	s = float(raw_input("Enter s value: "))					
	N = float(raw_input("Enter number of queries to be generated: "))
	trank = open('rank.txt', 'r')
    	tquery = open('queries.txt', 'w')
	a = 0
	sumf = 0.00
	for i in range (1, 161):
		a = a + (1/(math.pow(i, s)))
	for line in trank:
		k = int(line[:3])
		freq = float((1/(math.pow(k, s)))/a)		#Zipf Law Implementation
		f = (freq*N)
		fround = round(f)
		froundi = int(fround)
		for i in range(1, froundi + 1):
			tquery.write(line[3:])
		sumf += fround
	trank.close()
	tquery.close()
	lines = open('queries.txt').readlines()
	random.shuffle(lines)
	target5 = open('queries.txt', 'rw+').writelines(lines)
	target5 = open('queries.txt', 'r')
	tar = target5.read().splitlines()
	for line in tar:
		print line
		nsq(line)
	return

if __name__=='__main__':
        dict = {}						#Dictionary for Routing Table
	bs_address = (sys.argv[4],int(sys.argv[6]))
	myip = socket.gethostbyname(socket.gethostname())
        myport = int(sys.argv[2])
        myadd = (myip, myport)  
        myname = 'KA'
	old = open('old.txt', 'w')
	old.flush()
	old.close()
	slogf = open('slog.txt','w')
	log = open('hostname.log', 'w')
	dlog = open('delay.txt','w')				
	llog = open('latency.txt', 'w')
	m = open('entries.txt', 'r')
	log.write("Entries in this host: ")	
	for line in m:
		log.write(line)
	sok = ""
        while True:
      		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(myadd)
                print '1: REG, 2: UNREG, 3: DELETE UNAME, 4: JOIN DS, 5: LEAVE DS, 6: SEARCH, 7: LISTEN, 8: ROUTE, 9: TEARDOWN'
                select = raw_input()
                if (select == "1"):
                        listpass = reg2bs(bs_address)
                elif (select == "2"):
                        unreg2bs(bs_address)
                elif (select == "3"):
                        unregname2bs(bs_address)
                elif (select == "4"):
                        dict = join_ds(listpass, dict)
		elif (select == "6"):
                        search()
		elif (select == "5"):						#Implementation for LEAVE
			print "Leaving the Distributed System"
			unreg2bs(bs_address)
			leavemsg = "LEAVE" + " " + myip + " " + str(myport)
			leavemsg = "00" + str(len(leavemsg)) + " " +leavemsg
			for key, value in dict.iteritems():
		        	sock.sendto(leavemsg, (key, int(value)))
			for i in range(0, len(dict)):
				datalok, lad = sock.recvfrom(1024)
				(loip, loport) = lad
				splitfinal = datalok.split(" ")
				if (splitfinal[1] == "LEAVEOK"):
					del dict[loip]
			log.write("\nLeave DS successful: ")
			log.write(str(myadd))
			log.write(" at time: ")
			log.write(str(datetime.now()))
			print "Routing table entries after Leaving: ", dict
			print "All done to LEAVE DS"
		elif (select == "8"):
			print "Current entries in Routing Table: ", dict
		elif (select == "9"):						#Implementation for TEARDOWN
			print "Request for Teardown"
			unreg2bs(bs_address)
			tearmsg = "0000 TEARDOWN"
			for key, value in dict.iteritems():
		        	sock.sendto(tearmsg, (key, int(value)))
			dict.clear()
			log.write("\nTeardown Successful ")
			log.write(" at time: ")
			log.write(str(datetime.now()))
			print "Routing table entries after Teardown: ", dict
			print "Teardown"      
                elif (select == "7"):
                        while True:
				print "Waiting for connection..."
                                data, address = sock.recvfrom(1024)
                                data = data[5:]
                                splitfinal = data.split(" ")
                                if (splitfinal[0] == "JOIN"):
                                        jcmd = "0014 JOINOK 0"
                                        sock.sendto(jcmd, address)
                                        ip, port = address
                                        if not dict:
                                                dict = {ip : port}
                                        else:
                                                dict[ip] = port
					log.write("\nNode Join Successful. Routing Table: ")
					log.write(str(dict))
					log.write(" at time: ")
					log.write(str(datetime.now()))
                                        print "Current entries in Routing Table:", dict
                                        print 'Connected to Node: ', address

                                elif ((splitfinal[0] == "SER") and (int(splitfinal[len(splitfinal)-1]) < 10)):
					std = time.time()
                                        targent = open('entries.txt', 'r')
					log.write("\nSearch query received: ")
					log.write(str(address))
					log.write(" at time: ")
					log.write(str(datetime.now()))
					if (len(splitfinal) > 5):
						x = len(splitfinal) - 4
						sfilename = splitfinal[3]
						for i in range (1, x):
							sfilename = sfilename + ' ' + splitfinal[3+i]
					else:
						sfilename = splitfinal[3]
					shop = int(splitfinal[len(splitfinal)-1])
                                        sip = splitfinal[1]
                                        sport = int(splitfinal[2])
                                        sval = 0
					temp = targent.read().splitlines()
                                        for line in temp:
                                                if (sfilename == line):
                                                        searchlist = line
                                                        sval = sval + 1
                                        targent.close()
                                        if (sval > 0):
						isok = sip + " " + str(sport) + " " + sfilename
						if (isok != sok):
		                                        sercmd = "SEROK" + ' ' + str(sval) + ' ' + str(myip) + ' ' + str(myport) + ' ' + str(shop + 1)
		                                        for i in range(0, sval):
		                                                sercmd = sercmd + ' ' + searchlist
		                                        sreslen = len(sercmd)                            
		                                        if (sreslen < 100):
		                                                sercmd = "00" + str(sreslen) + ' ' + sercmd
		                                        else:
		                                                sercmd = "0" + str(sreslen) + ' ' + sercmd	
							sad = (sip, int(sport))
		                                        sock.sendto(sercmd, sad)
							endds = time.time()
							dels = endds - std
							dlog.write(str(dels))
							dlog.write("\n")
							sok = sip + " " + str(sport) + " " + sfilename  
							log.write("\nSearch query fulfilled: ")
							log.write(str(sad))
							log.write(" at time: ")
							log.write(str(datetime.now()))    
                                        else:
						old = open('old.txt', 'r')
						cipf = sip + " " + str(sport) + " " + sfilename
						c = 1
						for line in old:
							if (line == cipf):
								c = 0
						old.close()
						if (c != 0):
							if(shop < 5):                                                
								sercmd2 = "SER " + sip + ' ' + str(sport) + ' ' + sfilename + ' ' + str(shop + 1)
				                                sercmd2 = "00" + str(len(sercmd2)) + ' ' + sercmd2
				                                for key, value in dict.iteritems():
									sad = (sip, sport)
									oad = (key, int(value))
									if ((sad != oad) and (address != oad)):
				                                        	sock.sendto(sercmd2, oad)
									old = open('old.txt', 'w')
									ipf = sip + " " + str(sport) + " " + sfilename
									log.write("\nSearch query forwarded : ")
									log.write(str(oad))
									log.write(" at time: ")
									log.write(str(datetime.now()))
									old.write(ipf)
									old.close()
								enddf = time.time()
								delf = enddf - std
								dlog.write(str(delf))
								dlog.write("\n")						
				elif(splitfinal[0] == "LEAVE"):
					lip = splitfinal[1] 
					lport = int(splitfinal[2])
					ln = (lip, lport)
					for key, value in dict.iteritems():
						if (dict[lip] == lport):
							print "dict[lip]", dict[lip]
							del dict[lip]							
							lokmsg = "LEAVEOK" + " " + "0"
							lokmsg = "00" + str(len(lokmsg)) + " " + lokmsg
							break
							sock.sendto(lokmsg, (key, int(value)))
					log.write("\nLeave Successful: ")
					log.write(str(ln))
					log.write(" at time: ")
					log.write(str(datetime.now()))					
					print "After LEAVE: ", dict

				elif(splitfinal[0] == "TEARDOWN"):
					unreg2bs(bs_address)
					tearmsg = "0000 TEARDOWN"
					for key, value in dict.iteritems():
		        			sock.sendto(tearmsg, (key, int(value)))
					dict.clear()
					log.write("\nTeardown Successful: ")
					log.write(str(myadd))
					log.write(" at time: ")
					log.write(str(datetime.now()))
					print "Routing table entries after Teardown: ", dict
					print "Teardown"
					break		
                                else:
                                        print "Not connected in MAIN"
                else:
                        print 'Invalid entry. Please try again'    
	sock.close()    
