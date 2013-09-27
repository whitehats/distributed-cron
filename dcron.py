#!/usr/bin/env python
import memcache
import hashlib
import datetime
import calendar
import sh

# Timeout for action in seconds.
TIMEOUT = 21600 
# Address for memcache
ADDRESS = '127.0.0.1:11211'
# File with data
FILE_NAME = "schedule.txt"


def datetime_to_seconds(_datetime):
    timestamp = int(calendar.timegm(_datetime.utctimetuple()))
    return timestamp


def main():
    process_list=[]    
    mc = memcache.Client([ADDRESS], debug=0)

    def run(action):
        x = sh.bash(c=action, _bg=True, _timeout=TIMEOUT)
        process_list.append(x)


    with open(FILE_NAME, 'r') as fdane:
        for line in fdane:
            hashed = hashlib.sha224(line).hexdigest()
            mins, action = line.split(None, 1)
            mins = float(mins)
            nowdate = datetime.datetime.utcnow()
            mcdata = mc.get(hashed)

            if mcdata == None:
                mc.set(hashed, nowdate)
                print "Memcache entry does'nt exist, putting new to memory."
                run(action)
            elif mcdata + datetime.timedelta(minutes=mins) <= nowdate:
                mc.set(hashed, nowdate)
                print "Time left for %s, starting again."%mcdata
                run(action)
            elif abs(datetime_to_seconds(nowdate) - datetime_to_seconds(mcdata)) > \
                2*datetime.timedelta.total_seconds(datetime.timedelta(minutes=mins)):
                mc.set(hashed, nowdate)
                print "Wrong time-range for %s, starting again."%mcdata
                run(action)

    for elem in process_list:
        try:
            elem.wait()
        except sh.SignalException_9 :
            print "Error:  ", elem, "  action timeout!"

main()
