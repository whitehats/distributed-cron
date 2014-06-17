#!/usr/bin/env python
# -*- coding: utf-8 -*-
import hashlib
import logging
from datetime import datetime
import memcache
import sh
from croniter import croniter

# Timeout for action in seconds
TIMEOUT = 21600
# Address for memcache
ADDRESS = '127.0.0.1:11211'
# File with data
FILE_NAME = "schedule.txt"


def main():
    logging.basicConfig(level=logging.DEBUG)

    processes = []
    mc = memcache.Client([ADDRESS], debug=0)

    def run(action):
        processes.append(sh.bash(c=action, _bg=True, _timeout=TIMEOUT))

    with open(FILE_NAME, 'r') as f:
        for line in f:
            if line[0] == '#':
                continue

            hashed = hashlib.sha224(line).hexdigest()
            cron_spec, action = line.split(":", 1)
            action = action[:-1]
            cached_date = mc.get(hashed)
            now = datetime.utcnow()

            if cached_date is None:
                logging.info(
                    "No memcache entry for `{}`, new one. will be created"
                    .format(action)
                )
                cached_date = now
                mc.set(hashed, now)
                run(action)
                continue

            cron_entry = croniter(cron_spec, cached_date)
            new_date = cron_entry.get_next(datetime)

            logging.debug(
                "Attempting to run `{}` at `{}` "
                "(previous run at `{}`, expected next run at `{}`)"
                .format(action, now, cached_date, new_date)
            )
            if new_date <= now:
                logging.info(
                    "Running `{}` at `{}` (previous run at `{}`)"
                    .format(action, now, cached_date)
                )
                mc.set(hashed, now)
                run(action)

    for process in processes:
        try:
            logging.info("{}:STDOUT:\n{}".format(process.pid, process.stdout))
            logging.info("{}:STDERR:\n{}".format(process.pid, process.stderr))
        except sh.SignalException_9:
            logging.warning("`{}` timed out.".format(process))


main()
