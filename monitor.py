#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Yo, mon.
gblrenamémon is a monitoring script designed
to see if any global renames are taking an
extroardinary long time. Configuration options
are set in conf.py.

This is a standalone script that is intended
to be run as a cronjob.

(C) Kunal Mehta <legoktm@gmail.com>, 2014
Released under the terms of the GPL v2 (or higher) license.
"""

import calendar
import datetime
import os
import oursql
import subprocess
import time

import conf


def get_db(dbname):
    # Temp hack until migration thing finishes
    if dbname == 'centralauth':
        host = 'c2.labsdb'
    else:
        host = '%s.labsdb' % dbname

    return oursql.connect(
        db='%s_p' % dbname,
        host=host,
        read_defaults_file=os.path.expanduser('~/replica.my.cnf')
    )


def get_unix(mw_ts):
    return calendar.timegm(
        datetime.datetime.strptime(mw_ts, '%Y%m%d%H%M%S').utctimetuple()
    )


def get_all_current_renames():
    db = get_db('centralauth')
    with db.cursor(oursql.DictCursor) as cur:
        cur.execute("""
            SELECT
                ru_oldname,
                ru_newname,
                ru_wiki,
                ru_status
            FROM renameuser_status
        """)
        data = cur.fetchall()

    db.close()
    return data


def get_log_timestamp(db, newname):
    title = 'CentralAuth/' + newname
    with db.cursor(oursql.DictCursor) as cur:
        cur.execute("""
        SELECT
            log_timestamp
            user_name
        FROM logging
        JOIN user
        ON log_user=user_id
        WHERE log_type="gblrename"
        AND log_action="rename"
        AND log_namespace=-1
        AND log_title =?
        """, (title, ))
        data = cur.fetchall()
    if not data:
        return None
    return data[0]


def main():
    renames = get_all_current_renames()
    db = get_db('metawiki')
    mailer = Mailer(to=conf.TO_NOTIFY)
    for info in renames:
        ts = get_log_timestamp(db, info['ru_newname'])
        if not ts:
            # Uhhhhhhhh???
            ts = '999999999999999999'
        unix = get_unix(ts)
        if unix > (time.time() - conf.WAIT_UNTIL):
            mailer.send(info['ru_newname'], unix)


class Mailer:
    def __init__(self, to):
        self._list = None
        self.fname = 'mails.txt'
        self.to = to

    def send(self, username, age):
        if not self.have_sent(username):
            # Um this totally sucks. But subprocess
            # really, really wants a file object.
            with open('tmpfile.msg', 'w') as f:
                f.write('Error, The global rename for %s has been stuck for longer than 3 hours.' % username)

            for address in self.to:
                print 'Sending email to %s' % address
                subprocess.check_output(
                    ['mail', '-s', 'Global rename stuck', address],
                    stdin=open('tmpfile.msg', 'r')
                )
            self.mark_sent(username)

    def _load(self, force=False):
        if self._list is None or force:
            if os.path.isfile(self.fname):
                with open(self.fname, 'r') as f:
                    self._list = f.read().splitlines()

        return self._list

    def have_sent(self, username):
        self._load()
        return username in self._list

    def _save(self):
        with open('mails.txt', 'w') as f:
            f.write('\n'.join(self._list))

    def mark_sent(self, username):
        self._load(force=True)
        if username in self._list:
            # Race condition?
            return

        self._list.append(username)
        self._save()

if __name__ == '__main__':
    main()
