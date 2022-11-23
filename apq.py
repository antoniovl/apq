#!/usr/bin/env python
"""
Parse Postfix mailq and return a filtered list as JSON
"""

import sys, subprocess, re, time, datetime, argparse, json
from collections import OrderedDict

MONTH_MAP = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
UNIX_EPOCH = datetime.datetime(1970, 1, 1)
MAILQ_EXE = ["/usr/sbin/postqueue", "-p"]
MAILQ_EMPTY = "Mail queue is empty".lower()
# Status codes
ST_ACTIVE = "active"
ST_HELD = "held"
ST_DEFER = "deferred"


def call_mailq(args):
    """
    Call mailq and return stdout as a string
    """
    if not args.mailq_data:
        cmd = subprocess.Popen(MAILQ_EXE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = cmd.communicate()
        if cmd.returncode not in (0, 69):
            print("Error: mailq failed: \"{}\"".format(stderr.strip()), file=sys.stderr)
        if stdout:
            # Popen returns a binary string.
            stdout = stdout.decode('utf-8')
    else:
        with open(args.mailq_data, 'r') as f:
            stdout = f.read()
    return stdout.strip()


def parse_mq_old(args):
    """
    Parse mailq output and return data as a dict.
    """
    mailq_stdout = call_mailq(args)
    curmsg = None
    msgs = {}
    for line in mailq_stdout.splitlines():
        if not line or line[:10] == '-Queue ID-' or line[:2] == '--' or MAILQ_EMPTY in line.lower():
            continue
        if line[0] in '0123456789ABCDEF':
            s = line.split()
            curmsg = s[0]
            if curmsg[-1] == '*':
                status = 'active'
                curmsg = curmsg[:-1]
            elif curmsg[-1] == '|':
                status = 'held'
                curmsg = curmsg[:-1]
            else:
                status = 'deferred'
            msgs[curmsg] = {
                'size': s[1],
                'rawdate': ' '.join(s[2:6]),
                'sender': s[-1],
                'reason': '',
                'status': status,
                }
        elif '@' in line:  # XXX: pretty dumb check
            msg = msgs[curmsg]
            if not msg.get("recipients"):
                msg["recipients"] = [line.strip()]
            else:
                msg["recipients"].append(line.strip())
        elif line.lstrip(' ')[0] == '(':
            msgs[curmsg]['reason'] = line.strip()[1:-1].replace('\n', ' ')
        else:
            print("Error: Unknown line in mailq output: %s" % line, file=sys.stderr)
            sys.exit(1)
    return msgs


def _append_recipients(reason, addresses, recipients):
    if len(addresses) > 0:
        recipient = {
            "addresses": addresses
        }

        if reason:
            recipient["reason"] = reason

        recipients.append(recipient)


def parse_mq(args):
    """
    Parse mailq output and return data as a dict.
    :param args:
    :return:
    """
    MQ_STATE_HDR = 0
    MQ_STATE_MSG_DATA = 1
    MQ_STATE_RCPT = 2
    MQ_STATE_REASON = 3
    MQ_STATE_DONE = 4
    HEX_DIGITS = '0123456789ABCDEF'
    msgs = OrderedDict()
    msg = None
    queue_id = None
    addresses = None
    reason = None

    def _quit(msg, exit_code=1):
        print(msg, file=sys.stderr)
        sys.exit(exit_code)

    mailq_stdout = call_mailq(args)
    state = MQ_STATE_HDR

    for line in mailq_stdout.splitlines():
        if state == MQ_STATE_DONE:
            _quit("Unexpected input: %s".format(line))
        elif not line or line[:10] == '-Queue ID-':
            if state == MQ_STATE_RCPT:
                _append_recipients(reason, addresses, recipients)
                msg["recipients"] = recipients
                state = MQ_STATE_MSG_DATA
            continue
        elif line.lower() == MAILQ_EMPTY:
            if state != MQ_STATE_HDR:
                _quit("Unexpected input: %s".format(line))
            # Mailq Empty.
            return msgs
        elif line[:2] == "--":
            if state != MQ_STATE_REASON and state != MQ_STATE_MSG_DATA:
                _quit("Expected delay reason, got {}".format(line))
                sys.exit(1)
            # This should be the last line, it will be safe to just continue.
            state = MQ_STATE_DONE
            continue
        elif line[0] in HEX_DIGITS:
            if state == MQ_STATE_RCPT:
                # this is a new message
                msg["recipients"] = recipients
            s = line.split()
            queue_id = s[0]
            if queue_id[-1] == '*':
                status = ST_ACTIVE
                queue_id = queue_id[:-1]
            elif queue_id[-1] == '|':
                status = ST_HELD
                queue_id = queue_id[:-1]
            else:
                status = ST_DEFER
            msg = {
                'size': s[1],
                'rawdate': ' '.join(s[2:6]),
                'sender': s[-1],
                'status': status,
            }
            msgs[queue_id] = msg
            state = MQ_STATE_RCPT if status == ST_ACTIVE else MQ_STATE_REASON
            recipients = []
            addresses = []
            continue
        elif line.lstrip(' ')[0] == '(':
            if state != MQ_STATE_REASON and state != MQ_STATE_RCPT:
                _quit("Unexpected state for input \"{}\"".format(line.strip()))
            if state == MQ_STATE_RCPT:
                # New reason and set of recipients.
                # Save current data first
                _append_recipients(reason, addresses, recipients)

            reason = line.strip()[1:-1].replace('\n', ' ')
            state = MQ_STATE_RCPT
            addresses = []
            continue
        elif '@' in line:  # XXX: pretty dumb check
            if state != MQ_STATE_RCPT:
                _quit("Expected recipient address, got \"{}\"".format(line.strip()))
            addresses.append(line.strip())
            continue
        else:
            _quit("Unknown input line: {}".format(line))

    return msgs


def parse_ml():
    """
    Read and parse messages from /var/log/mail.log
    XXX: can be optimised as per parse_mq
    """
    lines = 0
    msgs = {}
    with open('/var/log/mail.log', 'rb') as f:
        for line in f.readlines():
            lines += 1
            if lines % 100000 == 0:
                # Technically off by one
                print('Processed %s lines (%s messages)...' % (lines, len(msgs)), file=sys.stderr)
            try:
                l = line.strip().split()
                if l[4][:13] == 'postfix/smtpd' and l[6][:7] == 'client=':
                    curmsg = l[5].rstrip(':')
                    if curmsg not in msgs:
                        msgs[curmsg] = {
                            'source_ip': l[6].rsplit('[')[-1].rstrip(']'),
                            'date': parse_syslog_date(' '.join(l[0:3])),
                        }
                elif False and l[4][:15] == 'postfix/cleanup' and l[6][:11] == 'message-id=': # dont want msgid right now
                    curmsg = l[5].rstrip(':')
                    if curmsg in msgs:
                        msgid = l[6].split('=', 1)[1]
                        if msgid[0] == '<' and msgid[-1] == '>':
                            # Not all message-ids are wrapped in < brackets >
                            msgid = msgid[1:-1]
                        msgs[curmsg]['message-id'] = msgid
                elif l[4][:12] == 'postfix/qmgr' and l[6][:5] == 'from=':
                    curmsg = l[5].rstrip(':')
                    if curmsg in msgs:
                        msgs[curmsg]['sender'] = l[6].split('<', 1)[1].rsplit('>')[0]
                elif l[4][:13] == 'postfix/smtp[' and any([i[:7] == 'status=' for i in l]):
                    curmsg = l[5].rstrip(':')
                    if curmsg in msgs:
                        status_field = [i for i in l if i[:7] == 'status='][0]
                        status = status_field.split('=')[1]
                        msgs[curmsg]['delivery-status'] = status
            except Exception:
                print('Warning: could not parse log line: %s' % repr(line), file=sys.stderr)
    print("Processed %s lines (%s messages)..." % (lines, len(msgs)), file=sys.stderr)
    return msgs


def parse_mailq_date(d, now):
    """
    Convert mailq plain text date string to unix epoch time
    """
    _, mon_str, day, time_str = d.split()
    hour, minute, second = time_str.split(':')
    d = datetime.datetime(year=now.year, month=MONTH_MAP[mon_str], day=int(day), hour=int(hour), minute=int(minute), second=int(second))
    # Catch messages generated "last year" (eg in Dec when you're running apq on Jan 1)
    if d > now:
        d = datetime.datetime(year=now.year-1, month=MONTH_MAP[mon_str], day=int(day), hour=int(hour), minute=int(minute), second=int(second))
    #return float(d.strftime('%s'))
    return float((d - UNIX_EPOCH).total_seconds())


def parse_syslog_date(d):
    """
    Parse a date in syslog's format (Sep 5 10:30:36) and return a UNIX time
    XXX: can be optimised as per parse_mailq_date
    """
    t = time.strptime(d + ' ' + time.strftime('%Y'), '%b %d %H:%M:%S %Y')
    if t > time.localtime():
        t = time.strptime(d + ' ' + str(int(time.strftime('%Y')-1)), '%b %d %H:%M:%S %Y')
    return time.mktime(t)


def filter_on_msg_key(msgs, pattern, key):
    """
    Filter msgs, returning only ones where 'key' exists and the value matches regex 'pattern'.
    """
    pat = re.compile(pattern, re.IGNORECASE)
    msgs = dict((msgid, data) for (msgid, data) in msgs.items() if key in data and re.search(pat, data[key]))
    return msgs


def filter_on_msg_reason(msgs, pattern):
    filtered = OrderedDict()
    pat = re.compile(pattern, re.IGNORECASE)
    for (queue_id, msg) in msgs.items():
        if msg["status"] == ST_ACTIVE:
            continue
        for recipient in msg["recipients"]:
            reason = recipient["reason"]
            if reason and re.search(pat, reason):
                filtered[queue_id] = msg

    return filtered


def filter_on_msg_recipient(msgs, pattern):
    filtered = OrderedDict()
    pat = re.compile(pattern, re.IGNORECASE)
    for (queue_id, msg) in msgs.items():
        for recipient in msg["recipients"]:
            addresses = recipient["addresses"]
            for address in addresses:
                if re.search(pat, address):
                    filtered[queue_id] = msg

    return filtered


def filter_on_msg_age(msgs, condition, age):
    """
    Filter msgs, returning only items where key 'date' meets 'condition' maxage/minage checking against 'age'.
    """
    assert condition in ['minage', 'maxage']
    # Determine age in seconds
    if age[-1] == 's':
        age_secs = int(age[:-1])
    elif age[-1] == 'm':
        age_secs = int(age[:-1]) * 60
    elif age[-1] == 'h':
        age_secs = int(age[:-1]) * 60 * 60
    elif age[-1] == 'd':
        age_secs = int(age[:-1]) * 60 * 60 * 24
    # Create lambda
    now = datetime.datetime.now()
    if condition == 'minage':
        f = lambda d: (now - datetime.datetime.fromtimestamp(d)).total_seconds() >= age_secs
    elif condition == 'maxage':
        f = lambda d: (now - datetime.datetime.fromtimestamp(d)).total_seconds() <= age_secs
    # Filter
    msgs = dict((msgid, data) for msgid, data in msgs.items() if f(data['date']))
    return msgs


def format_msgs_for_output(msgs):
    """
    Format msgs for output. Currently replaces time_struct dates with a string
    """
    for msgid in msgs:
        if 'date' in msgs[msgid]:
            msgs[msgid]['date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(msgs[msgid]['date']))
    return msgs


def parse_args():
    """
    Parse commandline arguments
    """
    parser = argparse.ArgumentParser(description='Parse postfix mail queue.')
    parser.add_argument('-j', '--json', action='store_true', help='JSON output (default)')
    parser.add_argument('-y', '--yaml', action='store_true', help='YAML output')
    parser.add_argument('-c', '--count', action='store_true', help='Return only the count of matching items')
    parser.add_argument('--log', action='store_true', help='Experimental: Search /var/log/mail.log as well.')
    parser.add_argument('--mailq-data', default=None, help='Use this file\'s contents instead of calling mailq')
    parser.add_argument('--reason', '-m', default=None, help='Select messages with a reason matching this regex')
    parser.add_argument('--recipient', '-r', default=None, help='Select messages with a recipient matching this regex')
    parser.add_argument('--sender', '-s', default=None, help='Select messages with a sender matching this regex')
    parser.add_argument('--parse-date', action='store_true', default=None, help='Parse dates into a more machine-readable format (slow) (implied by minage/maxage)')
    parser.add_argument('--maxage', '-n', default=None, help='Select messages younger than the given age. Format: age[{d,h,m,s}]. Defaults to seconds. eg: 3600, 1h')
    parser.add_argument('--minage', '-o', default=None, help='Select messages older than the given age. Format: age[{d,h,m,s}]. Defaults to seconds. eg: 3600, 1h')
    parser.add_argument('--exclude-active', '-x', action='store_true', help='Exclude items in the queue that are active')
    parser.add_argument('--only-active', action='store_true', help='Only include items in the queue that are active')
    parser.add_argument("--postfix3", action='store_true',
                        help="Output compatible with 'postqueue -j' (Postfix 3.1).")

    args = parser.parse_args()

    if args.minage and args.minage[-1].isdigit():
        args.minage += 's'
    elif args.minage and args.minage[-1] not in 'smhd':
        print >> sys.stderr, 'Error: --minage format is incorrect. Examples: 1800s, 30m'
        sys.exit(1)
    if args.maxage and args.maxage[-1].isdigit():
        args.maxage += 's'
    elif args.maxage and args.maxage[-1] not in 'smhd':
        print >> sys.stderr, 'Error: --maxage format is incorrect. Examples: 1800s, 30m'
        sys.exit(1)
    if args.exclude_active and args.only_active:
        print >> sys.stderr, 'Error: --exclude-active and --only-active are mutually exclusive'
        sys.exit(1)

    return args


def output_msgs(args, msgs):
    """
    Take msgs and format it as requested.
    """
    if args.count:
        print(len(msgs))
    else:
        msgs = format_msgs_for_output(msgs)
        if args.yaml:
            try:
                import yaml
            except ImportError:
                print("Error: Can't import yaml. Try installing PyYAML.", file=sys.stderr)
                sys.exit(1)
            print(yaml.dump(msgs))
        elif args.postfix3:
            for queue_id in msgs.keys():
                msg = msgs[queue_id]
                p3msg = convert_to_postfix31(queue_id, msg)
                print(json.dumps(p3msg))
        else:
            print(json.dumps(msgs, indent=2))


def parse_msg_dates(msgs, now):
    new_msgs = {}
    for msgid, data in msgs.items():
        if 'date' not in data:
            data['date'] = parse_mailq_date(data['rawdate'], now)
            new_msgs[msgid] = data
    return new_msgs


def convert_to_postfix31(queue_id, msg):
    """
    Converts the message dict to the equivalent generated by postqueue 3.1.
    :param queue_id:
    :param msg: Internal dict with the message info
    :return:
    """
    p3m = {
        "queue_name": msg.get("status"),
        "queue_id": queue_id,
        "arrival_time": int(parse_mailq_date(msg.get("rawdate"), datetime.datetime.now())),
        "message_size": msg.get("size"),
        "sender": msg.get("sender"),
        "recipients": []
    }
    for recipient in msg["recipients"]:
        reason = recipient["reason"]
        for address in recipient["addresses"]:
            r = {
                "delay_reason": reason,
                "address": address
            }
            p3m["recipients"].append(r)

    return p3m


def main():
    """
    Main function
    """
    args = parse_args()

    # Load messages
    msgs = {}
    if args.log:
        msgs.update(parse_ml())
    msgs.update(parse_mq(args))

    # Prepare data
    if args.parse_date or args.minage or args.maxage:
        now = datetime.datetime.now()
        msgs = parse_msg_dates(msgs, now)

    # Filter messages
    if args.reason:
        msgs = filter_on_msg_reason(msgs, args.reason)
    if args.sender:
        msgs = filter_on_msg_key(msgs, args.sender, 'sender')
    if args.recipient:
        msgs = filter_on_msg_recipient(msgs, args.recipient)
    if args.minage:
        msgs = filter_on_msg_age(msgs, 'minage', args.minage)
    if args.maxage:
        msgs = filter_on_msg_age(msgs, 'maxage', args.maxage)
    if args.exclude_active:
        msgs = dict((msgid, data) for (msgid, data) in msgs.items() if data.get('status') != 'active')
    elif args.only_active:
        msgs = dict((msgid, data) for (msgid, data) in msgs.items() if data.get('status') == 'active')

    output_msgs(args, msgs)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("")
        sys.exit(1)
