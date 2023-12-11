import sys
from html.parser import HTMLParser
import requests
import time
import datetime
import os
import threading

REFS_PER_REQUEST = 100


def print_progress_bars(fn, prefixes, suffix='Complete', decimals=1, length=100, fill='█'):
    def print_bar(i, iterable, prefix):
        it_len = len(iterable)
        percent = ("{0:." + str(decimals) + "f}").format(100 *
                                                         (i / float(it_len)))
        filled = int(length * i // it_len)
        bar = fill * filled + '-' * (length - filled)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}')

    # done  - are we done=
    # prog  - progress for each iterable
    done, iterables, prog = fn()
    k = len(iterables) - 1

    def print_all_bars(iterables, prog, k):
        print('\033[%dA' % len(iterables), end='')
        for i in range(0, k):
            print_bar(prog[i], iterables[i], prefix=prefixes[i])
        print_bar(prog[k], iterables[k], prefix=prefixes[k])

    print('\n' * k)

    while not done:
        print_all_bars(iterables, prog, k)
        done, iterables, prog = fn()

    print_all_bars(iterables, prog, k)


class ReferenceHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.count = 0  # References found in last html
        self.found_body = False
        self.refs = []
        self.tr_td_count = 0

    def handle_starttag(self, tag, attrs):
        if tag == "tbody":
            self.found_body = True
        if tag == "tr" and self.found_body:
            self.tr_td_count = 0
        if tag == "td" and self.found_body:
            self.tr_td_count += 1

    def handle_data(self, data):
        if self.tr_td_count == 4:
            ref = data.replace("\\n", "")
            ref = ref.strip()
            if ref:
                self.refs.append(ref)
                self.count += 1


def load_refs():
    refs = []
    try:
        with open('data/references.txt', 'r') as f:
            for line in f:
                if line.strip():
                    refs.append(line.strip())
    except:
        pass
    return refs


def update_ref():
    print("Updating reference list")
    parser = ReferenceHTMLParser()
    # This is a hacky method
    expected = list(range(0, len(load_refs())))
    # Emulate do-while(parser.count == 100)
    i = 1
    parser.count = REFS_PER_REQUEST
    error = ''
    # This is called by print_progress_bars

    def do_stuff():
        nonlocal i
        nonlocal error
        nonlocal parser
        parser.count = 0
        URL = 'https://webgate.ec.europa.eu/rasff-window/portal/?event=notificationsList&StartRow=%d' % i
        result = requests.get(URL)
        if result.status_code != 200:
            error = "Error: status %s: i = %d" % (result, i)
            return True, [expected], [i]
        i += parser.count
        parser.feed(str(result.content))
        return parser.count < REFS_PER_REQUEST, [expected], [i]
    stime = time.time()

    print_progress_bars(do_stuff, prefixes=['Progress'])

    if error:
        print(error)

    print("Time: %s" % (time.time() - stime))
    print("Found %d references" % len(parser.refs))

    out = 'data/raw/references.txt'
    os.makedirs('data', exist_ok=True)
    # Save references to unique file
    s = '\n'.join(parser.refs)
    with open(out, 'w') as f:
        f.write(s)


class XmlThread(threading.Thread):
    def __init__(self, thread_num, refs, out):
        super().__init__()
        self.thread_num = thread_num
        self.refs = refs
        self.data = []
        self.errors = []
        self.out = out
        self.count = 0
        self.done = False

    def get_name(self):
        return 'Thread %d' % self.thread_num

    def run(self):
        # t = (int) (len(self.refs) / 5)
        self.count = 0

        for r in self.refs:
            try:
                URL = 'https://webgate.ec.europa.eu/rasff-window/portal/?event=DetailsToXML&NOTIF_REFERENCE=%s' % r
                result = requests.get(URL)

                self.data.append(result.text)

                # if(self.i % t == 0):
                #     print("Thread %d is %.2f procent done" % (self.thread_num, (i / len(self.refs)) * 100))
                self.count += 1
            except:
                self.errors.append(r)

        # print("Thread %d is 100.00%% procent done" % self.thread_num)

        os.makedirs(self.out, exist_ok=True)
        with open(self.out + "/data_thread_%d.xml" % self.thread_num, 'w') as f:
            s = '\n'.join(self.data)
            f.write(s)

        if len(self.errors) != 0:
            with open(self.out + "/error_thread_%d.txt" % self.thread_num, 'w') as f:
                s = '\n'.join(self.errors)
                f.write(s)

        self.done = True


def update_xml(thread_count=8):
    print("Updating XML data")
    out = 'data/raw/'
    refs = load_refs()
    t = (int)(len(refs) / thread_count)
    # Create thread_count threads. The last thread handles the remaining of references
    thread_list = []
    for i in range(0, thread_count - 1):
        thread_list.append(XmlThread(i + 1, refs[i * t: (i + 1) * t], out))
    thread_list.append(
        XmlThread(thread_count, refs[(thread_count - 1) * t: len(refs)], out))
    # Prepare progress bar
    iterables = []
    progs = []
    prefixes = []
    for thread in thread_list:
        iterables.append(thread.refs)
        progs.append(thread.count)
        prefixes.append(thread.get_name())

    k = len(thread_list)

    def do_stuff():
        nonlocal k
        nonlocal iterables
        nonlocal progs
        nonlocal thread_list

        done = True
        # Do not change this for-loop
        for i in range(0, k):
            progs[i] = thread_list[i].count
            done = done and thread_list[i].done

        time.sleep(0.1)
        return done, iterables, progs

    class ProgressThread(threading.Thread):
        # def __init__(self, cb, prefixes):
        # self.prefixes = prefixes
        # pass
        def run(self):
            print_progress_bars(do_stuff, prefixes)

    thread_list.append(ProgressThread())
    stime = time.time()
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()
    print("Time: %s" % (time.time() - stime))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: update <options>\n  Options are one or more of following:\n   -ref    Update reference list\n   -xml    Update raw xml data")
        exit(0)

    cmd = {
        "-ref": update_ref,
        "-xml": update_xml
    }

    cmd.get(sys.argv[1], lambda: print("Unknown option", sys.argv[1]))()
