import csv
import os
import datetime
import collections
from operator import itemgetter


class auto_incl():
    ''' auto-increment ID '''
    def __init__(self, start, step):
        self.start = start
        self.step = step
        self.curr = self.start

    def count(self):
        while True:
            yield self.curr
            self.curr += self.step

    def reset(self):
        self.curr = self.start

# directory
# __file__ = './src/sessionization.py'
script_dir = os.path.dirname(__file__)
input_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'input')
log_dir = os.path.join(input_dir, "log.csv")
inactivity_dir = os.path.join(input_dir, "inactivity_period.txt")
output_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'output', 'sessionization.txt')
print "dirs: ", script_dir, input_dir, log_dir, inactivity_dir, output_dir

# inactivity window (in seconds)
with open(inactivity_dir, 'r') as read_file:
    tdelta = int(float(read_file.read()))
    print "inactivity_window: ", tdelta

with open(log_dir, 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    counter = auto_incl(0, 1).count()
    prev_dt = None
    # <key=str(ip), value=dict(first_dt, last_dt, id, count)>
    ip_map = collections.OrderedDict()
    # <key=dt(last_dt), value=list(ip)>
    last_dt_map = collections.OrderedDict()

    with open(output_dir, 'w') as new_file:
        fieldnames = ['ip', 'first_dt', 'last_dt', 'duration', 'count']
        csv_writer = csv.DictWriter(new_file, fieldnames=fieldnames, delimiter=",")
        for line in csv_reader:
            curr_dt = datetime.datetime.strptime(line['date'] + " " + line['time'], "%Y-%m-%d %H:%M:%S")

            ### init ip_map & last_dt_map
            if prev_dt == None:
                ip_map[line['ip']] = {'ip': line['ip'], 'first_dt': curr_dt, 'id': 0, 'last_dt': curr_dt, 'count': 1}
                last_dt_map[curr_dt] = [line['ip']]
                prev_dt = curr_dt
                continue

            ### process current datetime
            if prev_dt == curr_dt:
                if line["ip"] not in ip_map:
                    ip_map[line['ip']] = {'ip': line['ip'], 'first_dt': curr_dt, 'id': next(counter), 'last_dt': curr_dt, 'count': 1}
                    if curr_dt not in last_dt_map:
                        last_dt_map[curr_dt] = [line['ip']]
                    else:
                        last_dt_map[curr_dt].append(line['ip'])
                else:
                    last_dt = ip_map[line['ip']]['last_dt']
                    ip_map[line['ip']]['last_dt'] = curr_dt
                    ip_map[line['ip']]['count'] += 1
                    if last_dt != curr_dt:
                        # delete old
                        last_dt_map[last_dt].remove(line['ip'])
                        # add new
                        if curr_dt not in last_dt_map:
                            last_dt_map[curr_dt] = [line['ip']]
                        else:
                            last_dt_map[curr_dt].append(line['ip'])

            ### process new datetime
            else:
                ### clean dictionary accordingly & output inactive sessions
                inactive_ips = []
                inactive_ip_maps = []
                # clean last_dt_map
                for last_dt in last_dt_map:
                    if (curr_dt - last_dt).seconds > tdelta:
                        inactive_ips.extend(last_dt_map.pop(last_dt, None))
                # clean ip_map
                for inactive_ip in inactive_ips:
                    inactive_ip_maps.append(ip_map.pop(inactive_ip, None))
                # sort output by (first_dt, id)
                ready_to_output = sorted(inactive_ip_maps, key=itemgetter('first_dt', 'id'))
                # output write
                for record in ready_to_output:
                    record['duration'] = record['last_dt'].second - record['first_dt'].second + 1
                    del record['id']
                    csv_writer.writerow(record)
                # update prev_dt
                prev_dt = curr_dt
                # reset counter
                counter = auto_incl(0, 1).count()

                ### process the first record of every new datetime
                if line["ip"] not in ip_map:
                    ip_map[line['ip']] = {'ip': line['ip'], 'first_dt': curr_dt, 'id': next(counter), 'last_dt': curr_dt, 'count': 1}
                    last_dt_map[curr_dt] = [line['ip']]
                else:
                    last_dt = ip_map[line['ip']]['last_dt']
                    ip_map[line['ip']]['last_dt'] = curr_dt
                    ip_map[line['ip']]['count'] += 1
                    if last_dt != curr_dt:
                        last_dt_map[last_dt].remove(line['ip'])
                        last_dt_map[curr_dt] = [line['ip']]

        ### output remaining records in the dictionary when reaching the end of reader
        for value in ip_map.values():
            value['duration'] = value['last_dt'].second - value['first_dt'].second + 1
            del value['id']
            csv_writer.writerow(value)
