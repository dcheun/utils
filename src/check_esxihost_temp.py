#!/usr/bin/env python

"""Temperature Plugin for Nagios.

Check temperature sensor readings from ESXi hosts via vim-cmd hostsummary reading.

Check inlet, outlet, and CPU temps.

"""

import getopt
from subprocess import Popen, PIPE
import sys
from textwrap import dedent
import traceback

__author__ = "Danny Cheun"
__credits__ = ["Danny Cheun"]
__version__ = "1.0.1"
__maintainer__ = "Danny Cheun"
__email__ = "dcheun@gmail.com"


script_args = {}

def run_cmd(cmd, input_str=None, exit_on_fail=True,
            print_all=False, get_returncode=False):
    """Execute a command in a subprocess.
    
    @param cmd: The command to execute, type is usually a list.
    @keyword intput_str: A string to be passed in as stdin to the
            program to execute (default=None).
    @keyword exit_on_fail: If True, program will exit with error if the
            subprocess returns non-zero (default=True).
    @keyword print_all: If True, will print output, error to stdout,
            stderr respectively (default=False).
    @keyword get_return_code: If True, the return code from executing
            the command is included in the returned tuple.
    @return: The output, error tuple of the command.
    
    """
    try:
        p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        (output, error) = p.communicate(input=input_str)
    except Exception:
        raise
    
    if error and print_all:
        print >> sys.stderr, error
    
    if exit_on_fail and p.returncode != 0:
        print >> sys.stderr, ' ERROR: retcode=' + str(p.returncode) + ':', \
                'output=' + output, 'error=' + error
        sys.exit(1)
    
    if get_returncode:
        return (output, error, p.returncode)
    else:
        return (output, error)


def try_run_cmd(cmd, input_str=None, exit_on_fail=True,
                print_all=False, no_tb=False):
    """run_cmd() wrapper.
    
    @keyword no_tb: Suppresses traceback info if an exception is caught.
            Instead, prints a one liner error msg (default=False).
    @see: run_cmd()
    
    """
    try:
        (output, error) = run_cmd(cmd, input_str, exit_on_fail, print_all)
    except Exception:
        if no_tb:
            print >>sys.stderr, traceback.format_exc()
            sys.exit(1)
        else:
            raise
    
    return (output, error)


def get_host_summary(host):
    """Get the host summary through ssh.
    Assumes use of ssh keys, keys should already be added to host.
    
    @param host: An IP address string.
    
    """
    login = 'root@'+host
    cmd = ['ssh', '-n', login, 'vim-cmd hostsvc/hostsummary']
    (output,error) = try_run_cmd(cmd)
    return (output, error)


def get_temp_data(summary):
    """Parse out the temperature info from the given
    hostsummary string into a dictionary.
    
    @param summary: String output from hostsvc/hostsummary
    
    """
    temps = []
    temp = []
    summ_list = summary.split('\n')
    for line in summ_list:
        if 'Temp' in line:
            temp = [line.strip()]
        elif 'temperature' in line:
            temp.append(line.strip())
            temps.append(temp)
            temp = []
        elif temp != [] and '=' in line:
            temp.append(line.strip())
    unwanted = ['"', '{', '}', ',', '(', ')', '<', '>']
    temperatures = {}
    for temp in temps:
        d= {}
        for t in temp:
            kv = ''.join([x for x in t if x not in unwanted])
            k, v = kv.split(' = ')
            d[k] = v
        temperatures[d['name'].split(' --- ')[0]] = d
    return temperatures


def check_temps(host, temp_sensor):
    """Get temperature dictionary and check if it is
    in the acceptable range.
    
    @param host: An IP address string of the ESXi Host.
    @param temp_sensor: The temperature sensor to check.
            Should be one of the following: 'processor','inlet','outlet'
    
    """
    (output, error) = get_host_summary(host)
    
    if error:
        print >>sys.stderr, 'ERROR: %s' % error
        
    temps = get_temp_data(output)
    
    ########################################################################
    # Check the temperatures.
    ############ PROCESSOR CHECK ###############
    if temp_sensor == 'processor':
        # Extract temperatures
        try:
            proc_one = int(temps['Processor 1 Temp']['currentReading'])
            proc_two = int(temps['Processor 2 Temp']['currentReading'])
            temp_readings = ('Temperatures: Processor 1 %sC, Processor 2 %sC' %
                             (proc_one/100, proc_two/100))
        except Exception:
            print ('CRITICAL - Unable to get sensor readings: %s\n\n%s' %
                   (temps,traceback.format_exc()))
            sys.exit(2)
        # Check if the temperatures are within range.
        if (proc_one <= 500 or proc_one >= 7500 or
            proc_two <= 500 or proc_two >= 7500):
            print ('CRITICAL - Processor(s) crossed threshold: %s' %
                   temp_readings)
            sys.exit(2)
        elif (proc_one <= 1000 or proc_one >= 7200 or
              proc_two <= 1000 or proc_two >= 7200):
            print ('WARNING - Processor(s) approaching threshold: %s' %
                   temp_readings)
            sys.exit(1)
        elif (1000 < proc_one < 7200 or
              1000 < proc_two < 7200):
            print ('OK - Processor(s) within acceptable range: %s' %
                   temp_readings)
            sys.exit(0)
        else:
            print 'UNKNOWN - %s' % temp_readings
            sys.exit(3)
    ############ INLET (AMBIENT) CHECK ###############
    elif temp_sensor == 'inlet':
        # Extract temperatures
        try:
            inlet = int(temps['System Board 1 Inlet Temp']['currentReading'])
            temp_readings = ('Temperature: Inlet %sC' % (inlet/100))
        except Exception:
            print ('CRITICAL - Unable to get sensor readings: %s\n\n%s' %
                   (temps,traceback.format_exc()))
            sys.exit(2)
        # Check if the temperature is within range.
        if (inlet <= 1200 or inlet >= 3400):
            print ('CRITICAL - Inlet (ambient) crossed threshold: %s' %
                   temp_readings)
            sys.exit(2)
        elif (inlet <= 1600 or inlet >= 2700):
            print ('WARNING - Inlet (ambient) approaching threshold: %s' %
                   temp_readings)
            sys.exit(1)
        elif (1600 < inlet < 2700):
            print ('OK - Inlet (ambient) within acceptable range: %s' %
                   temp_readings)
            sys.exit(0)
        else:
            print 'UNKNOWN - %s' % temp_readings
            sys.exit(3)
    ############ OUTLET CHECK ###############
    elif temp_sensor == 'outlet':
        # Extract temperatures
        try:
            inlet = int(temps['System Board 1 Inlet Temp']['currentReading'])
            exhaust = int(temps['System Board 1 Exhaust Temp']['currentReading'])
            temp_readings = ('Temperatures: Inlet %sC, Exhaust %sC' %
                         (inlet/100, exhaust/100))
        except Exception:
            print ('CRITICAL - Unable to get sensor readings: %s\n\n%s' %
                   (temps,traceback.format_exc()))
            sys.exit(2)
        # Check if the temperature is within range.
        if (exhaust-inlet) >= 2500:
            print ('CRITICAL - Inlet to exhaust rise in temp crossed '
                   'threshold: %s' % temp_readings)
            sys.exit(2)
        elif (exhaust-inlet) >= 2000:
            print ('WARNING - Inlet to exhaust rise in temp approaching '
                   'threshold: %s' % temp_readings)
            sys.exit(1)
        elif (exhaust-inlet) < 2000:
            print ('OK - Inlet to exhaust rise in temp within acceptable '
                   'range: %s' % temp_readings)
            sys.exit(0)
        else:
            print 'UNKNOWN - %s' % temp_readings
            sys.exit(3)
    ############ UNKNOWN ###############
    else:
        print 'UNKNOWN - Invalid sensor: \'%s\'' % temp_sensor
        sys.exit(3)


###############################################################################
# Main.
###############################################################################
def usage():
    """Display usage information"""
    print dedent("""
    Required argument(s):
      -a <ADDRESS>, --address=<ADDRESS>
          An IP address string.
      -t <TEMP_SENSOR>, --temp-sensor=<TEMP_SENSOR>
          The temperature sensor. Specify one of the following for TEMP_SENSOR:
          'processor' - Check the processor(s) temperature.
          'inlet' - Checks the inlet (ambient) temperature.
          'outlet' - Checks the rise in temperature from inlet to outlet.
    
    Optional argument(s):
      -h, --help
          Displays this help screen.
    """)


def handle_args():
    """Take in arguments and set to dictionary"""
    global script_args
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'a:t:h',
                                   ['address=', 'temp-sensor=','help'])
    except getopt.GetoptError as e:
        print str(e)
        usage()
        sys.exit(2)
    
    for o, a in opts:
        if o == '-a' or o == '--address':
            script_args['address'] = a
        elif o == '-t' or o == '--temp-sensor':
            script_args['temp-sensor'] = a
        elif o == '-h' or o == '--help':
            script_args['help'] = True
        else:
            assert False, 'Unhandled option %s' % o
    
    if 'help' in script_args:
        usage()
        sys.exit(0)
    # Check for required script args.
    if 'address' not in script_args or 'temp-sensor' not in script_args:
        print >>sys.stderr, 'ERROR: Missing required argument(s).'
        usage()
        sys.exit(2)


def main():
    global scrip_args
    handle_args()
    check_temps(host=script_args['address'],
                temp_sensor=script_args['temp-sensor'])


if __name__ == '__main__':
    main()
