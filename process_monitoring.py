from subprocess import Popen, PIPE
output = Popen( "ps -e -o 'pid,etimes,command' | awk '{if($2>7200) print $0}'", stdout=PIPE, universal_newlines = True)
pdi_processes = []
for lines in output.stdout:
    print(lines)
    if lines.find('JA_full_load.kjb') =! -1:
        print('We have process which work more than 2 Hours')
        splitted_text = lines.split(' ', 2)
        pdi_processes.append([splitted_text[0], splitted_text[1]])
print('List of processes which need to be killed are:')
for item in pdi_processes:
    print(f'PID: {item[0]}')