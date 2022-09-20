import pandas as pd
import socket
import tldextract

def revese_dns(data):
    #reverse lookup of all ip's.
    try:
        reverse_domain_name = socket.gethostbyaddr(data)[0]
    except(socket.gaierror):
        pass
    except(socket.herror):
        reverse_domain_name = "NO PRT"
    return reverse_domain_name

reverse_dns = []
result_firstlevel = []

#reads ip list
df = pd.read_csv('iplist.txt')

#list comprehension
iplist = [x for x in df['ip']]

for ip in iplist:
    reverse_dns.append(revese_dns(ip))
for name in reverse_dns:
    ext = tldextract.extract(name)
    result_firstlevel.append(ext.registered_domain)
df['fistlevel'] = result_firstlevel
df['reverse'] = reverse_dns

df.to_csv('output.txt', index=False)