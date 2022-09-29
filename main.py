import pandas as pd
import tldextract
import socket
from concurrent import futures
_default_max_workders = 600
fileinput = 'iplist_exchange.txt'
fileoutput = 'output_exchange.csv'


def output_to_file(file_output):
    # Output to CSV
    df_resolve.to_csv(file_output, index=False)


def _single_request(ip):
    try:
        hostname, aliaslist, ipaddrlist = socket.gethostbyaddr(ip)
        ext = tldextract.extract(hostname)
        return [ip, hostname, ext.registered_domain]
    except Exception as e:
        return [ip, str(e)]


def reverse_dns_lookup(ip_list, max_workers=_default_max_workders):
    """Return the hostname, aliaslist, and ipaddrlist for a list of IP
    addresses.
    This is mainly useful for a long list of typically duplicated IP adresses
    and helps in getting the information very fast. It is basically the
    equivalent of running the `host` command on the command line many times:
    .. code-block:: bash
        $ host 66.249.80.0
        0.80.249.66.in-addr.arpa domain name pointer google-proxy-66-249-80-0.google.com.
    You also get a simple report about the counts of the IPs to get an overview
    of the top ones:
    # >>> import advertools as adv
    # >>> ip_list = ['66.249.66.194', '66.249.66.194', '66.249.66.194',
    # ...            '66.249.66.91', '66.249.66.91', '130.185.74.243',
    # ...            '31.56.96.51', '5.211.97.39']
    # >>> adv.reverse_dns_lookup([ip_list])
    ====  ============== ================================= ======================
      ..  ip_address     hostname                          errors
    ====  ============== ================================= ======================
       0  66.249.66.194  crawl-66-249-66-194.googlebot.com
       1  66.249.66.91   crawl-66-249-66-91.googlebot.com
       2  130.185.74.243 mail.garda.ir
       3  31.56.96.51    31-56-96-51.shatel.ir
       4  5.211.97.39                                      [Errno 1] Unknown host
    ====  ============== ================================= ======================
    :param list ip_list: a list of IP addresses.
    :param int max_workers: The maximum number of workers to use for multi
                            processing.
    """
    hosts = []
    socket.setdefaulttimeout(8)
    count_df = pd.DataFrame(ip_list, columns=['ip_address'])

    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for host in executor.map(_single_request, count_df['ip_address']):
            hosts.append(host)
    final_df = pd.DataFrame(hosts)
    columns = ['ip', 'hostname', 'First_level']
    final_df.columns = columns
    return final_df


# reads ip list
df = pd.read_csv(fileinput)

# list comprehension
iplist = [x for x in df['ip']]

# bulk reverse lookup
df_resolve = reverse_dns_lookup(iplist)

# uncomment onderstaande om te outputten naar file
output_to_file(fileoutput)
