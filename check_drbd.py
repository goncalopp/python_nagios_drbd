# check_drbd.py - Check DRBD statistics. Thresholds can be specified from the commandline

STATUS_FILE= '/proc/drbd'

summable_keys= set(['ns','nr','dw','dr','al','bm','lo','pe','ua','ap'])
enumerable_keys=    set(['cs'])
lr_enumerable_keys= set(['ro', 'ds'])    #"Local/Remote" format keys, aka lr_keys

attribute_descriptions= {
    "cs":  "connection state",
    "ro":  "roles",
    "ds":  "disk states",
    "ns":  "network send",
    "nr":  "network receive",
    "dw":  "disk write",
    "dr":  "disk read",
    "al":  "activity log",
    "bm":  "bit map",
    "lo":  "local count",
    "pe":  "pending",
    "ua":  "unacknowledged",
    "ap":  "application pending",
    "ep":  "epochs",
    "wo":  "write order",
    "oos": "out of sync",
    #--- only parser introduced attributes below
    "rp":  "replication protocol",
    "iof": "I/O flags"
    }

#-----------------------------------------------------------------------

def parse_proc_drbd():

    def parse_kv_item( s ):
        '''example:     cs:Connected'''
        try:
            k,v= s.split(":")
            try:
                v= int(v)
            except ValueError:
                pass
            return k,v
        except ValueError:
            raise Exception("Failed to parse token    "+s)

    def parse_next_resource(it):
        l1= it.next().split()                       #Get lists of tokens
        l2= it.next().split()                       #for each line.
        assert len(l1)==6
        assert len(l2)==13
        kv_items= l1[1:-2] + l2                       #Get "k:v" items
        items= dict(map(parse_kv_item, kv_items ))  #into a dictionary.

        items['rp']= l1[-2]                         #Get non "k:v" items
        items['iof']= l1[-1]                        #with fake keys there too.
        assert l1[0][-1]==':'
        resource= l1[0][:-1]
        return resource, items

    it = open(STATUS_FILE).read().splitlines().__iter__()
    assert it.next().startswith("version")
    it.next() #line 2 is source version
    resources={}
    while True:
        try:
            res_n, data= parse_next_resource( it )
            resources[res_n]= data
        except StopIteration:
            return resources

def local_part(s):
    l,r=s.split("/")
    return l

def remote_part(s):
    l,r=s.split("/")
    return r

def calc_stats( resource_data ):
    '''calculates interesting stats with data returned by parse_proc_drbd'''
    from collections import Counter
    n_resources= len(resource_data)
    by_attribute= dictionary_group_by( resource_data.values() )
    totals= {k:sum(v) for k,v in by_attribute.items() if k in summable_keys}

    def count_remote_and_local(values):
        '''counts lr_keys'''
        local= Counter(map(local_part, values))
        remote= Counter(map(remote_part, values))
        both= local+remote
        return {"local":local, "remote":remote, "both":both}

    lr_enumerable_counts= { a: count_remote_and_local(by_attribute[a]) for a in lr_enumerable_keys }
    enumerable_counts= { a: Counter(by_attribute[a]) for a in enumerable_keys }
    all_counts= dict( lr_enumerable_counts.items() + enumerable_counts.items() )

    def frequency( d, percentage=True ):
        '''Given a dictionary with counts on values, returns a new dictionary with
        the relative frequency of that count on the total'''
        total= sum(d.values())
        fac= 100.0 if percentage else 1.0
        return defaultdict( lambda:0, [(k, v*fac/total) for k,v in d.items()] )
    def frequency_keys(d):
        return map_dict(frequency, d)
    lr_enumerable_percentages= map_dict( frequency_keys, lr_enumerable_counts )
    enumerable_percentages= map_dict( frequency, enumerable_counts )

    all_percentages= dict( lr_enumerable_percentages.items() + enumerable_percentages.items() )

    all_stats= {'totals': totals, 'counts': all_counts, 'percentages': all_percentages }
    return all_stats


#-----------------------------------------------------------------------
from collections import defaultdict
from itertools import chain

def dictionary_group_by( list_of_dicts ):
    '''groups by key'''
    result= defaultdict(list)
    for d in list_of_dicts:
        for k,v in d.iteritems():
            result[k].append(v)
    return dict(result)

def map_dict( f, d ):
    '''Similar to map(), but for dictionaries'''
    return {k: f(v) for k,v in d.iteritems()}

#-----------------------------------------------------------------------

from pynag.Plugins import PluginHelper,ok,warning,critical,unknown

helper = PluginHelper()
#helper.parser.add_option()
helper.parse_arguments() #handles thresholds

try:
    data= parse_proc_drbd()
    stats= calc_stats(data)

    ds_both= stats['percentages']['ds']['both']
    metrics= {
    'percentage_up_to_date': ds_both['UpToDate'],
    'percentage_up_to_date_or_ahead': ds_both['UpToDate']+ds_both['Ahead']+ds_both['Behind'],
    'percentage_connected': stats['percentages']['cs']['Connected'],
    'percentage_local_primary': stats['percentages']['ro']['local']['Primary'],
    'abnormal_io_flags': sum(resource['iof']!="r-----" for resource in data.values()),
    }
    locals().update(metrics) #make available metrics as variables
    import pprint
    helper.add_long_output( pprint.pformat( data ))
except Exception as e:
    import traceback
    trace= traceback.format_exc(e)
    helper.exit(summary="A exception occurred", long_output=trace, exit_code=unknown, perfdata='')

for k,v in metrics.items():
    helper.add_metric(label=k, value=v)

helper.status(ok)           #default
helper.check_all_metrics()  #check thresholds

acceptable_ranges= {
    'percentage_up_to_date': (90,100),
    'percentage_up_to_date_or_ahead': (100,100),
    'percentage_connected': (100,100),
    'percentage_local_primary': (100,100),
    'abnormal_io_flags': (0,0),
    }
for metric,rang in acceptable_ranges.items():
    low,high= rang
    if not low <= metrics[metric] <= high:
        helper.status(critical)
        helper.add_summary( "{} out of accepted range {}".format(metric, rang))

helper.exit()               # Print out plugin information and exit nagios-style
