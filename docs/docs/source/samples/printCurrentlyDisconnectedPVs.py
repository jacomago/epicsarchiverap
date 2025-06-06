#!/usr/bin/env python3
'''This script gets a list of PVS that are currently disconnected and then prints them out by appliance'''

import os
import sys
import argparse
import time
import requests
import json
import datetime
import time

def getCurrentlyDisconnectedPVs(bplURL):
    '''Get a list of PVs that are currently disconnected'''
    url = bplURL + '/getCurrentlyDisconnectedPVs'
    resp = requests.get(url)
    resp.raise_for_status()
    currentlyDisconnectedPVs = resp.json()
    return currentlyDisconnectedPVs

def printByAppliance(currentlyDisconnectedPVs):
    '''Prints the disconnected PVs sorted by appliance and PV name'''
    # We get a dict of dicts going - first level is appliance and second level is pvName
    instance2pv2info = {}
    for currentlyDisconnectedPV in currentlyDisconnectedPVs:
        appliance = currentlyDisconnectedPV['instance']
        if appliance not in instance2pv2info:
            instance2pv2info[appliance] = {}
        instance2pv2info[appliance][currentlyDisconnectedPV['pvName']] = currentlyDisconnectedPV
    
    for appliance in sorted(instance2pv2info):
        print("Appliance {0}:".format(appliance))
        pvsInAppliance = instance2pv2info[appliance]
        for pv in sorted(pvsInAppliance):    
            print("{0} {1}".format(pv, pvsInAppliance[pv]['connectionLostAt']))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("--onlyNA", action='store_true', help="Print only those PVs for whom the connection lost is N/A.")
    parser.add_argument("--noNA", action='store_true', help="Print only those PVs for whom the connection lost is not N/A.")
    args = parser.parse_args()
    if not args.url.endswith('bpl'):
        print("The URL needs to point to the mgmt bpl; for example, http://arch.slac.stanford.edu/mgmt/bpl. ", args.url)
        sys.exit(1)
    currentlyDisconnectedPVs = getCurrentlyDisconnectedPVs(args.url)
    if args.onlyNA:
        printByAppliance([x for x in currentlyDisconnectedPVs if x['connectionLostAt'] == 'N/A'])
    elif args.noNA:
        printByAppliance([x for x in currentlyDisconnectedPVs if x['connectionLostAt'] != 'N/A'])
    else:
        printByAppliance(currentlyDisconnectedPVs)
    
    
