# Data Sources:  Classes for each commonly data source used during Tracking Program 
## The below shows an example of two of many data sources used:  SQL & Splunk
    # Removed:  Local Files and FTP
## All sensitive information has been stripped/renamed.

import time
import sys
import pypyodbc
import pandas as pd
import splunklib.client as client
import splunklib.results as results
from datetime import datetime as dt

class Splunk:
    # Splunk server connection info is saved as a dictionary and varies by server.

    def __init__( self , server ):
        for key in server:
            setattr( self , key , server[ key ] )
        self.service = client.Service( host=self.server , scheme='https' )
        self.service.login()

    def allSearches( self ):
        self.searches = self.service.saved_searches

    def savedSearch( self , name ):
        self.search = self.service.saved_searches[ name ]

    def newSearch( self , string ):
        query_head = 'search ' + \
        'index="' + self.index + '" ' + \
        'sourcetype="' + self.sourcetype + '" ' + \
        'host="' + self.host + '" '
        query = query_head + string
        self.search = query

    def run( self , type ):
        
        if type == 'saved':
            job = self.search.dispatch()
        
        elif type == 'one-shot':
            job = self.service.jobs.create( self.search , **self.kwargs )

        time.sleep(2)

        while True:
            while not job.is_ready():
                pass
            try:
                stats = {'isDone': job[ 'isDone' ]
                        ,'doneProgress' : float( job[ 'doneProgress' ] ) * 100
                        ,'scanCount': int( job[ 'scanCount' ] )
                        ,'eventCount' : int( job[ 'eventCount' ] )
                        ,'resultCount' : int( job[ 'resultCount' ] )
                        }
                status = ( '\r%(doneProgress)03.1f%%    %(scanCount)d scanned   '
                        '%(eventCount)d matched   %(resultCount)d results' ) % stats
            except:
                print('Error.')

            sys.stdout.write(status)
            sys.stdout.flush()
            if stats['isDone']=='1':
                break
            time.sleep(2)
        
        res = results.ResultsReader( job.results( count = 0 ) )
        data = []
        for row in res:
            if isinstance( row , OrderedDict ):
                data.append( dict( row ) )
        job.cancel()

        self.results = pd.DataFrame( data )

class SQL:
    """ Allows connection to multiple SQL Servers to run generic queries
        or commonly used queries (saved as methods) """

    def __init__( self , server_addy , db_name ):

        self.driver = 'ODBC Driver 13 for SQL Server'
        self.server = server_addy
        self.db = db_name       

    def connect( self ):
        self.conn = pypyodbc.connect( 'Driver={' + self.driver + '};'
            'Server=' + self.server + ';'
            'Database=' + self.db + ';'
            'Trusted_Connection=yes;')
    
    def get_df( self ):
        self.Connect()
        df = pd.read_sql( self.query , self.conn )
        return df

    def build_query( self, select, join, where ):
        self.select = 'Select ' + select
        self.join = ' From ' + join
        self.where = ' Where ' + where
        self.query = self.select + self.join + self.where

    def tracking_summary( self , startDate , endDate , partner_id=None , tracking_prefixes=None , mail_classes=None ):

        self.select = """Select main.customer_id, 
                p.partner_name, 
                main.destination_zipcode, 
                main.shipping_label_create_date,
                main.shipping_label_tracking_number,
                mc.mail_class_name, 
                main.origin_zipcode """
        self.join = """ from main_shipments_table main
                left join mail_class_table mc
                on main.mc_id = mc.id
                left join partners p
                on main.p_id = p.id"""
        self.where = f""" where main.shipping_label_create_date between '{str(startDate)}' AND '{str(endDate)}'"""
        if partner_id: # Int
            self.where += f""" AND p.partner_id = {partner_id} """
        if tracking_prefixes: # List
            if len(tracking_prefixes) == 1:
                self.where += f""" AND left(main.shipping_label_tracking_number,1) = {tracking_prefixes[0]}"""
            else:    
                self.where += f""" AND left(main.shipping_label_tracking_number,1) in {tuple(tracking_prefixes)}"""
        if mail_classes: # List
            if len(mail_classes) == 1:
                self.where += f""" AND main.reporting_mail_class_id = {mail_classes[0]}"""
            else:
                self.where += f""" AND main.reporting_mail_class_id in {tuple(mail_classes)}"""
        
        self.query = self.select + self.join + self.where