# Generalized API Caller
# Parses data for use in Tracking Program

import requests
import pickle
import os
import logging
import sys
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import pandas as pd
from threading import Thread
from datetime import datetime as dt
from queue import Queue

DEFAULT_LEVEL = logging.INFO #change to DEBUG for QA/Testing
logger = logging.getLogger()
if (logger.hasHandlers()):
    logger.handlers.clear()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

class PartnerAPI:

    def __init__( self , partner_name , url , id_batches , num_threads = 35 ):
        
        self.name = partner_name
        self.url = url
        self.batches = id_batches # These are dictionaries. Key--Name of the Batch. Values--List of tracking numbers.
        self.num_threads = num_threads

    def partner_url( self , tracking_ids ):
        """ Input: List of tracking ids
            Output: Target API url
            Can be called alone or within a queue. 
        """
        
        partner_url = self.url
        target_url = partner_url + tracking_ids[0]
        for i in tracking_ids[1:]:
            target_url += tracking_ids[i]

        return target_url
    
    def call_api( self , target_url ):
        """ Input: Target API url (with shipment info)
            Output: API returns a list of json events. One for each shipment.
            Can be called alone or within a queue.
        """

        with requests.Session() as s:

            retries = Retry( total=2 , backoff_factor=0.3 )
            s.mount( target_url , requests.adapters.HTTPAdapter( max_retries = retries ) )
            results = s.get( target )
            s.close()
            shipments = results.json()

        return shipments

    def parse_data( self , api_response_fn ):
            """ Input: Filename of api response pickle
                Output: List of dataframes of shipment events
                Can be called alone or within a queue.
            """

        with open( api_response_fn ) as file:
            shipments = pickle.load( file ) # List of Shipments

        parsed = []

        for parcel in shipments:

            try:
                tracking_id = parcel['Tracking_ID']
                status = parcel['Parcel_Status']

                try:
                    events = parcel['Event_Scans']
                except:
                    events = None
                
                if events:
                    events_df = pd.DataFrame( events )
                    events_df['TrackingID'] = tracking_id
                    events_df['Status'] = status

                    # Parse Datetime:
                    if 'T' in events['Datetime']:
                        events_df['Datetime'] = events_df.TimeStamp.apply( lambda x: dt.strptime( x[:19], '%Y-%m-%dT%H:%M:%S') )
                    else:
                        events_df['Datetime'] = pd.to_datetime( events_df['Datetime'] )

                else: # No "Event_Scans" found.
                    logging.debut( f"""No event scans found for {parcel['Tracking_ID]}""" )

            except:
                events_df = pd.DataFrame()
                events_df['Status'] = 'RRD API Response: ' + parcel['Message']
                logging.debug( f"""Error parsing API for shipment in {api_response_fn}.""" )

            if tracking_id:
                parsed.append( events_df )
            else:
                continue

        return parsed

    def call_api_queued( self , q , num_jobs ):
        """ Calls call_api() for each task in the API queue. """

        while not q.empty():
            s = q.qsize()
            batch = q.get()

            progress_perc = round( num_jobs / s , 2 ) * 100
            while progress_perc % 5 :
                sys.stdout.write( f"""{progress_perc} % of {num_jobs} complete.""" )
                sys.stdout.flush()

            targets = batch['urls'] # Returns target url for batch of ids.

            shipments = self.call_api( targets ) # API returns a list of json events. One for each shipment.

            with open( os.path.join( self.save_folder , self.name + '_API_Batches' , batch['name'] ) ) as file:
                pickle.dump( shipments , file )

        q.task_done()

    def parse_data_queued( self , q , num_jobs ):

        while not q.empty():
            s = q.qsize()
            batch = q.get()
        
        progress_perc = round( num_jobs / s , 2 ) * 100
        while progress_perc % 5 :
            sys.stdout.write( f"""{progress_perc} % of {num_jobs} complete.""" )
            sys.stdout.flush()
        
        file = glob( os.path.join( self.save_folder , self.name + '_API_Batches' , batch['name'] ) )
        df_list = parse_data()

        for df in df_list:
            tracking_id = df['Tracking_ID']

            with open( os.path.join( self.save_folder ) , self.name + '_API_Details' , tracking_id ) ) as file:
                pickle.dump( df , file )

        q.task_done()

    def call_queue( self ):
        """ Uses Class Attributes:  batches, num_threads, save_folder
            Uses Class Methods:  partner_url(), call_api()
            Output:  pickles API response
        """

        q = Queue( maxsize = 0 )

        with q.mutex:
            q.queue.clear()

        for name , ids in self.batches.items():
            # Batches are named because two processes will be performed on this data:
                # Get from API
                # Parse API Data
            
            batch = {}
            batch['name'] = name
            batch['urls'] = [ self.partner_url( i ) for i in ids ]
            q.put( batch )

        logging.info('API queue created.')
        num_jobs = len( self.batches )
        workers = []

        for i in range( self.num_threads ):
            worker = Thread( target = self.call_api_queued , args = ( q , num_jobs ) )
            workers.append( worker )
            worker.start()

        for w in workers:
            w.join()

    def parse_queue( self ):
        """ Uses Class Attributes:  batches, num_threads, save_folder
            Uses Class Methods:  parse_data()
            Output:  pickles dataframe of parsed api response
        """

        q = Queue( maxsize = 0 )

        with q.mutex:
            q.queue.clear()

        batch_names = [ name for name in self.batches ]

        for name in batch_names:
            q.put( name )

        logging.info('Parse Queue created.')
        num_job = len( self.batches )
        workers = []

        for i in range( self.num_threads ):
            worker = Thread( target = self.prase_data_queued , args = ( q , num_jobs ) )
            workers.append( worker )
            worker.start()

        for w in workers:
            w.join()
