'''
This module would contain functions related to persisting and retrieving logs and metadata from disk.
'''

import os

LOGS_DIR = "assignment/"

def persist_log(node_id, log):
    '''
    This function persists the given log to disk.
    Args:
        node_id (int): The unique identifier of the node.
        log (List[bytes]): The log to persist.
    
    '''

    pass

def retrieve_log(node_id):
    '''
    This function retrieves the log from disk for the given node.
    Args:
        node_id (int): The unique identifier of the node.
    Returns:
        List[bytes]: The retrieved log.
    '''

    pass

def persist_metadata(node_id, metadata):
    '''
    This function persists the given metadata to disk.
    Args:
        node_id (int): The unique identifier of the node.
        metadata (Any): The metadata to persist.
    '''
    pass

def retrieve_metadata(node_id):
    '''
    This function retrieves the metadata from disk for the given node.
    Args:
        node_id (int): The unique identifier of the node.
    Returns:
        Any: The retrieved metadata.
    '''
    
    pass