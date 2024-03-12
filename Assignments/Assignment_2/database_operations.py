'''
This file contains the functions that will be called by the server to perform the database operations.
'''

def handle_set(key, value, term):
    '''
    This function sets the value of the specified key in the database.
    Args:
        key (str): The key to set.
        value (str): The value to set.
        term (int): The term of the operation.
    '''

    pass

def handle_get(key):
    '''
    This function retrieves the value of the specified key from the database.
    Args:
        key (str): The key to retrieve.
    Returns:
        str: The value of the key, or None if the key does not exist.
    '''

    pass

def handle_no_op(term):
    '''
    This function performs a no-op operation to update the term in the database.
    Args:
        term (int): The term of the operation.
    '''

    pass