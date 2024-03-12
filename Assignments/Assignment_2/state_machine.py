'''
This file contains the definition of the state machine that Raft operates on. 
It includes functions for applying committed log entries to the state machine and retrieving the current state.
'''

class StateMachine:
    def apply_entry(self, entry):
        '''
        This function applies a committed log entry to the state machine.
        Args:
            entry (bytes): The log entry to apply.
        '''

        pass
    
    def get_state(self):
        '''
        This function returns the current state of the state machine.
        Returns:
            Any: The current state of the state machine.
        '''
        
        pass
