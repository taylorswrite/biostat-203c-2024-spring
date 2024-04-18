# PIC 16A HW1
# Name:
# Collaborators:
# Date:

import random # This is only needed in Problem 5

# Problem 1

def print_s(s):
    """
    Prints a given string.
    ---
    Args:
        s: A string.
    Returns:
        None
    ---
    """
    print(s)

# you do not have to add docstrings for the rest of these print_s_* functions.

def print_s_lines(s):
    print(s.replace(': ', '\n'))

def print_s_parts(s):
    print('\n'.join(s.replace(': ', '\n').split(sep = '\n')[::2]))

def print_s_some(s):
    print('\n'.join(sorted(s.split('\n'), key = len)[:-1]))

def print_s_change(s):
    sc = s.replace('math', 'data science')
    sc = s.replace('long division', 'machine learning')
    print(sc)

# Problem 2 

def make_count_dictionary(L):
    """
    Returns a dict of the number of occurance of an object in a list.
    ---
    Args:
        L: A string.
    Returns:
        D: A dictionary.
    ---
    """
    d_k = sorted(list(set(L)))
    d_v = []
    for i in d_k:
        d_v.append(L.count(i))
    D = dict(zip(d_k, d_v))
    return D

# Problem 3

def gimme_an_odd_number():
    """
    Returns a list of user inputed integers. Loops terminates when the user 
    enters an odd number.
    ---
    Args:
        none
    Returns:
        usr_list: A list.
    """
    usr_list = []
    while True:
        usr = int(input('Please enter an integer.'))
        if usr % 2 == 1:
            usr_list.append(usr)
            return usr_list
        else:
            usr_list.append(usr)# replace with your code

# Problem 4

def get_triangular_numbers(k):
    ''' WRITE YOUR OWN DOCSTRING HERE
    '''
    pass # replace with your code


def get_consonants(s):
    ''' WRITE YOUR OWN DOCSTRING HERE
    '''
    pass # replace with your code


def get_list_of_powers(X, k):
    ''' WRITE YOUR OWN DOCSTRING HERE
    '''
    pass # replace with your code


def get_list_of_even_powers(L, k):
    ''' WRITE YOUR OWN DOCSTRING HERE
    '''
    pass # replace with your code



# Problem 5

def random_walk(ub, lb):
    ''' WRITE YOUR OWN DOCSTRING HERE
    '''
    pass # replace with your code


# If you uncomment these two lines, you can run 
# the gimme_an_odd_number() function by
# running this script on your IDE or terminal. 
# Of course you can run the function in notebook as well. 
# Make sure this stays commented when you submit
# your code.
#
# if __name__ == "__main__":
#     gimme_an_odd_number()