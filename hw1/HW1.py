# PIC 16A HW1
# Name:
# Collaborators:
# Date:

import random # This is only needed in Problem 5
# random.seed(1)

# Problem 1

def print_s(s):
    """
    Prints a given string.
    ---
    Args:
        s: A string.
    Returns:
        None
    """
    print(s)

# you do not have to add docstrings for the rest of these print_s_* functions.

def print_s_lines(s):
    print(s.replace(': ', '\n'))

def print_s_parts(s):
    sos = s.replace(' ', '').replace(':', '\n').split(sep = '\n')[::2]
    print('\n'.join(sos))

def print_s_some(s):
    print('\n'.join(sorted(s.split('\n'), key = len, reverse = True)[1:]))

def print_s_change(s):
    sc = s.replace('math', 'data science')
    sc = sc.replace('long division', 'machine learning')
    print(sc)

# Problem 2 

def make_count_dictionary(L):
    """
    Return a dictionary of the frequency of characters in a list.
    ---
    Args:
        L: A list
    Returns:
        D: A dictionary
    """
    L_c = []
    d_k = []
    d_v = []       
    for i in L:
        L_c.append(i)
        if i not in d_k:
            d_k.append(i)
    for i in d_k:
        d_v.append(L_c.count(i))
    D = dict(zip(d_k, d_v))
    return D

# Problem 3

def gimme_an_odd_number():
    """
    A loop that terminates when a user inputs an odd number.
    Returns a list of user inputted numbers.
    ---
    Args:
        None
    Returns:
        usr_list: A list
    """
    usr_list = []
    print("")
    while True:
        usr = int(input("Please enter an integer."))
        if usr % 2 == 1:
            usr_list.append(usr)
            return usr_list
        else:
            usr_list.append(usr)
            
# Problem 4

def get_triangular_numbers(k):
    """
    Returns a list of the number of objects needed to make
    a k-sided, equalateral triangle from 1 to k.
    ---
    Args:
        k: An Integer
    Returns:
        num_list: A list
    """
    num_list = []
    for i in range(1, k + 1):
        num_list.append(int(i * (i + 1) / 2))
    return num_list

def get_consonants(s):
    """
    Returns a list of characters that are not a vowel, space, comma, or period.
    ---
    Args:
        s: A string
    Returns:
        cp_list: A list
    """
    rm_list = ["a", "e", "i", "o", "u", " ", ",", ".",]
    cp_list = []
    for i in s:
        if i in rm_list:
            pass
        else:
            cp_list.append(i)
    return cp_list


def get_list_of_powers(X, k):
    """
    Returns a 2 dimentional list of integers. Each element is a list
    of the powers of an element of X from 0 to k.
    ---
    Args:
        X: List of integers
        k: An Integer
    Returns:
        L: A 2-dimentional list
    """
    L = []
    for i in X:
        L_sub = []
        for j in range(0, k + 1):
            L_sub.append(i**j)
        L.append(L_sub)
    return L


def get_list_of_even_powers(X, k):
    """
    Returns a 2 dimentional list of integers. Each element is a list
    of the even powers of an element of X from 0 to k.
    ---
    Args:
        X: List of integers
        k: An Integer
    Returns:
        L: A 2-dimentional list
    """
    L = []
    for i in X:
        L_sub = []
        for j in range(0, k + 1, 2):
            L_sub.append(i**j)
        L.append(L_sub)
    return L



# Problem 5

def random_walk(ub, lb):
    """
    Returns the last postion, position history, and step history for a fair
    coin toss where head moves forwards (+1) and tails moves backwards (-1).
    """
    pos = 0
    positions = [0]
    steps = []
    for _ in range(lb, ub + 1):
        x = random.choice(["heads", "tails"])
        if x == "heads":
            pos += 1
            positions.append(pos)
            steps.append(1)
        else:
            pos += -1
            positions.append(pos)
            steps.append(-1)            
    return pos, positions, steps


# If you uncomment these two lines, you can run 
# the gimme_an_odd_number() function by
# running this script on your IDE or terminal. 
# Of course you can run the function in notebook as well. 
# Make sure this stays commented when you submit
# your code.
#
# if __name__ == "__main__":
#     gimme_an_odd_number()