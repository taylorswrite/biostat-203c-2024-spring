#!/usr/bin/env python
# coding: utf-8

# # Homework 3
# 
# Due date: May 19, 2024
# 
# Submission instructions: 
# - __Autograder will not be used for scoring, but you still need to submit the python file converted from this notebook (.py) and the notebook file (.ipynb) to the code submission window.__ 
# To convert a Jupyter Notebook (`.ipynb`) to a regular Python script (`.py`):
#   - In Jupyter Notebook: File > Download as > Python (.py)
#   - In JupyterLab: File > Save and Export Notebook As... > Executable Script
#   - In VS Code Jupyter Notebook App: In the toolbar, there is an Export menu. Click on it, and select Python script.
# - Submit `hw3.ipynb` and `hw3.py` on Gradescope under the window "Homework 3 - code". Do **NOT** change the file name.
# - Convert this notebook into a pdf file and submit it on Gradescope under the window "Homework 3 - PDF". Make sure all your code and text outputs in the problems are visible. 
# 
# 
# This homework requires two new packages, `pyarrow` and `duckdb`. Pleas make sure to install them in your `BIOSTAT203C-24S` environment:
# 
# ```bash
# conda activate BIOSTAT203C-24S
# conda install -c conda-forge pyarrow python-duckdb
# ```

# In[1]:


import sys
import gzip
import time
import random
import duckdb
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
import pyarrow.parquet as pq
from matplotlib import pyplot as plt

# Create a syslink named `mimic` in working directory. 
pth_hsp = './mimic/hosp/'
pth_icu = './mimic/icu/'

# Common Objects. Added here in case of kernel crash.
lab_col = ['subject_id', 'itemid', 'charttime', 'valuenum']
lab_itemid = [50912, 50971, 50983, 50902, 50882, 51221, 51301, 50931]


# ## Problem 1. 
# 
# Recall the simple random walk.  At each step, we flip a fair coin. If heads, we move "foward" one unit; if tails, we move "backward." 
# 
# ### (A).
# 
# Way back in Homework 1, you wrote some code to simulate a random walk in Python. 
# 
# Start with this code, or use posted solutions for HW1. If you have since written random walk code that you prefer, you can use this instead. Regardless, take your code, modify it, and enclose it in a function `rw()`. This function should accept a single argument `n`, the length of the walk. The output should be a list giving the position of the random walker, starting with the position after the first step. For example, 
# 
# ```python
# rw(5)
# [1, 2, 3, 2, 3]
# ```
# 
# Unlike in the HW1 problem, you should not use upper or lower bounds. The walk should always run for as long as the user-specified number of steps `n`. 
# 
# Use your function to print out the positions of a random walk of length `n = 10`. 
# 
# Don't forget a helpful docstring!

# ### ANSWER 1A

# #### Documentation
# rw  takes a positive integer, `n`, and returns a list of all positions from a random walk of `n` steps where each step is decided by a coin flip. If the coin flip results in heads, step forward. If the coin flip results in tails, step backward. The variable, `pos`, is the current position; therefore, append that position after each step.
# 
# If n is not an integer or `n` is less than zero, the user will be prompted to enter a new `n`.

# In[2]:


def rw(n):
  """
  A list of positions from a random walk.
  ---
  Args:
    n: A positive integer. Number of steps.
  Returns:
    positions: A list of poistions.
  """
  # Input Catch. Must be int.
  while not isinstance(n, int) or n < 0:
    user = input("Enter a positive integer for n: ")
    # try to make users input an int
    try:
        n = int(user)
    # Supply back into the while while loop if error
    except ValueError:
        n
  pos = 0 # Initialized current position
  positions = [] # Empty list. Track positions after a step.
  while len(positions) < n:
    x = random.choice(["heads", "tails"])
    if x == "heads":
        pos += 1 # If heads, move forward one step.
        positions.append(pos) # Append position after step.
    elif x == "tails":
        pos -= 1 # If tails, move backward one step.
        positions.append(pos) # Append position after step.
  return positions

rw(10)


# #### Robustness Test

# In[3]:


rw("Oops")


# In[4]:


rw(-10)


# In[5]:


rw(10.0)


# In[6]:


# Comprehensive test. All test in list should result in no errors
tests = [0, 1, 5, 30, 50]
# Store results in a dictionary.
rw_results = dict()
# loop through tests and store in dictionary
for test in tests:
  # label for the values
  name = f"{test} Random Positions"
  # Store the result in the dictionary
  rw_results[name] = rw(test)

# Loop through dictionary and print the results in a readable format
# also print the length of the object
for key, val in rw_results.items():
  print(f"{key} (length = {len(val)}):\n{val}")


# ### (B). 
# 
# Now create a function called `rw2(n)`, where the argument `n` means the same thing that it did in Part A. Do so using `numpy` tools. Demonstrate your function as above, by creating a random walk of length 10. You can (and should) return your walk as a `numpy` array. 
# 
# **Requirements**: 
# 
# - No for-loops. 
# - This function is simple enough to be implemented as a one-liner of fewer than 80 characters, using lambda notation. Even if you choose not to use lambda notation, the body of your function definition should be no more than three lines long. Importing `numpy` does not count as a line. 
# - A docstring is required if and only if you take more than one line to define the function. 
# 
# **Hints**:
# 
# - Check the documentation for `np.random.choice()`. 
# - `np.cumsum()`. 
# 

# ### Answer 1B

# #### Documentation
# `rw2` is a lambda function that uses numpy to perform the same operations as `rw`. Because of numpy's vast number of methods that can be applied on a numpy array, the function was able to be simplified into one line without the input catch error. `rw2` uses the numpy and random libraries to create an array of -1 and 1 with replacement which signify a coin flip of heads and tails. The cumulative sum of the numpy array acts like the current position of the random walk.

# In[7]:


# Return positions from n random steps
# random choice is used like a coin flip 1=heads -1=tails
# cumsum is used to get the position after each step.
rw2 = lambda n: np.random.choice([-1, 1], size=n, replace=True).cumsum()

rw2(10)


# In[8]:


# list of error free tests because rw2 does not have an input catch.
tests = [0, 1, 5, 30, 50]
# store results in dictionary
rw_results = dict()
# loop through the tests and store the results in the dictionary
for test in tests:
  name = f"{test} Random Positions"
  rw_results[name] = rw2(test)
# loop thought the dictionary and print the results in a readable format.
# Also output the length of the object.
for key, val in rw_results.items():
  print(f"{key} (length = {len(val)}):\n{list(val)}")


# ### (C).
# 
# Use the `%timeit` magic macro to compare the runtime of `rw()` and `rw2()`. Test how each function does in computing a random walk of length `n = 10000`. 

# ### Answer 1C

# In[9]:


get_ipython().run_cell_magic('timeit', '', 'rw(10_000)\n')


# In[10]:


get_ipython().run_cell_magic('timeit', '', 'rw(1_000)\n')


# In[11]:


get_ipython().run_cell_magic('timeit', '', 'rw(100)\n')


# In[12]:


get_ipython().run_cell_magic('timeit', '', 'rw2(10_000)\n')


# In[13]:


get_ipython().run_cell_magic('timeit', '', 'rw2(1_000)\n')


# In[14]:


get_ipython().run_cell_magic('timeit', '', 'rw2(100)\n')


# ### (D). 
# 
# Write a few sentences in which you comment on (a) the performance of each function and (b) the ease of writing and reading each function. 

# ### ANSWER 1D
# 
# The function, `rw2()`, was 82% faster than the function, `rw()`. Built-in python list operations were used in `rw()` and numpy array operations were used in `rw2()`. The average run-times for `rw()` and `rw2()` were 61.1 µs and 362 µs, respectively. The numpy array function, `rw2()`, was easier to write, occupying only one line, and was more readable than the built-in python list operations in `rw()`.
# 
# For robustness, multiple values of an were supplied to the functions to test if different lengths resulted in differing results. from this test numpy was always significantly faster than using base python functions.

# ### (E). 
# 
# 
# In this problem, we will perform a `d`-dimensional random walk. There are many ways to define such a walk. Here's the definition we'll use for this problem: 
# 
# > At each timestep, the walker takes one random step forward or backward **in each of `d` directions.** 
# 
# For example, in a two-dimensional walk on a grid, in each timestep the walker would take a step either north or south, and then another step either east or west. Another way to think about is as the walker taking a single "diagonal" step either northeast, southeast, southwest, or northwest. 
# 
# Write a function called `rw_d(n,d)` that implements a `d`-dimensional random walk. `n` is again the number of steps that the walker should take, and `d` is the dimension of the walk. The output should be given as a `numpy` array of shape `(n,d)`, where the `k`th row of the array specifies the position of the walker after `k` steps. For example: 
# 
# ```python
# P = rw_d(5, 3)
# P
# ```
# ```
# array([[-1, -1, -1],
#        [ 0, -2, -2],
#        [-1, -3, -3],
#        [-2, -2, -2],
#        [-1, -3, -1]])
# ```
# 
# In this example, the third row `P[2,:] = [-1, -3, -3]` gives the position of the walk after 3 steps. 
# 
# Demonstrate your function by generating a 3d walk with 5 steps, as shown in the example above. 
# 
# All the same requirements and hints from Part B apply in this problem as well. It should be possible to solve this problem by making only a few small modifications to your solution from Part B. If you are finding that this is not possible, you may want to either (a) read the documentation for the relevant `numpy` functions more closely or (b) reconsider your Part B approach. 
# 
# 
# 
# 

# ### Answer 1E

# #### Documentation
# `rw_d` uses the numpy library to list positions from a multi directional random walk. It takes two values. `d` describes the number of directions. `n`, describes the number of steps in each direction. Both `d` and `n` must be positive integers, but if positive integers are not supplied then the user is prompted to enter a new set. The biggest difference between `rw_d` and `rw2` is that the array is reshaped into the correct shape and a cumulative sum is applied along the vertical axis. 

# In[102]:


def rw_d(n, d):
  """
  A list of positions from a multidimential random walk.
  ---
  Args:
    n: A positive integer. The number of steps in each dimention.
    d: A positive integer. The number of dimentions.
  Return:
    positions: An numpy array. The positions after step in a dimention.
  """
  while not isinstance(n, int) or n < 0:
    user_n = input("Enter a positive integer for n: ")
    try:
      n = int(user_n)
    except ValueError:
        pass
  while not isinstance(d, int) or d < 0:
    user_d = input("Enter a positive integer for d: ")
    try:
      d = int(user_d)
    except ValueError:
        pass
  positions = (
    np.random.choice(
      [-1, 1], # -1 for tails and 1 for heads.
      size = (n * d), # The number of elements in array.
      replace = True) # Reuse heads and tails.
    .reshape(n, d) # Reshape into n rows and d columns.
    .cumsum(axis=0) # Columnar Cumulative Sum 
  )
  return positions

rw_d(5,3)


# #### Robustness Test

# In[103]:


rw_d('goggle', 2)


# In[104]:


rw_d(2, "bing")


# In[105]:


# list of tests to perform
tests = [[0, 0], [1, 5],[10, 10],[10, 5]]
# Store in dictionary
rw_results = dict()
# loop through tests
for test in tests:
  # for each list, [0]=n and [1]=d
  n = test[0]
  d = test[1]
  # label the results
  name = f"{test} Random Positions"
  # store the results in dictionary
  rw_results[name] = rw_d(n, d)
# Loop through the dictionary and print the results in a readable format
# Also add the shape of the array.
for key, val in rw_results.items():
  print(f"{key} (Shape = {val.shape}):\n{val}")


# ### (F).
# 
# In a few sentences, describe how you would have solved Part E without `numpy` tools. Take a guess as to how many lines it would have taken you to define the appropriate function. Based on your findings in Parts C and D, how would you expect its performance to compare to your `numpy`-based function from Part E? Which approach would your recommend? 
# 
# Note: while I obviously prefer the `numpy` approach, it is reasonable and valid to prefer the "vanilla" way instead. Either way, you should be ready to justify your preference on the basis of writeability, readability, and performance. 

# ### Answer 1F
# 
# Without using numpy tools, Part E could be solved by creating a 2-D list. Use a for-loop to call `rw()`  d times with an input of n. For each iteration, zip the old list with the new list. Results in an array shape of (n,d). The resulting function would be approximately 9 lines long (20 lines including `rw()`) and would have a longer runtime than if numpy tools were used. The numpy based function from part E 5-10 times faster than the non-numpy function proposed. Therefore, the utilization of numpy tooling is preferred for its readability and  speed.

# ### (G).
# 
# Once you've implemented `rw_d()`, you can run the following code to generate a large random walk and visualize it. 
# 
# ```python
# from matplotlib import pyplot as plt
# 
# W = rw_d(20000, 2)
# plt.plot(W[:,0], W[:,1])
# ```
# 
# You may be interested in looking at several other visualizations of multidimensional random walks [on Wikipedia](https://en.wikipedia.org/wiki/Random_walk). Your result in this part will not look exactly the same, but should look qualitatively fairly similar. 
# 
# You only need to show one plot. If you like, you might enjoy playing around with the plot settings. While `ax.plot()` is the normal method to use here, `ax.scatter()` with partially transparent points can also produce some intriguing images. 

# ### Answer 1G

# In[19]:


# Plot 2-D Random Walk
W = rw_d(20000, 2)
plt.plot(W[:,0], W[:,1])


# ## Problem 2. Reading MIMIC-IV datafile
# In this exercise, we explore various tools for ingesting the [MIMIC-IV](https://mimic.mit.edu/docs/iv/) data introduced in BIOSTAT 203B, but we will do it in Python this time.
# 
# Let's display the contents of MIMIC `hosp` and `icu` data folders: (if a cell starts with a `!`, the command is run in the shell.)

# ### Directions To Setup Mimic
# 
# - Create a system link (`ln -s`) between Mimic and the current working directory of this notebook. Using a system link is storage efficient and allows for more readable relative paths to be used to reference the mimic data.
# 
# - Use the tags `-goh` to show permissions, size, and filename.

# In[20]:


# Hosp Files
get_ipython().system('ls -goh ./mimic/hosp/')


# In[21]:


# ICU Files
get_ipython().system('ls -goh ./mimic/icu/')


# ### (A). Speed, memory, and data types
# 
# Standard way to read a CSV file would be using the `read_csv` function of the `pandas` package. Let us check the speed of reading a moderate-sized compressed csv file, `admissions.csv.gz`. How much memory does the resulting data frame use?
# 
# _Note:_ If you start a cell with `%%time`, the runtime will be measured. 

# In[22]:


# this function is not required as part of the homework. The purpose of
# This function is to calculate the percent differences of data ingestion 
# methods.
def percent_diff(i_time=None, f_time=None, i_mem=None, f_mem=None):
  """
  Print Pandas and Polars Comparison Metrics
  ---
  Optional Args:
    i_time: A positive float. The runtime of a initial cell in s.
    f_time: A positive float. The runtime of a final cell in s.
    i_mem: A positive float. Initial dataframe object memory usage in GB.
    f_mem: A positive float. Final dataframe object memory usage in GB.
  Return:
    None
  """
  if (i_time is not None) and (f_time is not None): # Check if empty
    time_diff = round((f_time - i_time) / i_time * 100) # percent diff.
    if time_diff > 0: # if percent diff. is greater than 0 output text
      print(f"final runtime was {abs(time_diff)}% slower than initial.")
    elif time_diff < 0: # if percent diff. is less than 0 output text
      print(f"final runtime was {abs(time_diff)}% faster than initial.")
    elif time_diff == 0: # if percent diff. is equal to 0 output text
      print(f"final runtime was the same as initial.")
    else:
      raise TypeError
  else:
    pass
  if (i_mem is not None) and (f_mem is not None): # Check if empty
    mem_diff = round((f_mem - i_mem) / i_mem * 100) # percent diff
    if mem_diff > 0: # if percent diff. is greater than 0 output text
      print(f"final df memory usage was {abs(mem_diff)}% more than initial.")
    elif mem_diff < 0: # if percent diff. is less than 0 output text
      print(f"final df memory usage was {abs(mem_diff)}% less than initial.")
    elif mem_diff == 0: # if percent diff. is equal to 0 output text
      print(f"final df memory usage was the same as initial.")
    else:
      raise TypeError
  else:
    pass


# ### Answer 2A
# 
# #### Preface
# 
# It is more beneficial to compare the time and memory consumption of pandas to another method to better understand strengths and weaknesses of data ingestion methods therefore for each problem pandas will be compared to either polars or DuckDB. Runtime is a good metric to measure the performance of these libraries however, it is not consistent and will vary across systems and even within the same system depending on resource availability. Therefore the relative performance is a better metric than the absolute performance of these python libraries.
# 
# EDIT: The changes to the homework implimented on 5/6 disrupted the order of my test. Therefore, please don't markdown if the numbers changed significantly because the homework was significantly changed after I completed the assignment. Please don't change the homework after you disperse it. It is disruptive.
# 
# #### Pandas
# 
# The runtime to ingest `admissions.csv.gz` using pandas was 884 ms and the resulting data frame memory usage was 0.368 GB.
# 
# #### Polars
# 
# The runtime to ingest `admissions.csv.gz` using pandas was 214 ms and the resulting data frame memory usage was 0.362 GB.
# 
# #### Summary
# 
# Polars consumed less resources than pandas. The Polars runtime was __42% faster__ and the Polars data frame object memory usage was __2% less__ than Pandas. 

# #### Description
# Pandas was used to read in the admissions.csv.gz file from a symbolic link of the mimic data set located in the working directory.

# In[23]:


get_ipython().run_cell_magic('time', '', '## Pandas\n# Read admissions using Pandas\ndf_pd_adm = pd.read_csv("./mimic/hosp/admissions.csv.gz")\n')


# In[24]:


# Print the memory usage of the pandas object
print(round(sys.getsizeof(df_pd_adm)/10**9, 3), "GB")


# #### Description
# Polars was used to read in the admissions.csv.gz file from a symbolic link of the mimic data set located in the working directory. Results are documented in the answer above. To make the memory usage consistent, the polars data frame was converted to a pandas data frame.

# In[25]:


get_ipython().run_cell_magic('time', '', '## Polars\n# Read admissions using Polars then convert the Polars DataFrame (pl.DataFrame)\n# to a Pandas DataFrame (pd.DataFrame).\ndf_pl_adm = pl.read_csv("./mimic/hosp/admissions.csv.gz").to_pandas()\n')


# In[26]:


# Print the memory usage of the pandas object.
# The pl.DataFrame was converted to a pd.DataFrame
print(round(sys.getsizeof(df_pl_adm)/10**9, 3), "GB")


# In[106]:


# Initial: Pandas reading admissions.csv.gz
# Final: Polars reading admissions.csv.gz
percent_diff(i_time=1.17, i_mem=0.368, f_time=0.690, f_mem=0.362)


# ### (B). User-supplied data types
# 
# Re-ingest `admissions.csv.gz` by indicating appropriate column data types in [`pd.read_csv`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html). Does the run time change? How much memory does the result dataframe use? (Hint: `dtype` and `parse_dates` arguments in `pd.read_csv`.)

# ### Answer 2B
# 
# 
# #### Pandas (Categorical) 
# 
# Categorical variables were assigned as type `categorical`. Dates were assigned as date. The runtime to ingest `admissions.csv.gz` using pandas and declaring data types was 1.05 seconds and the resulting data frame memory usage was 0.029 GB. Compared to pandas reading without declaring data types (1A), the pandas runtime with declared data types was __10% Faster__ and the data frame memory usage was __92% less__.
# 
# #### Pandas (String)
# 
# Categorical variables were assigned as type `string`. Dates were assigned as date. The runtime to ingest `admissions.csv.gz` using pandas and declaring data types was 3.76 seconds and the resulting data frame memory usage was 0.277 GB. Compared to pandas reading without declaring data types (1A), the pandas runtime with declared data types was __6% Faster__ and the data frame memory usage was __24% less__.
# 
# #### Polars (Categorical)
# 
# Categorical variables were assigned as type `categorical`. Dates were assigned as date. The runtime to ingest `admissions.csv.gz` using polar and declaring data types was 0.207 seconds and the resulting data frame memory usage was 0.029 GB. Compared to pandas reading without declaring data types (1A), the polars runtime with declared data types was __81% faster__ and the data frame memory usage was __92% less__.
# 
# 
# #### Summary
# For pandas, declaring variables decreased the runtime and decreased the size of the resulting data frame. Additionally, for pandas the size of the resulting data frame was less when categorical variables were assigned the type, `categorical`, instead of `string`. Polars, was the fastest and had similar memory usage as pandas. This makes sense becasue polars is multi-threaded and built with rust.

# #### Documentation
# 
# The admissions data set was read by pandas and while reading in the data, types were declared. declaring column has the potential to improve resource usage while upholding data, integrity. In addition, polars was used to also read in the data using a similar method of declaring column. for pandas two methods were tested. One method declared categorical variables as string type however that did not perform as well as declaring the categorical variables as categories instead of strings. To make the memory usage consistent, the polars data frame was converted to a pandas data frame.

# In[28]:


get_ipython().run_cell_magic('time', '', '\n## Pandas\n# Assign data types while reading. For categorical variables assign the dtype as\n# a category.\ndf_pd_cat_adm = pd.read_csv(\n  "./mimic/hosp/admissions.csv.gz",\n  # assign data types\n  dtype={\n    \'subject_id\': \'int64\',\n    \'hadm_id\': \'int64\',\n    \'admission_type\': \'category\',\n    \'admit_provider_id\': \'category\',\n    \'admission_location\': \'category\',\n    \'discharge_location\': \'category\',\n    \'insurance\': \'category\',\n    \'language\': \'category\',\n    \'marital_status\': \'category\',\n    \'race\': \'category\',\n    \'hospital_expire_flag\': \'category\'\n  },\n  # assign date types\n  parse_dates=[\n    \'admittime\',\n    \'dischtime\',\n    \'deathtime\',\n    \'edregtime\',\n    \'admittime\',\n    \'edouttime\',\n    \'deathtime\',\n  ]\n)\n')


# In[29]:


# Object memory usage
print(round(sys.getsizeof(df_pd_cat_adm)/10**9,3), "GB")


# In[107]:


# Initial = Pandas without declared data types
# Final = Pandas with declared data types (categorical)
percent_diff(i_time=1.17, i_mem=0.368, f_time=1.05, f_mem=0.029)


# In[31]:


get_ipython().run_cell_magic('time', '', '\n## Pandas\n# Assign data types while reading. For categorical variables assign the dtype as\n# a string.\ndf_pd_str_adm = (\n  pd.read_csv(\n    "./mimic/hosp/admissions.csv.gz",\n    # assign data types\n    dtype={\n      \'subject_id\': \'int64\',\n      \'hadm_id\': \'int64\',\n      \'admission_type\': \'string\',\n      \'admit_provider_id\': \'string\',\n      \'admission_location\': \'string\',\n      \'discharge_location\': \'string\',\n      \'insurance\': \'string\',\n      \'language\': \'string\',\n      \'marital_status\': \'string\',\n      \'race\': \'string\',\n      \'hospital_expire_flag\': \'string\'\n    },\n    # assign date types\n    parse_dates=[\n      \'admittime\',\n      \'dischtime\',\n      \'deathtime\',\n      \'edregtime\',\n      \'admittime\',\n      \'edouttime\',\n      \'deathtime\',])\n)\n')


# In[32]:


# Object memory Usage
print(round(sys.getsizeof(df_pd_str_adm)/10**9, 3), "GB")


# In[108]:


# Initial = Pandas without declared data types
# Final = Pandas with declared data types (string)
percent_diff(i_time=1.17, i_mem=0.368, f_time=1.1, f_mem=0.278)


# In[34]:


get_ipython().run_cell_magic('time', '', '\n## Polars\n# Assign data types while reading. For categorical variables assign the dtype as\n# a category.\ndf_pl_cat_adm = (\n  pl.read_csv(\n    "./mimic/hosp/admissions.csv.gz", # read file\n    # assign data types\n    dtypes={ \n      \'subject_id\': pl.Int64,\n      \'hadm_id\': pl.Int64,\n      \'admission_type\': pl.Categorical,\n      \'admit_provider_id\': pl.Categorical,\n      \'admission_location\': pl.Categorical,\n      \'discharge_location\': pl.Categorical,\n      \'insurance\': pl.Categorical,\n      \'language\': pl.Categorical,\n      \'marital_status\': pl.Categorical,\n      \'race\': pl.Categorical,\n      \'hospital_expire_flag\': pl.Categorical}, \n    try_parse_dates=True) # convert columns to date.\n  .to_pandas() # convert to pd.dataframe\n)\n')


# In[109]:


# Polars Object memory usage
print(round(sys.getsizeof(df_pl_cat_adm)/10**9,3), "GB")


# In[111]:


# Initial = Polars without declared data types(initial) 
# Final = Polars with declared data types
percent_diff(i_time=1.17, i_mem=0.368, f_time=0.220, f_mem=0.029)


# ## Problem 3. Ingest big data files
# 
# 
# Let us focus on a bigger file, `labevents.csv.gz`, which is about 125x bigger than `admissions.csv.gz`.

# ##### Documentation
# The below code is bash. The size of lab events is 1.8 GB below that code are the first 10 rows of lab events using bash.

# In[37]:


# Size of labevents as a csv.gz
get_ipython().system('ls -goh ./mimic/hosp/labevents.csv.gz')


# Display the first 10 lines of this file.

# In[38]:


# First 10 results of labevents
get_ipython().system('zcat < ./mimic/hosp/labevents.csv.gz | head -10')


# ### (A). Ingest `labevents.csv.gz` by `pd.read_csv`
# 
# Try to ingest `labevents.csv.gz` using `pd.read_csv`. What happens? If it takes more than 5 minutes on your computer, then abort the program and report your findings. 

# ### Answer 3A
# 
# #### Pandas
# 
# Pandas was able to read `labevents.csv.gz` within 5 minutes. The runtime to read the file was 2 minutes and the resulting data frame memory usage was 61 GB.
# 
# final runtime was 32% faster than initial.
# final df memory usage was 3% more than initial.
# #### Polars
# 
# Polars was able to read `labevents.csv.gz` within 5 min. The runtime to read the file was 1 minute and 22 seconds.
# 
# #### Summary
# 
# The runtime to read in the compressed csv, labevents, using Polars was 32% faster than Pandas, but the resulting memory usage of the dataframe was 3% more which is an acceptable tradeoff for the time savings.

# #### Description
# The compressed CSV labevents was read by pandas and polars. To make the memory usage consistent, the polars data frame was converted to a pandas data frame.

# In[39]:


get_ipython().run_cell_magic('time', '', '#Pandas read labevents.csv.gz\ndf_pd_lab = pd.read_csv("./mimic/hosp/labevents.csv.gz")\n')


# In[40]:


print(round(sys.getsizeof(df_pd_lab)/10**9,3), "GB")


# In[41]:


get_ipython().run_cell_magic('time', '', '#Polars read labevents.csv.gz\ndf_pl_lab = pl.read_csv("./mimic/hosp/labevents.csv.gz").to_pandas()\n')


# In[42]:


print(round(sys.getsizeof(df_pl_lab)/10**9,3), "GB")


# In[112]:


# Initial: Pandas reading labevents
# Final: Polars reading labevents
percent_diff(
  i_time=(2 * 60 + 1),
  i_mem=61.002, 
  f_time=(1 * 60 + 22), 
  f_mem=62.998
)


# ### (B). Ingest selected columns of `labevents.csv.gz` by `pd.read_csv`
# 
# Try to ingest only columns `subject_id`, `itemid`, `charttime`, and `valuenum` in `labevents.csv.gz` using `pd.read_csv`.  Does this solve the ingestion issue? (Hint: `usecols` argument in `pd.read_csv`.)

# ### Answer 3B
# 
# #### Pandas
# 
# The runtime to read selected columns of the file was 1 minute and 7 seconds and the resulting data frame memory usage was 11.817 GB. The runtime for reading selected columns in pandas was __45% faster__ and the resulting data frame memory usage was __81% smaller__ when compared the pandas reading all columns.
# 
# #### Polars
# 
# The runtime to read selected columns of the file was 44.8 seconds and the resulting data frame memory usage was 11.817 GB. The runtime for reading selected columns in polars was __74% faster__ and the resulting data frame memory usage was __81% smaller__ when compared the pandas reading all columns.
# 
# #### Summary
# 
# Again, Polars was the fastest method for reading in an selecting columns.

# Description: While reading the compressed CSV file labevents using pandas, the columns were selected. This was replicated for polars as well. To make the memory usage consistent, the polars data frame was converted to a pandas data frame.

# In[113]:


get_ipython().run_cell_magic('time', '', '# Pandas Read select labevent columns\ndf_pd_lab = pd.read_csv("./mimic/hosp/labevents.csv.gz", usecols = lab_col)\n')


# In[114]:


print(round(sys.getsizeof(df_pd_lab)/10**9,3), "GB")


# In[115]:


# Initial: Pandas reading all columns
# Final: Pandas reading selected columns
percent_diff(
  i_time= (2 * 60 + 1),
  i_mem= 61.002,
  f_time= (1 * 60 + 7),
  f_mem= 11.817
)


# In[116]:


get_ipython().run_cell_magic('time', '', '# Polars Read select labevent columns\ndf_pl_lab = (\n  pl.read_csv("./mimic/hosp/labevents.csv.gz", # read file\n    columns = lab_col) # Select columns\n    .to_pandas() # Convert pl.dataframe to pd.dataframe\n)\n')


# In[117]:


print(round(sys.getsizeof(df_pl_lab)/10**9,3), "GB")


# In[118]:


# Initial: Pandas reading all columns
# Final: Polars reading specific columns
percent_diff(
  i_time= (2 * 60 + 1),
  i_mem= 61.002,
  f_time= (30.9),
  f_mem= 11.817
)


# ### (C). Ingest subset of `labevents.csv.gz`
# 
# Back in BIOSTAT 203B, our first strategy to handle this big data file was to make a subset of the `labevents` data.  Read the [MIMIC documentation](https://mimic.mit.edu/docs/iv/modules/hosp/labevents/) for the content in data file `labevents.csv.gz`.
# 
# As before, we will only be interested in the following lab items: creatinine (50912), potassium (50971), sodium (50983), chloride (50902), bicarbonate (50882), hematocrit (51221), white blood cell count (51301), and glucose (50931) and the following columns: `subject_id`, `itemid`, `charttime`, `valuenum`. 
# 
# Rerun the Bash command to extract these columns and rows from `labevents.csv.gz` and save the result to a new file `labevents_filtered.csv.gz` in the current working directory (Q2.3 of HW2). How long does it take?(_Updated 5/6: You may reuse the file you created last quarter and report the elapsed time from the last quarter for this part._)
# 
# Display the first 10 lines of the new file `labevents_filtered.csv.gz`. How many lines are in this new file? How long does it take `pd.read_csv()` to ingest `labevents_filtered.csv.gz`?
# 

# ### Answer 3C
# 
# The runtime to select columns and filter `labevents` using Bash was 1 minute and 36 seconds and the runtime to ingest `labevents_filtered` was 10 seconds. The number of rows in the resulting dataset was 24,855,909.

# Description: the below bash code reads in the compressed CSV labevents using zcat, then is filtered by Rows that equal the necessary item IDs and finally select the columns. This is then saved to another compressed CSV called lab events filtered. After processing the data using bash, pandas was used to read the file that was created in bash. The first 10 rows of the file was displayed using pandas `head` function.

# In[65]:


get_ipython().run_cell_magic('bash', '', '# Read labevents, select columns, and filter rows by itemid\ntime(zcat < ./mimic/hosp/labevents.csv.gz | \\\n  awk -F, \\\n  \'BEGIN {OFS = ","} {if (NR == 1 || $5 == 50912 || $5 == 50971 ||\n    $5 == 50983 || $5 == 50902 || $5 == 50882 || $5 == 51221 || \n    $5 == 51301 || $5 == 50931) {print $2, $5, $7, $10}}\' | \\\n  gzip > labevents_filtered.csv.gz)\necho "complete"\n')


# In[66]:


get_ipython().run_cell_magic('time', '', '# Pandas read labevents_filtered.csv.gz\ndf_pd_lab_filter = pd.read_csv("./labevents_filtered.csv.gz")\n')


# In[67]:


df_pd_lab_filter.head(10)


# In[68]:


df_pd_lab_filter.shape


# In[69]:


print(round(sys.getsizeof(df_pd_lab_filter)/10**9,3), "GB")


# ### (D). Review
# 
# Write several sentences on what Apache Arrow, the Parquet format, and DuckDB are. Imagine you want to explain it to a layman in an elevator, as you did before. (It's OK to copy-paste the sentences from your previous submission.)
# 
# Also, now is the good time to review [basic SQL commands](https://ucla-biostat-203b.github.io/2024winter/slides/12-dbplyr/dbintro.html) covered in BIOSTAT 203B.

# ### Answer 3D
# 
# #### Apache Arrow
# 
# Apache Arrow is fast. By organizing data in columnar format and reducing redundant operations, it is able to accomplish similar tasks as pandas in a fraction of the time ([Apache Arrow](https://arrow.apache.org/overview/)).
# 
# #### Parquet Format
# 
# Parquet is an efficient storage format. It stores data in a columnar format efficiently by creating encoding dictionaries and bit-packing, allowing it be faster and smaller than csv files ([Apache Parquet](https://parquet.apache.org/docs/file-format/)).
# 
# #### DuckDB
# 
# DuckDB is a fast and portable database management system. DuckDB can connect to database servers or be server-less, performing SQL operations on large files and tables while being memory and time efficient. It also has support across a wide range of languages.([DuckDB](https://duckdb.org/why_duckdb.html#:~:text=DuckDB%20offers%20a%20flexible%20extension,protocols%20are%20implemented%20as%20extensions.)).
# 

# ### (E). Ingest `labevents.csv.gz` by Apache Arrow (modified 5/6)
# 
# Our second strategy again is to use [Apache Arrow](https://arrow.apache.org/) for larger-than-memory data analytics. We will use the package `pyarrow`. Unlike in R, this package works with the `csv.gz` format. We don't need to keep the decompressed data on disk. We could just use `dplyr` verbs in R, but here, we need a different set of commands. The core idea behind the commands are still similar, though. There is one notable difference in approach: 
# 
# - R's `arrow` package allowed lazy evaluation but required `csv` file to be decompressed beforehand. 
# - On the other hand, `pyarrow` allows `csv.gz` format, but lazy evaluation is not available. For larger-than-memory data, streaming approach can be used.
# 
# Follow these steps to ingest the data:
# - Use [`pyarrow.csv.read_csv`](https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html) to read in `labevents.csv.gz`. It creates an object of type [`pyarrow.Table`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html). _If this does not work on your computer, state that fact. It's OK to not complete this part in that case. However, you still need the `filter_table()` function for the next part. It's still recommend to _
# 
# - Define a function `filter_table()` that takes in a `pyarrow.Table` as an argument, and returns `pyarrow.Table` doing the following:
#     - Select columns using the [`.select()`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.select) method. 
#     - Filter the rows based on the column `itemid` using the [`.filter()`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.filter) method. You should use [`Expression`](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Expression) for improved performance. In particular, use the `isin()` method for constructing it.
#     
# - Finally, let's obtain the result in `pandas` `DataFrame` using the method `.to_pandas()`. 
# 
# How long does the ingest+select+filter process take? Display the number of rows and the first 10 rows of the result dataframe, and make sure they match those of (C).
# 

# ### Answer 3E
# 
# #### Apache Arrow
# 
# The runtime to read "labevents.csv.gz", select columns, and filter rows was 1 minute 16 seconds and the resulting data frame memory usage was 0.606 GB. 

# Description: homework three was changed and now requires a function to be used to filter a pyarrow table. Therefore, filter_table was used to select columns and filter rose by items in the table and return a filter table. Inside the function are the columns and item IDs. We wish to filter the table. The arrow functions select and filter were used to filter the data set. In order to use the filter function, additional arrow functions were needed such as `is_in` and `field` to specify which column to filter by and format the input in a compatible way.

# In[70]:


def filter_table(arrow_tbl):
  """
  Select columns in columns_filter and filter rows by items in items_filter from
  a pyarrows table and return a filtered pyarrows table.
  Args:
    arrow_tbl: A pa.Table of labevents.csv.gz
  Return:
    pa_tbl_ftr: A filtered pa.Table of labevents.csv.gz
  """
  lab_col = ['subject_id', 'itemid', 'charttime', 'valuenum']
  lab_itemid = [50912, 50971, 50983, 50902, 50882, 51221, 51301, 50931]
  pa_tbl_ftr = (
    arrow_tbl # pa.dataframe
    .select(lab_col) # select columns
    .filter(pc.is_in(pc.field("itemid"), pa.array(lab_itemid))) # filter by itemid
  )
  return pa_tbl_ftr


# In[71]:


get_ipython().run_cell_magic('time', '', 'df_pa_lab_ftr = csv.read_csv(pth_hsp + "labevents.csv.gz") # read file\ndf_pa_lab_ftr = filter_table(df_pa_lab_ftr).to_pandas()\n')


# In[72]:


print(round(sys.getsizeof(df_pa_lab_ftr)/10**9,3), "GB")


# In[73]:


df_pa_lab_ftr.shape


# In[74]:


df_pa_lab_ftr.head(10)


# ### (F). Streaming data (added 5/6)
# 
# When working with the `csv.gz` file, the entire file will need to be decompressed in memory, which might not be feasible. You can stream data, and processing them in several chunks that fits into the memory.
# 
# If the function `filter_table()` is defined correctly, the following should successfully ingest the data. Discuss what this code is doing in markdown. Also, add sufficient comment to the code. 

# ### Answer 3F
# 
# #### Apache Arrow (Read, Select, Filter, then Convert to parquet)
# 
# The runtime to read, select, filter `labevents.csv.gz` in batches and write as a parquet was 80 seconds and the resulting parquet file was 101 MB.

# Description: Below is an additional way to read in the compressed CSV of labevents. Similar to the base python function `with open`, arrow has a similar function called `open_csv`. Unlike the previous arrow method, the file is streamed into the python environment instead of all at once. This allows for the file to be batched. Batches consume less memory than reading in the whole data set at once; Therefore, for systems with less resources, those systems are able to open the file. This is done by reading in each batch, then filtering that batch and appending it to a python object. The final object is a arrow table. The arrow table is converted to a pandas data frame.

# In[75]:


get_ipython().run_cell_magic('time', '', "\n# Path to labevents\nin_path = './mimic/hosp/labevents.csv.gz'\n\n# initialize filtered object\nfiltered = None\n# open csv as reader\nwith csv.open_csv(in_path) as reader:\n  # for each partition in the reader.\n  for next_chunk in reader:\n    # if the partition is empty stop loop\n    if next_chunk is None:\n      break\n    # Convert chunk/partition into a table\n    next_table = pa.Table.from_batches([next_chunk])\n    # Filter that chunk/partition by filter_table\n    next_subset = filter_table(next_table)\n    # For the first chunk, have the filtered chunk assigned to filtered.\n    if filtered is None:\n      filtered = next_subset\n    # Otherwise append that chuck to the bottom of the filtered\n    else:\n      filtered = pa.concat_tables([filtered, next_subset])\n\nfiltered_df = filtered.to_pandas()\n")


# In[76]:


print(round(sys.getsizeof(filtered_df)/10**9,3), "GB")


# In[77]:


filtered_df.head(10)


# ### (G). Convert `labevents.csv.gz` to Parquet format and ingest/select/filter
# 
# Re-write the csv.gz file `labevents.csv.gz` in the binary Parquet format using the code below. Add comments to the code. How large is the Parquet file(s)?

# ### Answer 3G
# 
# The resulting parquet file from the below code was 2.0 gb which is 5% more than the csv.gz counterpart. This is still a good result because the parquet file is not compressed and is faster to load than csv.gz because it does not have to be decompressed when using some libraries.
# 
# To filter the parquet using pyarrow, the runtime was 5.32 seconds and the memory usage was 0.795 GB. Using pyarrow to filter a parquet file was 91% faster than filtering on the compressed csv file.

# #### Documentation
# 
# PyArrow was used to stream chunks from the compressed csv file, labevents, to a parquet file using `open_csv` to stream in the data and `ParquetWriter` to steam the chucks into a parquet file format. Similar to the base python function with open, arrow has a similar function called `open_csv`. The file is streamed into the python environment instead of all at once. This allows for the file to be batched. Batches consume less memory than reading in the whole data set at once; Therefore, for systems with less resources, those systems are able to open the file. To stream the batches into a parquet file format, the object, `writer`, is initialized to using `ParquetWriter` which is a class that allows for iterative building of a parquet file. Each batch is added to this oject class and once the file is created, the files is closed.

# In[90]:


get_ipython().run_cell_magic('time', '', "# Set the path to the symbolic link directory and labevents\nin_path = './mimic/hosp/labevents.csv.gz'\n# Set the save parquet path\nout_path = './labevents.parquet'\n\n# Initialize the object.\nwriter = None\n# Stream the compressed csv to the python object reader\nwith csv.open_csv(in_path) as reader:\n    # Loop through each chuck from the streamed object, reader.\n    for next_chunk in reader:\n        # stop if the chunck is the empty which is true when at end of stream\n        if next_chunk is None:\n            break\n        # If writer is empty,\n        # initialize object, writer, for incrementally building a Parquet file.\n        if writer is None:\n            writer = pq.ParquetWriter(out_path, next_chunk.schema)\n        # For each chunk, add the chunk to next_table\n        next_table = pa.Table.from_batches([next_chunk])\n        # For each chunk, write write table as parquet using the\n        # object, writer which was initialized when it was first empty.\n        writer.write_table(next_table)\nwriter.close() # Close the streaming to prevent errors and improve security.\n")


# In[91]:


get_ipython().system('ls -goh ./labevents.parquet')


# In[92]:


get_ipython().system('ls -goh ./mimic/hosp/labevents.csv.gz')


# In[93]:


# Initial is labevents.csv.gz
# Final is labevents.parquet
percent_diff(
    i_mem = 1.9,
    f_mem = 2.0
)


# In[124]:


get_ipython().run_cell_magic('time', '', 'df_pa_lab_ftr = (\n  pq.read_table(\'./labevents.parquet\') # read labevents.parquet\n  .select(lab_col) # select columns\n  .filter(pc.is_in(pc.field("itemid"), pa.array(lab_itemid))) # Filter by itemid\n  .sort_by([("subject_id", "ascending"),("charttime", "ascending")])\n  .to_pandas() # Convert to pd.dataframe\n)\n')


# In[125]:


print(round(sys.getsizeof(df_pa_lab_ftr)/10**9,3), "GB")


# In[126]:


df_pa_lab_ftr.head(10)


# In[127]:


percent_diff(
    i_time= 59.9,
    i_mem= 0.795,
    f_time= 5.32,
    f_mem= 0.795
)


# ### (H). DuckDB
# 
# Let's use `duckdb` package in Python to use the DuckDB interface. In Python, DuckDB can interact smoothly with `pandas` and `pyarrow`. I recommend reading: 
# 
# - https://duckdb.org/2021/05/14/sql-on-pandas.html
# - https://duckdb.org/docs/guides/python/sql_on_arrow.html
# 
# In Python, you will mostly use SQL commands to work with DuckDB. Check out the [data ingestion API](https://duckdb.org/docs/api/python/data_ingestion).
# 
# 
# Ingest the Parquet file, select columns, and filter rows as in (F). How long does the ingest+select+filter process take? Please make sure to call `.df()` method to have the final result as a `pandas` `DataFrame`. Display the number of rows and the first 10 rows of the result dataframe and make sure they match those in (C). 
# 
# __This should be significantly faster than the results before (but not including) Part (F).__ 
# _Hint_: It could be a single SQL command.
# 

# ### Answer 3H
# 
# #### PyArrow
# 
# The runtime to read, select, and filter the parquet was 5.32 seconds and the resulting data frame memory usage was 0.795 GB.
# 
# #### DuckDB
# 
# The runtime to read, select, and filter the parquet was 7.89 seconds and the resulting data frame memory usage was 0.795 GB.
# 
# #### Polars
# 
# The runtime to read, select, and filter the parquet was 1.95 seconds and the resulting data frame memory usage was 0.795 GB.
# 
# final runtime was 75% faster than initial.
# final df memory usage was the same as initial.
# 
# #### Summary
# 
# The runtime to read, select, and filter the parquet in polars was __75% faster__ than DuckDB and __63% faster__ than PyArrow, but resulted in the same memory usage as DuckDB.

# #### Documentation
# 
# DuckDB was used to read, select columns and filter by item ID. A database was created in memory. The programming language SQL was used to select columns, read in the parquet, and filter by itemid. Using this method however resulted in a dataset with a different order than the bash method. to match the bash method, an order by statement was added to the SQL query. After adding the orderby, the output matched the bash command.
#  
# PyArrow was used to read, select columns, and filter by item ID. Unlike the previous PyArrow functions, `read_csv`, was used which reads in all of the file at once instead of batches. It is less memory efficient but is faster than reading in batches. In order to use the filter function, additional arrow functions were needed such as `is_in` and `field` to specify which column to filter by and format the input in a compatible way.
# 
# Polars was used to read, select columns, and filter by item ID. Unlike the other methods, Polars didn't read in the data at all and only created a link because the parquet file was lazy loaded. before reading in the data, queries were supplied to polars and polars organized the queries in the most efficient order and performed them and read in the parquet file when it was collected.

# In[96]:


get_ipython().run_cell_magic('time', '', '# open server-less database and close when done\nwith duckdb.connect(database=\':memory:\') as con:\n  # SQL query\n  df_db_lab_ftr = con.execute(\n    """\n    SELECT subject_id, itemid, charttime, valuenum\n    FROM \'./labevents.parquet\' \n    WHERE itemid IN (50912, 50971, 50983, 50902, 50882, 51221, 51301, 50931)\n    ORDER BY subject_id, charttime ASC\n    """\n  ).df() # Output as pandas data frame\n')


# In[97]:


print(round(sys.getsizeof(df_db_lab_ftr)/10**9,3), "GB")


# In[98]:


df_db_lab_ftr.head(10)


# In[120]:


get_ipython().run_cell_magic('time', '', "df_pl_lab_ftr = (\n  pl.scan_parquet('./labevents.parquet') # lazy read\n  .select(lab_col) # select columns\n  .filter(pl.col('itemid').is_in(lab_itemid)) # filter rows\n  .sort(['subject_id','charttime'])\n  .collect() # collect pl.lazyframe to pl.dataframe\n  .to_pandas() # convert to pd.dataframe\n)\n")


# In[121]:


print(round(sys.getsizeof(df_pl_lab_ftr)/10**9,3), "GB")


# In[123]:


df_pl_lab_ftr.head(10)


# In[122]:


# Initial: DuckDB read, select, and filter
# Final: Polars read, select, and filter
percent_diff(
  i_time = 7.89,
  i_mem = 0.795,
  f_time = 1.95,
  f_mem = 0.795
)


# In[128]:


# Initial: Pyarrow read, select, and filter
# Final: Polars read, select, and filter
percent_diff(
  i_time = 5.32,
  i_mem = 0.795,
  f_time = 1.95,
  f_mem = 0.795
)


# ### (I). Comparison (added 5/6)
# Compare your results with those from Homework 2 of BIOSTAT 203B. 

# The biggest comparison between Homework 2 from Biostat 203B and this assignment is between R and python and the packages and libraries that were used in each. For example, reading in the dataset, labevents, with tidyverse did not work, my system crashed and it took over 5 minutes. Similarly, when I tried to read in only select columns of labevents with tidyverse, it also crashed and it took over 5 minutes. Bash was needed to decompress the labevents file for pyarrow to work. Pyarrow was able to read in the decompressed csv. The total time to decompress and read in the data was 119 seconds, significantly more than the 74 seconds to read in the data with pyarrow. DuckDB in R also needed a decompressed csv. Therefore, it's faster to use python and pyarrow, duckdb, polars, and pandas than any of the packages used in R in the 203B HW2.

# ## Problem 4. Ingest and filter `chartevents.csv.gz`
# 
# [`chartevents.csv.gz`](https://mimic.mit.edu/docs/iv/modules/icu/chartevents/) contains all the charted data available for a patient. During their ICU stay, the primary repository of a patient’s information is their electronic chart. The `itemid` variable indicates a single measurement type in the database. The `value` variable is the value measured for `itemid`. The first 10 lines of `chartevents.csv.gz` are
# 

# #### Documentation
# 
# The bash commands below output the first 10 rows of chartevents and d_items.

# In[100]:


get_ipython().system('zcat < ./mimic/icu/chartevents.csv.gz | head -10')


# [`d_items.csv.gz`](https://mimic.mit.edu/docs/iv/modules/icu/d_items/) is the dictionary for the `itemid` in `chartevents.csv.gz`.

# In[101]:


get_ipython().system('zcat < ./mimic/icu/d_items.csv.gz | head -10')


# Again, we are interested in the vitals for ICU patients: heart rate (220045), mean non-invasive blood pressure (220181), systolic non-invasive blood pressure (220179), body temperature in Fahrenheit (223761), and respiratory rate (220210). Retrieve a subset of `chartevents.csv.gz` only containing these items, using the favorite method you learnt in Problem 3. 
# 
# Document the steps and show your code. Display the number of rows and the first 10 rows of the result `DataFrame`.

# ### Answer 4
# 
# #### DuckDB
# 
# DuckDB was chosen over pandas, Apache Arrow, and polars becuase it was the fastest and most reliable at reading in a large, compressed csv. The runtime to read, select, and filter `chartevents.csv.gz` was 1min 31s and the resulting data frame was .
# 
# Polars was unable to read the compressed csv.

# In[129]:


get_ipython().run_cell_magic('time', '', '# open server-less database in memory and close when done.\nwith duckdb.connect(database=\':memory:\') as con:\n    df_db_chrt_ftr = con.execute( # SQL query\n      """\n      SELECT *\n      FROM \'./mimic/icu/chartevents.csv.gz\' \n      WHERE itemid IN (220045, 220181, 220179, 223761, 220210)\n      """\n    ).df() # Output as pandas dataframe\n')


# In[130]:


df_db_chrt_ftr.shape


# In[131]:


print(round(sys.getsizeof(df_db_chrt_ftr)/10**9,3), "GB")


# In[132]:


df_db_chrt_ftr.head(10)


# ### Summary and Conclusion
# 
# Use DuckDB to read and query on large, compressed data files. Use polars to lazily read and query parquet files. Use pandas for compatibility and use PyArrow to read in batches for larger than memory data.
