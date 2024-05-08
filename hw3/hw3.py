#!/usr/bin/env python
# coding: utf-8

# # Homework 3
# ### Name: William Martinez
# ### Collaborators: None
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

# In[3]:


# Return positions from n random steps
rw2 = lambda n: np.random.choice([-1, 1], size=n, replace=True).cumsum()

rw2(10)


# ### (C).
# 
# Use the `%timeit` magic macro to compare the runtime of `rw()` and `rw2()`. Test how each function does in computing a random walk of length `n = 10000`. 

# ### Answer 1C

# In[4]:


get_ipython().run_cell_magic('timeit', '', 'rw(10_000)\n')


# In[5]:


get_ipython().run_cell_magic('timeit', '', 'rw2(10_000)\n')


# ### (D). 
# 
# Write a few sentences in which you comment on (a) the performance of each function and (b) the ease of writing and reading each function. 

# ### ANSWER 1D
# 
# The function, `rw2()`, was 82% faster than the function, `rw()`. Built-in python list operations were used in `rw()` and numpy array operations were used in `rw2()`. The average run-times for `rw()` and `rw2()` were 61.1 µs and 362 µs, respectively. The numpy array function, `rw2()`, was easier to write, occupying only one line, and was more readable than the built-in python list operations in `rw()`.

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

# In[6]:


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
  positions = (
    np.random.choice(
      [-1, 1], # -1 for tails and 1 for heads.
      size = (n * d), # The number of elements in array.
      replace = True) # Reuse heads and tails.
    .reshape(n, d) # Reshape into n rows and d columns.
    .cumsum(axis=0) # Columnar Cumulative Sum 
  )
  return positions

rw_d(5, 3)


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

# In[7]:


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

# In[8]:


# Hosp Files
get_ipython().system('ls -goh ./mimic/hosp/')


# In[9]:


# ICU Files
get_ipython().system('ls -goh ./mimic/icu/')


# ### (A). Speed, memory, and data types
# 
# Standard way to read a CSV file would be using the `read_csv` function of the `pandas` package. Let us check the speed of reading a moderate-sized compressed csv file, `admissions.csv.gz`. How much memory does the resulting data frame use?
# 
# _Note:_ If you start a cell with `%%time`, the runtime will be measured. 

# In[10]:


def percent_diff(i_time=None, f_time=None, i_mem=None, f_mem=None):
  """
  Print Pandas and Polars Comparison Metrics
  ---
  Args:
    i_time: A positive float. The runtime of a initial cell in s.
    f_time: A positive float. The runtime of a final cell in s.
  Optional Args:
    i_mem: A positive float. Initial dataframe object memory usage in GB.
    f_mem: A positive float. Final dataframe object memory usage in GB.
  Return:
    None
  """
  if (i_time is not None) and (f_time is not None): # Check if empty
    time_diff = round((f_time - i_time) / i_time * 100,3) # percent diff.
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
    mem_diff = round((f_mem - i_mem) / i_mem * 100,3) # percent diff
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
# #### Pandas
# 
# The runtime to ingest `admissions.csv.gz` using pandas was 884 ms and the resulting data frame memory usage was 0.368 GB.
# 
# #### Polars
# 
# The runtime to ingest `admissions.csv.gz` using pandas was 214 ms and the resulting data frame memory usage was 4.8e-08 GB.
# 
# #### Summary
# 
# Polars consumed less resources than pandas. The Polars runtime was __76% faster__ and the Polars data frame object memory usage was __100% less__ than Pandas.

# In[11]:


get_ipython().run_cell_magic('time', '', '# Read admissions using Pandas\ndf_pd_adm = pd.read_csv("./mimic/hosp/admissions.csv.gz")\n')


# In[12]:


print(round(sys.getsizeof(df_pd_adm)/10**9,3), "GB")


# In[13]:


get_ipython().run_cell_magic('time', '', '# Read admissions using Polars\ndf_pl_adm = pl.read_csv("./mimic/hosp/admissions.csv.gz")\n')


# In[14]:


print(sys.getsizeof(df_pl_adm)/10**9, "GB")


# In[15]:


# Initial: Pandas reading admissions.csv.gz
# Final: Polars reading admissions.csv.gz
percent_diff(i_time=0.878, i_mem=0.368, f_time=0.214, f_mem=4.8e-08)


# In[16]:


del df_pd_adm
del df_pl_adm


# ### (B). User-supplied data types
# 
# Re-ingest `admissions.csv.gz` by indicating appropriate column data types in [`pd.read_csv`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html). Does the run time change? How much memory does the result dataframe use? (Hint: `dtype` and `parse_dates` arguments in `pd.read_csv`.)

# ### Answer 2B
# 
# #### Pandas (Categorical) 
# 
# Categorical variables were assigned as type `categorical`. Dates were assigned as date. The runtime to ingest `admissions.csv.gz` using pandas and declaring data types was 3.69 seconds and the resulting data frame memory usage was 0.029 GB. Compared to pandas reading without declaring data types (1A), the pandas runtime with declared data types was __331% slower__ but the data frame memory usage was __92% less__.
# 
# #### Pandas (String)
# 
# Categorical variables were assigned as type `string`. Dates were assigned as date. The runtime to ingest `admissions.csv.gz` using pandas and declaring data types was 3.76 seconds and the resulting data frame memory usage was 0.277 GB. Compared to pandas reading without declaring data types (1A), the pandas runtime with declared data types was __328% slower__ but the data frame memory usage was __25% less__.
# 
# #### Polars (Categorical)
# 
# Categorical variables were assigned as type `string`. Dates were assigned as date. The runtime to ingest `admissions.csv.gz` using polar and declaring data types was 0.207 seconds and the resulting data frame memory usage was 0.029 GB. Compared to pandas reading without declaring data types (1A), the pandas runtime with declared data types was __328% slower__ but the data frame memory usage was __604,165x more__.
# 
# 
# #### Summary
# For pandas, declaring variables increased the runtime but decreased the the size of the resulting data frame. Additionally, for pandas the size of the resulting data frame was less when categorical variables were assigned the type, `categorical`. Polars, had a slower runtime and larger resulting data frame; however, compared to pandas, the it was faster and the resulting data frame was smaller. Of the methods tested, polars was best because it accomplished the same task as pandas while using less resources.

# In[17]:


get_ipython().run_cell_magic('time', '', '# Pandas: Assigning data types while reading (category)\ndf_pd_cat_adm = pd.read_csv(\n  "./mimic/hosp/admissions.csv.gz",\n  # assign data types\n  dtype={\n    \'subject_id\': \'int64\',\n    \'hadm_id\': \'int64\',\n    \'admission_type\': \'category\',\n    \'admit_provider_id\': \'category\',\n    \'admission_location\': \'category\',\n    \'discharge_location\': \'category\',\n    \'insurance\': \'category\',\n    \'language\': \'category\',\n    \'marital_status\': \'category\',\n    \'race\': \'category\',\n    \'hospital_expire_flag\': \'category\'\n  },\n  # assign date types\n  parse_dates=[\n    \'admittime\',\n    \'dischtime\',\n    \'deathtime\',\n    \'edregtime\',\n    \'admittime\',\n    \'edouttime\',\n    \'deathtime\',\n  ]\n)\n')


# In[18]:


print(round(sys.getsizeof(df_pd_cat_adm)/10**9,3), "GB")


# In[19]:


# Initial = Pandas without declared data types
# Final = Pandas with declared data types (categorical)
percent_diff(i_time=0.878, i_mem=0.368, f_time=3.78, f_mem=0.029)


# In[20]:


get_ipython().run_cell_magic('time', '', '# Pandas: Assigning data types while reading (string)\ndf_pd_str_adm = (\n  pd.read_csv(\n    "./mimic/hosp/admissions.csv.gz",\n    # assign data types\n    dtype={\n      \'subject_id\': \'int64\',\n      \'hadm_id\': \'int64\',\n      \'admission_type\': \'string\',\n      \'admit_provider_id\': \'string\',\n      \'admission_location\': \'string\',\n      \'discharge_location\': \'string\',\n      \'insurance\': \'string\',\n      \'language\': \'string\',\n      \'marital_status\': \'string\',\n      \'race\': \'string\',\n      \'hospital_expire_flag\': \'string\'\n    },\n    # assign date types\n    parse_dates=[\n      \'admittime\',\n      \'dischtime\',\n      \'deathtime\',\n      \'edregtime\',\n      \'admittime\',\n      \'edouttime\',\n      \'deathtime\',])\n)\n')


# In[21]:


print(round(sys.getsizeof(df_pd_str_adm)/10**9, 3), "GB")


# In[22]:


# Initial = Pandas without declared data types
# Final = Pandas with declared data types (string)
percent_diff(i_time=0.878, i_mem=0.368, f_time=3.76, f_mem=0.277)


# In[23]:


get_ipython().run_cell_magic('time', '', '# Polars: Assigning data types while reading (category)\ndf_pl_cat_adm = (\n  pl.read_csv(\n    "./mimic/hosp/admissions.csv.gz", # read file\n    # assign data types\n    dtypes={ \n      \'subject_id\': pl.Int64,\n      \'hadm_id\': pl.Int64,\n      \'admission_type\': pl.Categorical,\n      \'admit_provider_id\': pl.Categorical,\n      \'admission_location\': pl.Categorical,\n      \'discharge_location\': pl.Categorical,\n      \'insurance\': pl.Categorical,\n      \'language\': pl.Categorical,\n      \'marital_status\': pl.Categorical,\n      \'race\': pl.Categorical,\n      \'hospital_expire_flag\': pl.Categorical}, \n    try_parse_dates=True) # convert columns to date.\n  .to_pandas() # convert to pd.dataframe\n)\n')


# In[24]:


print(round(sys.getsizeof(df_pl_cat_adm)/10**9,3), "GB")


# In[25]:


# Initial = Polars without declared data types(initial) 
# Final = Polars with declared data types
percent_diff(i_time=0.214, i_mem=4.8e-08, f_time=0.199, f_mem=0.029)


# In[26]:


del df_pd_cat_adm
del df_pd_str_adm
del df_pl_cat_adm


# ## Problem 3. Ingest big data files
# 
# 
# Let us focus on a bigger file, `labevents.csv.gz`, which is about 125x bigger than `admissions.csv.gz`.

# In[27]:


# Size of labevents as a csv.gz
get_ipython().system('ls -goh ./mimic/hosp/labevents.csv.gz')


# Display the first 10 lines of this file.

# In[28]:


# First 10 results of labevents
get_ipython().system('zcat < ./mimic/hosp/labevents.csv.gz | head -10')


# ### (A). Ingest `labevents.csv.gz` by `pd.read_csv`
# 
# Try to ingest `labevents.csv.gz` using `pd.read_csv`. What happens? If it takes more than 5 minutes on your computer, then abort the program and report your findings. 

# ### Answer 3A
# 
# #### Pandas
# 
# Pandas was able to read `labevents.csv.gz` within 5 minutes. The runtime to read the file was 3 minutes and 17 seconds and the resulting data frame memory usage was 61 GB.
# 
# #### Polars
# 
# Polars was unable to read `labevents.csv.gz` within 5 minutes. From problem, 4A, it was determined that polars doesn't read compressed csv files and convert them to pandas data frames efficiently for large compressed csv files. The kernel will crash.
# 
# #### Summary
# 
# Use Pandas to read in large (but less than memory) compressed files because polars is not efficient at reading large compressed files.

# In[29]:


get_ipython().run_cell_magic('time', '', '#Pandas read labevents.csv.gz\ndf_pd_lab = pd.read_csv("./mimic/hosp/labevents.csv.gz")\n')


# In[30]:


print(round(sys.getsizeof(df_pd_lab)/10**9,3), "GB")


# In[31]:


del df_pd_lab


# In[32]:


get_ipython().run_cell_magic('time', '', '#Polars read labevents.csv.gz Failed\n# df_pl_lab = pl.read_csv("./mimic/hosp/labevents.csv.gz").to_pandas()\n')


# In[33]:


# print(round(sys.getsizeof(df_pl_lab)/10**9,3), "GB")


# In[34]:


# del df_pl_lab


# ### (B). Ingest selected columns of `labevents.csv.gz` by `pd.read_csv`
# 
# Try to ingest only columns `subject_id`, `itemid`, `charttime`, and `valuenum` in `labevents.csv.gz` using `pd.read_csv`.  Does this solve the ingestion issue? (Hint: `usecols` argument in `pd.read_csv`.)

# ### Answer 3B
# 
# #### Pandas
# 
# The runtime to read selected columns of the file was 1 minute and 12 seconds and the resulting data frame memory usage was 11.817 GB. The runtime for reading selected columns in pandas was __63% faster__ and the resulting data frame memory usage was __81% smaller__ when compared the pandas reading all columns.
# 
# #### Polars
# 
# The runtime to read selected columns of the file was 44.8 seconds and the resulting data frame memory usage was 11.817 GB. The runtime for reading selected columns in polars was __77% faster__ and the resulting data frame memory usage was __81% smaller__ when compared the pandas reading all columns.
# 
# #### Summary
# 
# For large, compressed files, pandas is the preferred method. Pandas increased compatibility, reliability, and readability outweigh the marginal performance increases provided by polars when reading large, compressed files.

# In[35]:


get_ipython().run_cell_magic('time', '', '# Pandas Read select labevent columns\ndf_pd_lab = pd.read_csv("./mimic/hosp/labevents.csv.gz", usecols = lab_col)\n')


# In[36]:


print(round(sys.getsizeof(df_pd_lab)/10**9,3), "GB")


# In[37]:


# Initial: Pandas reading all columns
# Final: Pandas reading selected columns
percent_diff(
  i_time= (3 * 60 + 17),
  i_mem= 61.002,
  f_time= (1 * 60 + 12),
  f_mem= 11.817
)


# In[38]:


del df_pd_lab


# In[39]:


get_ipython().run_cell_magic('time', '', '# Polars Read select labevent columns\ndf_pl_lab = (\n  pl.read_csv("./mimic/hosp/labevents.csv.gz", # read file\n    columns = lab_col) # Select columns\n    .to_pandas() # Convert pl.dataframe to pd.dataframe\n)\n')


# In[40]:


print(round(sys.getsizeof(df_pl_lab)/10**9,3), "GB")


# In[41]:


# Initial: Pandas reading all columns
# Final: Polars reading specific columns
percent_diff(
  i_time= (3 * 60 + 17),
  i_mem= 61.002,
  f_time= (44.8),
  f_mem= 11.817
)


# In[42]:


del df_pl_lab


# ### (C). Ingest subset of `labevents.csv.gz`
# 
# Back in BIOSTAT 203B, our first strategy to handle this big data file was to make a subset of the `labevents` data.  Read the [MIMIC documentation](https://mimic.mit.edu/docs/iv/modules/hosp/labevents/) for the content in data file `labevents.csv.gz`.
# 
# As before, we will only be interested in the following lab items: creatinine (50912), potassium (50971), sodium (50983), chloride (50902), bicarbonate (50882), hematocrit (51221), white blood cell count (51301), and glucose (50931) and the following columns: `subject_id`, `itemid`, `charttime`, `valuenum`. 
# 
# Rerun the Bash command to extract these columns and rows from `labevents.csv.gz` and save the result to a new file `labevents_filtered.csv.gz` in the current working directory (Q2.3 of HW2). How long does it take?
# 
# Display the first 10 lines of the new file `labevents_filtered.csv.gz`. How many lines are in this new file? How long does it take `pd.read_csv()` to ingest `labevents_filtered.csv.gz`?
# 

# ### Answer 3C
# 
# The runtime to select columns and filter `labevents` using Bash was 5 minutes and 34 seconds and the runtime to ingest `labevents_filtered` was 7 seconds. The number of rows in the resulting dataset was 24,855,909.

# In[43]:


get_ipython().run_cell_magic('bash', '', '# Read labevents, select columns, and filter rows by itemid\ntime(zcat < ./mimic/hosp/labevents.csv.gz | \\\n  awk -F, \\\n  \'BEGIN {OFS = ","} {if (NR == 1 || $5 == 50912 || $5 == 50971 ||\n    $5 == 50983 || $5 == 50902 || $5 == 50882 || $5 == 51221 || \n    $5 == 51301 || $5 == 50931) {print $2, $5, $7, $10}}\' | \\\n  gzip > labevents_filtered.csv.gz)\necho "complete"\n')


# In[44]:


get_ipython().run_cell_magic('time', '', '# Pandas read labevents_filtered.csv.gz\ndf_pd_lab_filter = pd.read_csv("./labevents_filtered.csv.gz")\n')


# In[45]:


df_pd_lab_filter.head(10)


# In[46]:


df_pd_lab_filter.shape


# In[47]:


print(round(sys.getsizeof(df_pd_lab_filter)/10**9,3), "GB")


# In[48]:


del df_pd_lab_filter


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

# ### (E). Ingest `labevents.csv.gz` by Apache Arrow
# 
# Our second strategy again is to use [Apache Arrow](https://arrow.apache.org/) for larger-than-memory data analytics. We will use the package `pyarrow`. Unlike in R, this package works with the `csv.gz` format. We don't need to decompress the data. We could just use `dplyr` verbs in R, but here, we need a different set of commands. The core idea behind the commands are still the same, though.
# 
# - Let's use [`pyarrow.csv.read_csv`](https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html) to ingest `labevents.csv.gz`. It creates an object of type [`pyarrow.Table`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html).
# 
# - Next, select columns using the [`select()`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.select) method. 
# 
# - As in (C), filter the rows based on the column `itemid` using the [`filter()`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.filter) method. It is strongly recommended to use [`Expression`](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Expression), in particular, the `isin()` method. 
# 
# - Finally, let's obtain the result in `pandas` `DataFrame` using the method `to_pandas()`. 
# 
# How long does the ingest+select+filter process take? Display the number of rows and the first 10 rows of the result dataframe, and make sure they match those of (C).

# ### Answer 3E
# 
# #### Apache Arrow
# 
# The runtime to read "labevents.csv.gz", select columns, and filter rows was 1 minute 16 seconds and the resulting data frame memory usage was 0.606 GB. 

# In[49]:


get_ipython().run_cell_magic('time', '', '# Apache Arrow read labevents\ndf_pa_lab_ftr = csv.read_csv(pth_hsp + "labevents.csv.gz") # read file\n# Apache Arrow filter labevents\ndf_pa_lab_ftr = (\n  df_pa_lab_ftr # pa.dataframe\n  .select(lab_col) # select columns\n  .filter(pc.is_in(pc.field("itemid"), pa.array(lab_itemid))) # filter by itemid\n  .to_pandas() # convert to pd.dataframe\n)\n')


# In[50]:


print(round(sys.getsizeof(df_pa_lab_ftr)/10**9,3), "GB")


# In[51]:


df_pa_lab_ftr.shape


# In[52]:


df_pa_lab_ftr.head(10)


# In[53]:


del df_pa_lab_ftr


# ### (F). Compress `labevents.csv.gz` to Parquet format and ingest/select/filter
# 
# 
# Re-write the csv.gz file `labevents.csv.gz` in the binary Parquet format (Hint: [`pyarrow.parquet.write_table`](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html).) How large is the Parquet file(s)? 
# 
# How long does the ingest+select+filter process of the Parquet file(s) take?  
# Display the number of rows and the first 10 rows of the result dataframe and make sure they match those in (C). 
# 
# __This should be significantly faster than all the previous results.__ 
# _Hint._ Use [`pyarrow.parquet.read_table`](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html) method with the keyword argument `columns`. Also, make sure that you are using an `Expression`. 

# ### Answer 3F
# 
# #### Apache Arrow (Convert to parquet)
# 
# The runtime to read `labevents.csv.gz` and write as a parquet was 4 minutes and 10 seconds. The resulting parquet file was 1.6 GB, 11% smaller than `labevents.csv.gz`
# 
# #### Apache Arrow (Read, Select, and filter)
# 
# The runtime to read, select, and filter the parquet was 6.6 seconds and the resulting data frame memory usage was 0.795 GB.
# 
# #### Polars (Read, Select, and filter)
# 
# The runtime to read, select, and filter the parquet was 1.5 seconds and the resulting data frame memory usage was 0.795 GB.
# 
# #### Summary
# 
# The runtime to read, select, and filter the parquet in polars was __78% faster__ than Apache Arrow. Polars shares a similar back-end to Apache Arrow, but polars is able to lazily read and perform operations on non-compressed files, making it faster than Apache Arrow.

# In[54]:


get_ipython().run_cell_magic('time', '', '# Apache Arrow Read labevents\ndf_pa_lab = csv.read_csv(pth_hsp + "labevents.csv.gz")\n# Apache Arrow write labevents as parquet\npq.write_table(df_pa_lab, \'./pa_labevents.parquet\')\n')


# In[55]:


del df_pa_lab


# In[56]:


# size of labevents parquet file
get_ipython().system('ls -goh ./pa_labevents.parquet')


# In[57]:


# Initial: labevents.csv.gz
# Final: labevents.parquet
percent_diff(
  i_mem = 1.8,
  f_mem = 1.6
)


# In[82]:


get_ipython().run_cell_magic('time', '', 'df_pa_lab_ftr = (\n  pq.read_table(\'./pa_labevents.parquet\') # read labevents.parquet\n  .select(lab_col) # select columns\n  .filter(pc.is_in(pc.field("itemid"), pa.array(lab_itemid))) # Filter by itemid\n  .sort_by([("subject_id", "ascending"),("charttime", "ascending")])\n  .to_pandas() # Convert to pd.dataframe\n)\n')


# In[59]:


print(round(sys.getsizeof(df_pa_lab_ftr)/10**9,3), "GB")


# In[84]:


df_pa_lab_ftr.head(10)


# In[60]:


del df_pa_lab_ftr


# In[81]:


get_ipython().run_cell_magic('time', '', "df_pl_lab_ftr = (\n  pl.scan_parquet('./pa_labevents.parquet') # lazy read\n  .select(lab_col) # select columns\n  .filter(pl.col('itemid').is_in(lab_itemid)) # filter rows\n  .sort(['subject_id','charttime'])\n  .collect() # collect pl.lazyframe to pl.dataframe\n  .to_pandas() # convert to pd.dataframe\n)\n")


# In[62]:


print(round(sys.getsizeof(df_pl_lab_ftr)/10**9,3), "GB")


# In[91]:


df_pl_lab_ftr.head(10)


# In[63]:


# Initial: Apache Arrow read, select, and filter
# Final: Polars read, select, and filter (lazy)
percent_diff(
  i_time = 6.57,
  i_mem = 0.606,
  f_time = 1.47,
  f_mem = 0.606
)


# ### (G). DuckDB
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

# ### Answer 3G
# 
# #### DuckDB
# 
# The runtime to read, select, and filter the parquet was 3.47 seconds and the resulting data frame memory usage was 1.893 GB.
# 
# #### Summary
# 
# The runtime to read, select, and filter the parquet in polars was __58% faster__ and the resulting data frame memory usage was __68% less__ than using DuckDB.

# In[89]:


get_ipython().run_cell_magic('time', '', '# open server-less database and close when done\nwith duckdb.connect(database=\':memory:\') as con:\n  # SQL query\n  df_db_lab_ftr = con.execute(\n    """\n    SELECT subject_id, itemid, charttime, valuenum\n    FROM \'./pl_labevents.parquet\' \n    WHERE itemid IN (50912, 50971, 50983, 50902, 50882, 51221, 51301, 50931)\n    ORDER BY subject_id, charttime ASC\n    """\n  ).df() # Output as pandas data frame\n')


# In[76]:


print(round(sys.getsizeof(df_db_lab_ftr)/10**9,3), "GB")


# In[90]:


df_db_lab_ftr.head(10)


# In[67]:


del df_db_lab_ftr


# In[68]:


# Initial: DuckDB read, select, and filter
# Final: Polars read, select, and filter
percent_diff(
  i_time = 3.47,
  i_mem = 1.893,
  f_time = 1.47,
  f_mem = 0.606
)


# ## Problem 4. Ingest and filter `chartevents.csv.gz`
# 
# [`chartevents.csv.gz`](https://mimic.mit.edu/docs/iv/modules/icu/chartevents/) contains all the charted data available for a patient. During their ICU stay, the primary repository of a patient’s information is their electronic chart. The `itemid` variable indicates a single measurement type in the database. The `value` variable is the value measured for `itemid`. The first 10 lines of `chartevents.csv.gz` are
# 

# In[69]:


get_ipython().system('zcat < ./mimic/icu/chartevents.csv.gz | head -10')


# [`d_items.csv.gz`](https://mimic.mit.edu/docs/iv/modules/icu/d_items/) is the dictionary for the `itemid` in `chartevents.csv.gz`.

# In[70]:


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

# In[71]:


get_ipython().run_cell_magic('time', '', '# open server-less database in memory and close when done.\nwith duckdb.connect(database=\':memory:\') as con:\n    df_db_chrt_ftr = con.execute( # SQL query\n      """\n      SELECT *\n      FROM \'./mimic/icu/chartevents.csv.gz\' \n      WHERE itemid IN (220045, 220181, 220179, 223761, 220210)\n      """\n    ).df() # Output as pandas dataframe\n')


# In[72]:


df_db_chrt_ftr.shape


# In[73]:


print(round(sys.getsizeof(df_db_chrt_ftr)/10**9,3), "GB")


# In[74]:


df_db_chrt_ftr.head(10)


# ### Summary and Conclusion
# 
# Use DuckDB to read and query on large, compressed data files. Use polars to lazily read and query parquet files. Use pandas for compatibility. From this analysis, there is no clear reason to use Apache Arrow over DuckDB, polars, and pandas.
