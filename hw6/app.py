from dash import Dash, dcc, html, Output, Input
import plotly.express as px
import duckdb

# Init message database
message_db = None

def get_message_db():
    global message_db # get current database from global
    if message_db: # If db not empty
        return message_db # return the current db
    else: # If db is empty
        message_db = sqlite3.connect( # create a db link
          "messages_db.sqlite", # db name
          check_same_thread=False # multi-threaded concurancy
        )
        cmd = ''
        cursor = message_db.cursor()
        cursor.execute(cmd)
        return message_db

app = Dash(__name__)

radio_b = dcc.RadioItems(df.columns, 'time')
my_graph = dcc.Graph()
app.layout = html.Div([radio_b, my_graph])

if __name__ == '__main__':
    app.run(debug=True)

## UI
# A text box for submitting a message.
# A text box for submitting the name of the user.

# A "submit" button.

## Backend

# Write 2 Python functions for database management
# get_message_db() should handle creating the database of messages.
    # Check whether there is a database called message_db
    # If not, then connect to that database and assign it to the global variable message_db
    # To do this last step, write a line like message_db = sqlite3.connect("messages_db.sqlite")
    # Check whether a table called messages exists in message_db, 
    # if not create it, {CREATE TABLE IF NOT EXISTS}
message_db = None
def get_message_db():
    pass
    # write some helpful comments here
    global message_db
    if message_db:
        return message_db
    else:
        message_db = sqlite3.connect("messages_db.sqlite", check_same_thread=False)
        cmd = '' # replace this with your SQL query
        cursor = message_db.cursor()
        cursor.execute(cmd)
        return message_db
    
# The function insert_message(handle, message)
    # handle inserting a user message into the database of messages.
    # insert the message into the message database. 
    # Remember that you’ll need to provide the handle and the message itself. 
    # You’ll need to write a SQL command to perform the insertion 
    # SQL commands, it is necessary to run db.commit() 
    # after inserting a row into db in order to ensure that your row insertion has been saved.
    # write a callback function submit() to update the components.
    # Extract the handle and the message from the components
    # You might want to use the keyword argment prevent_initial_call

def insert_message(handle, message):
    pass