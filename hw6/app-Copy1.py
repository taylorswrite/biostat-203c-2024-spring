from dash import Dash, clientside_callback, dcc, html, Output, Input, State
import dash_bootstrap_components as dbc
import time
import sqlite3

# At startup init message_db
message_db = None

# Create a messaging database if it doesn't exist
def get_message_db():
    global message_db
    if message_db:
        message_db = sqlite3.connect(
            "messages_db.sqlite", 
            check_same_thread=False
        )
        return message_db
    else:
        message_db = sqlite3.connect(
            "messages_db.sqlite", 
            check_same_thread=False
        )
        cmd = (
            """
            CREATE TABLE 
            IF NOT EXISTS message_db (handle TEXT, message TEXT)
            """
        )
        cursor = message_db.cursor()
        cursor.execute(cmd)
        return message_db

def insert_message(username, message): # triggered on submit
    conn = get_message_db() # open database within function
    cursor = conn.cursor() # open an execution 
    cursor.execute( # insert username and message into database table
        "INSERT INTO message_db (handle, message) VALUES (?, ?)", 
        (username, message)
    )
    conn.commit() # commit the transaction to database
    conn.close() # close the database

def random_messages(n):
    conn = get_message_db() # open database within function
    cursor = conn.cursor() # open an execution 
    row = cursor.execute( # Randomly message from database(prompt)
        f"SELECT * FROM message_db ORDER BY RANDOM() LIMIT {n};"
    )
    rows = cursor.fetchall() # aggregate all 
    username = list()
    messages = list()
    for row in rows: # iterate over rows
        usernames.append(row[0]) # username column
        messages.append(row[1]) # message column
    conn.close() # close the database
    return usernames, messages # return username and message

def delete_messages():
    conn = get_message_db() # open database within function
    cursor = conn.cursor() # open an execution 
    row = cursor.execute( # Delete all rows in database
        f"DELETE FROM message_db"
    )
    conn.commit() # commit the transaction to database
    conn.close() # close the database

# Initialize the app and set styling
app = Dash(
    __name__, 
    external_stylesheets=[
        dbc.themes.BOOTSTRAP, 
        dbc.icons.FONT_AWESOME
    ]
)


### UI ELEMENTS

# Headers
header = html.H1("teamwork messenger", #style={
    # "font-family": "Cosmic Sans",
    # 'background': '''linear-gradient(
    #     45deg, 
    #     #ee0022, 
    #     #7755ff, 
    #     #dd1199, 
    #     #7755ff, 
    #     #ee0022
    # )''',
    # "-webkit-background-clip": "text",
    # "-webkit-text-fill-color": "transparent",
    # "-webkit-text-stroke-width": "0.5px",
    # "-webkit-text-stroke-color": "black"}
)
feed_header = html.H4("Message Feed")
user_header = html.H4("Send a Message")

# Text Input Boxes (Username and User Message)
input_groups = html.Div([
        dbc.InputGroup([
            dbc.InputGroupText("@"), 
            dbc.Input(id='username', placeholder="Username")],
        ),
        dbc.InputGroup([
            dbc.Textarea(
                id='message', 
                style={'height': 362},
                placeholder="Message"
        )
    ])
])

# Message Box
message_box = html.Div(
    id='all_messages', 
    style={
        'height': '400px', 
        'overflow-y': 'scroll',
        'scroll-behavior': 'smooth',
        'border': '0.2px solid grey',
        'border-radius': '5px',
        'padding': '20px 10px', 
        'display': 'flex', 
        'flex-direction': 'column-reverse',
         'background-clip': 'padding-box',
        
    }
)

# Buttons
submit_button = dbc.Button(
    "Send Message", 
    id='submit_button', 
    color="primary"
)
update_button = dbc.Button(
    "Update Messages", 
    id='update_button',
    color="secondary"
)
delete_button = dbc.Button(
    "Delete Messages", 
    id='delete_button', 
    color="danger"
)

# Alert message
sent_alert = dbc.Alert(
    "Message Sent!",
    id="sent",
    dismissable=True,
    is_open=False,
    duration=3000,
    color="success",
)

update_alert = dbc.Alert(
    "All Messages updated.",
    id="updated",
    dismissable=True,
    is_open=False,
    duration=3000,
    color="secondary",
)

bad_message_alert = dbc.Alert(
    "A Username and Message is needed.",
    id="error",
    dismissable=True,
    is_open=False,
    duration=3000,
    color="danger",
)

deleted_alert = dbc.Alert(
    "All messages deleted.",
    id="delete",
    dismissable=True,
    is_open=False,
    duration=3000,
    color="danger",
)

# Dark and Light Mode
color_mode_switch =  html.Span(
    [
        dbc.Label(className="fa fa-moon", html_for="switch"),
        dbc.Switch( id="switch", value=True, className="d-inline-block ms-1", persistence=True),
        dbc.Label(className="fa fa-sun", html_for="switch")
    ]
)

### UI Layout
# Use Dash Bootstrap (dbc) to configure the app layout
app.layout = dbc.Container([
    # Header Row 
    dbc.Row([ # 1 of 3 main rows
        dbc.Col([header], width=6), # column within row
        dbc.Col([color_mode_switch], style={'text-align': 'right'}, width=6) # column within row
    ]),
    
    # Message Row
    dbc.Row([ # 2 of 3 main rows
        # User Input
        dbc.Col([ # 1 of 2 columns within row
            dbc.Row([user_header]), # header 
            dbc.Row([input_groups]) # username and user message
        ]),
        # Message Database
        dbc.Col([ # 2 of 2 columns within row
            dbc.Row([feed_header]), # header
            dbc.Row([message_box]), # all messages
        ])
    ]),
    
    # Button Row
    dbc.Row([ # 3 of 3 main rows
        dbc.Col([ # 1 of 2 columns
            dbc.Row([ # row within column
                dbc.Col([submit_button]), # Added button in column for formating
            ]),
            dbc.Row([sent_alert]), # show sent notification below button
            dbc.Row([bad_message_alert]) # show error notification
        ]),
        dbc.Col([ # 2 of 2 columns
            dbc.Row([ # row within column
                dbc.Col([update_button,delete_button]), # in col for formating
            ]),
            dbc.Row([update_alert]),
            dbc.Row([deleted_alert]) # show updated notification below button
        ])
    ])
])

###

### Server and Callbacks

# Callback is ran in user's browser
clientside_callback(
    # Javascript Function to change the app theme
    """
    (switchOn) => { //check if switch on/True
       document.documentElement.setAttribute( //Set theme
           "data-bs-theme", //Parent theme
           switchOn ? "light" : "dark" //If true set to light else dark
       ); 
       return window.dash_clientside.no_update //only update theming
    }
    """,
    # Output of javascript. Changes theme
    Output("switch", "id"),
    # Input is the switch position
    Input("switch", "value"),
)

# Callback function to send message
@app.callback( # funciton decorator wrapper for dash app
    Output('sent', 'is_open'), # Output message sent notification
    Output('error', 'is_open'), # Output message error notification
    Input('submit_button', 'n_clicks'), # Trigger callback on submit button
    State('username', 'value'), # username contents
    State('message', 'value'), # message contents
    prevent_initial_call=True # Don't perform callback on app startup
)
def submit(submit_button, username, message): # triggered on submit
    if username and message: # If username and message aren't empty
        insert_message(username, message)
        return True, False # show sent notification
    else: # if username or message are empty
        return False, True # show error notification

# Function callback to update database
@app.callback( # function decorator
    Output('all_messages', 'children'), # Output html text
    Output('updated', 'is_open'), # Updated notification
    Input('update_button', 'n_clicks'), # Trigger is update button
    prevent_initial_call=True # Don't Run on startup
)
def view(n_clicks): # Triggered by update button
    usernames, messages = random_messages(8) # show the past 8 messages
    all_rand_messages = list()
    for i, username in enumerate(usernames): # iterate over message dict
        message = messages[i]
        all_rand_messages.append( # append html
            html.Div([
                html.Div(username, style={'font-weight': 'bold'}),
                html.Div(message)
            ], style={'margin-bottom': '10px'})
        )
    return all_rand_messages, True

@app.callback(
    Output('delete', 'is_open'),
    Input('delete_button', 'n_clicks'),
    prevent_initial_call=True
)
def delete(n_clicks):
    delete_messages()
    return True


# Run the app on port 8051
if __name__ == '__main__':
    app.run_server(debug=True, port=8051)
