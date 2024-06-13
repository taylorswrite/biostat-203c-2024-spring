from dash import Dash, clientside_callback, dcc, html, Output, Input, State
import dash_bootstrap_components as dbc
import sqlite3

# At startup init message_db
message_db = None

# Create a messaging database if it doesn't exist
def get_message_db():
    global message_db
    if message_db:
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


# Initialize the app and set styling
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])


### UI ELEMENTS
# Headers
header = html.H1("BruinConnect")
feed_header = html.H4("Message Feed")
user_header = html.H4("Send a Message")
switch_header = html.P("Light/Dark")

# Text Input Boxes (Username and User Message)
input_groups = html.Div(
    [
        dbc.InputGroup([
            dbc.InputGroupText("@"), 
            dbc.Input(
                id='input-1-state', 
                placeholder="Username"
            )],
            className="mb-3",
        ),
        dbc.InputGroup([
            dbc.InputGroupText(
                "💬",
            ),
            dbc.Textarea(
                id='input-2-state', 
                style={'height': 250},
                placeholder="Message"
            )],
            className="mb-3",
        )
    ]
)

# Message Box
message_box = html.Div(
    id='messages-list', 
    style={
        'height': '300px', 
        'overflow-y': 'scroll', 
        'border': '1px solid #ccc', 
        'padding': '10px', 
        'display': 'flex', 
        'flex-direction': 'column-reverse'
    }
)

# Buttons
submit_button = dbc.Button(
    "Send Message", 
    id='submit-button-state', 
    color="primary", 
    className="me-1"
)
update_button = dbc.Button(
    "Update Messages", 
    id='update-button-state',
    color="secondary", 
    className="me-1"
)
delete_button = dbc.Button(
    "Delete Messages", 
    id='delete-button-state', 
    color="danger", 
    className="me-1"
)

# Alert message
alert = dbc.Alert(
    "Message Sent!",
    id="alert",
    dismissable=True,
    is_open=False,
    duration=3000,
    color="success",
)

# Dark and Light Mode
color_mode_switch =  html.Span(
    [
        dbc.Label(className="fa fa-moon", html_for="switch"),
        dbc.Switch( id="switch", value=True, className="d-inline-block ms-1", persistence=True),
        dbc.Label(className="fa fa-sun", html_for="switch"),
    ]
)

### UI Layout
app.layout = dbc.Container(
    dbc.Row([
        dbc.Col([header], width=10),
        dbc.Col([switch_header], width=1, align='baseline'),
        dbc.Col([color_mode_switch], width=1, align='baseline'),
        dbc.Col([
            user_header,
            dbc.Row([
                input_groups,
                dbc.Col([
                    submit_button
                ])
            ]),
            dbc.Row(
                dbc.Col(alert, width=12),
            ),
        ], width=7),  # Left column taking 8 columns
        dbc.Col([
            dbc.Row([
                feed_header,
                message_box,
            ]),
            dbc.Row([
                dbc.Col([
                        update_button, 
                        delete_button
                ])
            ])
        ], width=5)  # Right column taking 4 columns
    ])
)
###

###
# Define callback to handle form submission
clientside_callback(
    """
    (switchOn) => {
       document.documentElement.setAttribute("data-bs-theme", switchOn ? "light" : "dark"); 
       return window.dash_clientside.no_update
    }
    """,
    Output("switch", "id"),
    Input("switch", "value"),
)

@app.callback(
    Output('alert', 'is_open'),
    Input('submit-button-state', 'n_clicks'),
    State('input-1-state', 'value'),
    State('input-2-state', 'value'),
    prevent_initial_call=True
)
def handle_submit(n_clicks, input1, input2):
    if n_clicks > 0:
        conn = get_message_db()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO message_db (handle, message) VALUES (?, ?)", (input1, input2))
        conn.commit()
        return True
    return False

# Define callback to update the message list
@app.callback(
    Output('messages-list', 'children'),
    Input('update-button-state', 'n_clicks'),
    prevent_initial_call=True
)
def update_messages(n_clicks):
    conn = get_message_db()
    cursor = conn.cursor()
    cursor.execute("SELECT handle, message FROM message_db")
    messages = cursor.fetchall()
    messages_list = messages_list = [
        html.Div([
            html.Div(handle, style={'font-weight': 'bold'}),
            html.Div(message)
        ], style={'margin-bottom': '10px'})
        for handle, message in messages[::-1]
    ]
    return messages_list

# @app.callback(
#     Input('update-button-state', 'n_clicks'),
#     prevent_initial_call=True
# )
# def update_messages(n_clicks):
#     conn = get_message_db()
#     cursor = conn.cursor()
#     cursor.execute("SELECT handle, message FROM message_db")
#     messages = cursor.fetchall()
#     messages_list = messages_list = [
#         html.Div([
#             html.Div(handle, style={'font-weight': 'bold'}),
#             html.Div(message)
#         ], style={'margin-bottom': '10px'})
#         for handle, message in messages[::-1]
#     ]
#     return messages_list


# Run the app on port 8051
if __name__ == '__main__':
    app.run_server(debug=True, port=8051)
