"""This module calculates a Wizards's load by exposing these API's -

1. POST /chat/ : Chat information between Wizard and Traveler
   Sample body - {"wizardid" : "wizard1", "chatid" : "traveler1", "time" : "2018-04-22 21:16:00"}
2. POST /wizard/traveler : Assign a Traveler to a specified Wizard
   Sample body - {"wizardid" : "wizard1", "chatid" : "traveler1"}
3. POST /wizard/shift : A Wizard toggles between his/her shift
   Sample body - {"wizardid" : "wizard1", "shift" : "OFF"}

NOTE : A traveler is identified by a chatid. chatid and traveler_id are used interchangeably.
"""
import os

from flask import Flask
from flask import request
from sqlalchemy import create_engine


application = Flask(__name__)


def connect_to_database():
    """Method to extract environment variables to connect to RDS instance."""
    rds_host = os.environ['RDS_HOSTNAME']
    rds_port = os.environ['RDS_PORT']
    rds_username = os.environ['RDS_USERNAME']
    rds_password = os.environ['RDS_PASSWORD']
    rds_database = os.environ['RDS_DB_NAME']

    return create_engine("mysql+mysqlconnector://"+rds_username+":"+ rds_password + "@" +
                         rds_host + ":" + rds_port + "/" + rds_database, echo=True)


engine = connect_to_database()

# SQL Queries to be used below.
CHAT_INSERT_QUERY = """
    INSERT INTO `CHAT_ACTIVITY` (wizard_id, traveler_id, chat_time)
    VALUES ('{0}', '{1}', '{2}') ON DUPLICATE KEY 
    UPDATE  wizard_id='{0}', chat_time='{2}'
"""

CHAT_DELETE_QUERY = """
    DELETE FROM CHAT_ACTIVITY WHERE chat_time < (NOW() - INTERVAL 10 MINUTE);
"""

WIZARD_INSERT_QUERY = """
    INSERT INTO `WIZARD_INFO` (wizard_id, traveler_id, shift)
    VALUES ('{0}', '{1}', '{2}')
"""

WIZARD_SHIFT_OFF_QUERY = """
    DELETE FROM WIZARD_INFO WHERE wizard_id = '{0}'
"""

WIZARD_SHIFT_ON_QUERY = """
    SELECT DISTINCT shift FROM WIZARD_INFO WHERE wizard_id = '{0}'
"""


@application.route('/', methods=['GET'])
def hello():
    """Show the name of concerned travelers handled
    by a Wizard with heavy load in the home page."""
    return compute_wizard_load()


@application.route('/chat', methods=['POST'])
def chat_activity():
    """Method to be called when a message is exchanged between a traveler and a wizard.
    In case a wizard wants to take over a traveler, he/she directly messages the traveler
    - relationship changes in WIZARD_INFO table."""
    content = request.get_json(silent=True)
    try:
        wizard_id = content['wizardid']
        traveler_id = content['chatid']
        chat_time = content['time']

    except KeyError as exc:
        print exc.message
        return "Uh-Oh! Bad payload. Make sure body has wizardid, chatid, and time fields."

    # Note : In Chat_Activity at any moment there will only be one traveler_id
    # denoting which traveler has sent message to which wizard or vice versa.
    # If any new wizard is assigned, this will be updated as and when a traveler sends a message.

    # Insert chat activity values, and keep only last ten minutes data
    engine.execute(CHAT_INSERT_QUERY.format(wizard_id, traveler_id, chat_time))
    engine.execute(CHAT_DELETE_QUERY)

    # Update Wizard->Travler information.
    update_wizard_traveler_info(traveler_id, wizard_id)

    return "Traveler " + traveler_id + " got message from " + wizard_id + " at " + str(chat_time)


@application.route('/wizard/traveler', methods=['POST'])
def wizard_activity():
    """Method to be called when an admin wizard assigns a traveler to a different wizard.
    Impacts WIZARD_INFO table."""
    content = request.get_json(silent=True)
    try:
        wizard_id = content['wizardid']
        traveler_id = content['chatid']

    except KeyError as exc:
        print exc.message
        return "Uh-Oh! Bad payload. Make sure body has wizardid and chatid fields."

    # Note : Chat_Activity won't be impacted because they reflect the event truth.
    # If a new wizard is assigned to traveler, whenever a message is sent to wizard
    # it will be reflected in Chat_Activity.

    # Update Wizard->Traveler information.
    update_wizard_traveler_info(traveler_id, wizard_id)
    return "Traveler " + traveler_id + " is assigned to " + wizard_id


@application.route('/wizard/shift', methods=['POST'])
def wizard_shift():
    """Method to be called when a Wizard changes his shifts between ON/OFF.
    Impacts WIZARD_INFO table."""
    content = request.get_json(silent=True)
    try:
        wizard_id = content['wizardid']
        shift = content['shift']

    except KeyError as exc:
        print exc.message
        return "Uh-Oh! Bad payload. Make sure body has wizardid and shift fields."

    # Check for valid values.
    if shift not in ["ON", "OFF"]:
        return "Uh-Oh! Wrong values. Make sure shift is 'ON' or 'OFF'."

    if shift == "ON":
        # Check if Wizard is in ACTIVE state.
        results = engine.execute(WIZARD_SHIFT_ON_QUERY.format(wizard_id))
        for result in results:
            if result["shift"] == "ON":
                return "Wizard " + wizard_id + " is already in active shift."

    # First delete obsolete records for wizard
    engine.execute(WIZARD_SHIFT_OFF_QUERY.format(wizard_id))

    # Setting the initial value when shift is ON/OFF for a wizard.
    engine.execute(WIZARD_INSERT_QUERY.format(wizard_id, 'NULL', shift))
    return "Wizard " + wizard_id


def compute_wizard_load():
    """Method to compute a Wizard's load and send notification to concerned users
    that traffic is high right now."""
    engine.execute(CHAT_DELETE_QUERY)

    wizard_ids = []
    traveler_ids = []

    # Filter out Wizards whose shift is ON and
    # are handling more than 3 travelers
    wizard_load_query = """
        SELECT wizard_id FROM WIZARD_INFO WHERE shift = 'ON' 
        GROUP BY wizard_id HAVING COUNT(traveler_id) > 3;
        """
    results = engine.execute(wizard_load_query)

    for result in results:
        wizard_ids.append("'"+result['wizard_id']+"'")

    if not wizard_ids:
        return "All wizards are happy for now. Low-load!"

    # Extract traveler ids that are serviced by heavy load wizards.
    load_query = """
        SELECT traveler_id FROM `CHAT_ACTIVITY` chat WHERE chat.wizard_id IN ({0});
        """

    results = engine.execute(load_query.format(",".join(wizard_ids)))

    for result in results:
        traveler_ids.append(result['traveler_id'])
    return "Load heavy for these travelers : {0}".format(traveler_ids)


def update_wizard_traveler_info(traveler_id, wizard_id):
    """Method to Check if traveler_id exists. Consider, a wizard has taken over a traveler
     by directly sending a message."""
    results = engine.execute("SELECT DISTINCT traveler_id FROM WIZARD_INFO")
    traveler_exists = False
    for result in results:
        if traveler_id == result["traveler_id"]:
            traveler_exists = True
            break

    if traveler_exists:
        # Delete an existing relationship between a previous wizard for a traveler
        engine.execute("DELETE FROM WIZARD_INFO WHERE traveler_id = '{0}'".format(traveler_id))
    # Insert the current relationship between Wizard and Traveler
    engine.execute(WIZARD_INSERT_QUERY.format(wizard_id, traveler_id, 'ON'))


if __name__ == '__main__':
    application.run()
