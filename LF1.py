import json
import datetime
import time
import os
import dateutil.parser
import logging
import boto3
import math

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# --- Helpers that build all of the responses ---

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def confirm_intent(session_attributes, intent_name, slots, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')

def isvalid_city(city):
    valid_cities = ['new york', 'los angeles', 'chicago', 'houston', 'philadelphia', 'phoenix', 'san antonio',
                    'san diego', 'dallas', 'san jose', 'austin', 'jacksonville', 'san francisco', 'indianapolis',
                    'columbus', 'fort worth', 'charlotte', 'detroit', 'el paso', 'seattle', 'denver', 'washington dc',
                    'memphis', 'boston', 'nashville', 'baltimore', 'portland']
    return city.lower() in valid_cities

def isvalid_cusine(cusine):
    valid_cusine = ['thai','indian', 'chinese', 'mexican', 'italian', 'french', 'japanese', 'american', 'continental', 'mediterrian','greek','korean','african']
    return cusine.lower() in valid_cusine


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def get_day_difference(later_date, earlier_date):
    later_datetime = dateutil.parser.parse(later_date).date()
    earlier_datetime = dateutil.parser.parse(earlier_date).date()
    return abs(later_datetime - earlier_datetime).days


def add_days(date, number_of_days):
    new_date = dateutil.parser.parse(date).date()
    new_date += datetime.timedelta(days=number_of_days)
    return new_date.strftime('%Y-%m-%d')


def build_validation_result(isvalid, violated_slot, message_content):
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def validateDiningIntent(slots):
    location = slots['help']
    cuisine = slots['city_done']
    numOfPeople = slots['cusine_done']
    date = slots['personcount_done']
    time = slots['date_done']
    email = slots['time_done']
    
    
    if location and not isvalid_city(location):
        return build_validation_result(
            False,
            'help',
            'We do not support {} as of now'.format(location)
        )
    
    if cuisine and not isvalid_cusine(cuisine):
        return build_validation_result(
            False,
            'city_done',
            'We do not have suggestions for  {} as of now'.format(cuisine)
        )
    
    if numOfPeople:
        if int(numOfPeople) <= 0 or int(numOfPeople) >10:
            return build_validation_result(False,
                                           'cusine_done',
                                           'Maximum 10 people allowed. Try again')

    if date:
        if not isvalid_date(date):
            return build_validation_result(False, 'personcount_done', 'I did not understand the date, Kindly let me know again')
        if datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'personcount_done', 'Date has Already passed away. Kindly enter a valid date')
    # if time:
    #     if len(time) != 5:
    #         # Not a valid time; use a prompt defined on the build-time model.
    #         return build_validation_result(False, 'date_done', None)

    #     hour, minute = time.split(':')
    #     hour = parse_int(hour)
    #     minute = parse_int(minute)
    #     if math.isnan(hour) or math.isnan(minute):
    #         # Not a valid time; use a prompt defined on the build-time model.
    #         return build_validation_result(False, 'date_done', None)

    #     if hour < 10 or hour > 16:
    #         # Outside of business hours
    #         return build_validation_result(False, 'date_done', None)
    
    if email:
        sqs = boto3.resource('sqs',region_name='us-east-1', aws_access_key_id='AKIAWAVSKGK2LZP7PPNS', aws_secret_access_key='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')
        msg = {"cuisine": cuisine, "email": email}
        que = sqs.get_queue_by_name(QueueName='RestaurantSuggestionRequest')
        response = que.send_message(MessageBody=json.dumps(msg))
        logger.debug(response)
    return {'isValid': True}


def GreetingIntent(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Hi there, how can I help?'}
        }
    }


def DiningSuggestionsIntent(intent_request):
    if intent_request['invocationSource'] == 'DialogCodeHook':
        slots = get_slots(intent_request)

        validation_result = validateDiningIntent(slots)

        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])
        else:
            close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Great! You will receive your suggestion shortly.'})

        if intent_request[
            'sessionAttributes'] is not None:
            output_session_attributes = intent_request['sessionAttributes']
        else:
            output_session_attributes = {}

        return delegate(output_session_attributes, get_slots(intent_request))


def ThankYouIntent(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'You’re welcome.'}
        }
    }

# --- Intents ---


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'GreetingIntent':
        return GreetingIntent(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        return DiningSuggestionsIntent(intent_request)
    elif intent_name == 'ThankYouIntent' :
        return ThankYouIntent(intent_request)
        

    raise Exception('Intent with name ' + intent_name + ' not supported')


# --- Main handler ---


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    #logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
