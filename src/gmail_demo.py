#!/usr/bin/env python

"""Gmail GSuite Demo.

gmail_demo sends emails based on a defined GSuite user email accounts to each other.
The subject, body of email, sender, receiver are all random.

@requires: pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

References:
https://medium.com/lyfepedia/sending-emails-with-gmail-api-and-python-49474e32c81f

"""

from __future__ import print_function
from apiclient import errors
import base64
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.mime.text import MIMEText
from google.oauth2 import service_account
from googleapiclient.discovery import build
# from httplib2 import Http
import getopt
import mimetypes
import os
import random
import requests
import sys
from textwrap import dedent
import time

__author__ = "Danny Cheun"
__credits__ = ["Danny Cheun"]
__version__ = "1.0.0"
__maintainer__ = "Danny Cheun"
__email__ = "dcheun@gmail.com"

# Export on *
# __all__ = []

# Globals
# Store script_args passed to script.
script_args = {}


class Analyzer(object):
    
    """Analyzes resources."""
    
    # Mail params
    _SMTP_ADDR = 'smtp.gmail.com'
    _SMTP_PORT = 587
    # Lorem Ipsum Generator API params
    _BLI_BASE_URL = 'https://baconipsum.com/api/'
    # Other attributes.
    _max_send = 0
    _users = {}
    _process_cnt = 0
    _debug = False
    
    def __init__(self, max_send=20):
        """Constructs a new Analyzer object.
        
        @keyword max_send: The maximum number of emails to send. Default=20.
                Also, limited to 500 to be polite.
        
        """
        try:
            self._max_send = int(max_send)
        except (ValueError,TypeError):
            self._max_send = 20
        if self._max_send > 500:
            self._max_send = 500
        self._users = {'user1@gmaildemo.com':{'pswd':'ZGVtb0AxMjM='},
                       'user2@gmaildemo.com':{'pswd':'ZGVtb0A0NTY='},
                       'user3@gmaildemo.com':{'pswd':'ZGVtb0A3ODk='},
                       'user4@gmaildemo.com':{'pswd':'ZGVtb0AwMTI='},
                       'user5@gmaildemo.com':{'pswd':'ZGVtb0AzNDU='},
                       }
    
    def get_random_text(self, _type=None, paras=None, sentences=None,
                        start_with_lorem=None, _format=None):
        """Makes an API call to Bacon Ipsum to generate random text.
        
        @keyword _type: Specify "all-meat" or "meat-and-filler".
                Default=Randomize choices.
        @keyword paras: Number of paragraphs.
                Default=Randomize between 1-10.
        @keyword sentences: Number of sentences (overrides paragraphs).
                Default=None
        @keyword start_with_lorem: Specify 1 to start the first paragraph with
                "Bacon ipsum dolor sit amet"
                Default=None
        @keyword _format: Only html or text supported.
                Default=html
        
        """
        URL = [self._BLI_BASE_URL]
        # handle _type
        _type_opts = ['all-meat', 'meat-and-filler']
        if _type not in _type_opts:
            random.shuffle(_type_opts)
            _type = _type_opts[0]
        URL.append('?type=%s' % _type)
        # handle paras
        try:
            int(paras)
        except (ValueError, TypeError):
            paras = random.randint(1, 10)
        URL.append('&paras=%s' % paras)
        # handle sentences
        try:
            int(sentences)
        except (ValueError, TypeError):
            sentences = None
        if sentences:
            URL.append('&sentences=%s' % sentences)
        # handle start_with_lorem
        try:
            if int(start_with_lorem) != 1:
                start_with_lorem = False
        except (ValueError, TypeError):
            start_with_lorem = False
        if start_with_lorem:
            URL.append('&start-with-lorem=1')
        # handle format - Only html or text supported.
        _format_opts = ['html', 'text']
        if _format not in _format_opts:
            _format = 'html'
        URL.append('&format=%s' % _format)
        # Make API call.
        request_url = ''.join(URL)
        r = requests.get(request_url)
        # Validate response.
        self.validate_response(r)
        return r._content
    
    @staticmethod
    def validate_response(response):
        """Validates HTTP request responses.
        
        @param response: The requests.Response object to validate.
        
        """
        r = response
        if not isinstance(r, requests.Response):
            raise Exception('response must be instance of requests.Response.')
        # Check for OK status.
        if r.status_code not in [200, 204]:
            err = ["response: status_code=%s,reason='%s'" %
                   (r.status_code, r.reason)]
            raise Exception(''.join(err))
    
    def process(self):
        """Top level process."""
        users = self._users.keys()
        for i in range(self._max_send):
            # Get random sender and recipient.
            random.shuffle(users)
            from_addr = users[0]
            to_addrs = users[1]
            # Get random text for subject line, trim off after 75 chars.
            subject = self.get_random_text(sentences=1, _format='text')[:75]
            body = self.get_random_text()
            html = body
            # Compose and send message.
            print('INFO: Sending mail: from_addr=%s,to_addrs=%s,subject=%s' %
                  (from_addr, to_addrs, subject))
            self.send_mail(from_addr, to_addrs, subject, body, html=html)
            self._process_cnt += 1
            # Courtesy wait.
            time.sleep(0.5)
    
    @staticmethod
    def send_message(service, user_id, message):
        """Send an email message.
        
        @param service: Authorized Gmail API service instance.
        @param user_id: User's email address. The special value "me"
                can be used to indicate the authenticated user.
        @param message: Message to be sent.
        
        @return: Sent message.
        
        """
        try:
            message = (service.users().messages().send(userId=user_id, body=message)
                       .execute())
            print('Message Id: %s' % message['id'])
            return message
        except errors.HttpError as error:
            print('An error occurred: %s' % error)
    
    @staticmethod
    def service_account_login(email_from):
        SCOPES = ['https://www.googleapis.com/auth/gmail.send']
        # This file must be in the same directory.
        SERVICE_ACCOUNT_FILE = 'service-key.json'
        
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        delegated_credentials = credentials.with_subject(email_from)
        service = build('gmail', 'v1', credentials=delegated_credentials)
        return service
    
    def send_mail(self, from_addr, to_addrs, subject, body, cc='',
                  bcc='', html=None, files=[], high_importance=False):
        """Sends an email to the specified address via GSuite email account.
        
        @param from_addr: The sender's email address. This is the email account
                to log into to send the email.
        @param to_addrs: The recipient's email address.  Note that if this
                contains multiple addresses, pass in a string containing
                comma separated addresses.
        @param subject: The subject of the email.
        @param body: The body of the email.
        @keyword cc: Comma separated string of the Cc addresses.
        @keyword bcc: Comma separated string of the Bcc addresses.
        @keyword html: The HTML text.
                Note the body should contain the text part,
                otherwise set body to ''.
        @keyword files: A list of files to attach (absolute filenames).
       
        """
        ##############################################
        # Mixed part is for attachments.
        ##############################################
        msg = MIMEMultipart('mixed')
        msg['Subject'] = subject
        msg['From'] = from_addr
        msg['To'] = to_addrs
        msg['Cc'] = cc
        if high_importance:
            msg['Importance'] = 'high'
        ##############################################
        # Alternative part. This is for text and html.
        ##############################################
        altpart = MIMEMultipart('alternative')
        # Attach body as plain text part of email.
        plain_text_part = MIMEText(body, 'plain', 'utf-8')
        altpart.attach(plain_text_part)
        # Attach HTML part if specified.
        if html:
            html_part = MIMEText(html, 'html', 'utf-8')
            altpart.attach(html_part)
        # Now attach alternative part to mixed part.
        msg.attach(altpart)
        # Email file attachments.
        for f in files:
            with open(f,'rb') as fp:
                # Guess type.
                mtype, msubtype = (mimetypes.guess_type(f)[0] or
                                   'application/octet-stream').split('/')
                attachment = MIMEBase(mtype, msubtype)
                attachment.add_header('Content-Disposition',
                                      'attachment;filename="%s"' % os.path.basename(f))
                attachment.add_header('Content-Transfer-Encoding', 'base64')
                attachment.set_payload(base64.b64encode(fp.read()))
                msg.attach(attachment)
        if to_addrs is None:
            to_addrs = ''
        if cc is None:
            cc = ''
        # Use Google API to send mail.
        service = self.service_account_login(from_addr)
        message = {'raw': base64.urlsafe_b64encode(msg.as_string())}
        return self.send_message(service, from_addr, message)


###############################################################################
# Main.
###############################################################################
def usage():
    """Print usage info."""
    program_name = os.path.basename(sys.argv[0])
    message = ['Usage: %s <options>...' % program_name]
    message.append(dedent('''
    Optional argument(s):
      -m <MAX_SEND>, --max-send=<MAX_SEND>
            The maximum emails to send with a ceiling of 100.
      -h, --help
            Displays this help screen.
    '''))
    print('\n'.join(message))


def handle_args():
    """Handle script's command line script_args."""
    global script_args
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'm:h',
                                   ['max-send=',
                                    'help','debug'])
    except getopt.GetoptError as e:
        # Print usage info and exit.
        print(str(e))
        usage()
        sys.exit(2)
    
    for o, a in opts:
        if o == '-m' or o == '--max-send':
            script_args['max_send'] = a
        elif o == '-h' or o == '--help':
            script_args['help'] = a
        elif o == '--debug':
            script_args['debug'] = a
        else:
            assert False, 'Unhandled option %s' % o
    
    # Check for help.
    if 'help' in script_args:
        usage()
        sys.exit(0)


def main():
    global script_args
    handle_args()
    analyzer = Analyzer(max_send=script_args.get('max_send', 20))
    analyzer.process()
    print('Done sending %s emails.' % analyzer._process_cnt)

if __name__ == '__main__':
    main()
