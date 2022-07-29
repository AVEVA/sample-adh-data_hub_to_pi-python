from __future__ import annotations
from enum import Enum
import logging
import requests
import json
import gzip


class OMFMessageType(Enum):
    """
    enum 0-2
    """
    Type = 'Type'
    Container = 'Container'
    Data = 'Data'


class OMFMessageAction(Enum):
    """
    enum 0-2
    """
    Create = 'Create'
    Update = 'Update'
    Delete = 'Delete'


class PIOMFClient(object):
    """Handles communication with PI OMF Endpoint."""

    def __init__(self, url: str, data_archive: str, username: str,
                 password: str, omf_version: str = '1.2', verify_ssl: bool = True, logging_enabled: bool = False):
        self.__url = url
        self.__data_archive = data_archive
        self.__username = username
        self.__password = password
        self.__omf_version = omf_version
        self.__verify_ssl = verify_ssl
        self.__logging_enabled = logging_enabled

        self.__omf_endpoint = f'{url}/omf'
        self.__session = requests.Session()

    @property
    def Url(self) -> str:
        """
        Gets the base url
        :return:
        """
        return self.__url

    @property
    def DataArchive(self) -> str:
        """
        Gets the data archive
        :return:
        """
        return self.__data_archive

    @property
    def Username(self) -> str:
        """
        Gets the username
        :return:
        """
        return self.__username

    @property
    def Password(self) -> str:
        """
        Gets the password
        :return:
        """
        return self.__password

    @property
    def OMFVersion(self) -> str:
        """
        Gets the omf version
        :return:
        """
        return self.__omf_version

    @property
    def VerifySSL(self) -> bool:
        """
        Gets whether SSL should be verified
        :return:
        """
        return self.__verify_ssl

    @property
    def LoggingEnabled(self) -> bool:
        """
        Whether logging is enabled (default False)
        :return:
        """
        return self.LoggingEnabled    
    
    @LoggingEnabled.setter
    def logging_enabled(self, value: bool):
        self.LoggingEnabled = value

    @property
    def OMFEndpoint(self) -> str:
        """
        Gets the omf endpoint
        :return:
        """
        return self.__omf_endpoint

    def verifySuccessfulResponse(self, response, main_message: str, throw_on_bad: bool = True):
        """
        Verifies that a response was successful and optionally throws an exception on a bad response
        :param response: Http response
        :param main_message: Message to print in addition to response information
        :param throw_on_bad: Optional parameter to throw an exception on a bad response
        """

        # response code in 200s if the request was successful!
        if response.status_code < 200 or response.status_code >= 300:
            response.close()
            if self.__logging_enabled:
                logging.info(f'request executed in {response.elapsed.microseconds / 1000}ms - status code: {response.status_code}')
                logging.debug(f'{main_message}. Response: {response.status_code} {response.text}.')

            if throw_on_bad:
                raise Exception(
                    f'{main_message}. Response: {response.status_code} {response.text}. ')

    def omfRequest(self, message_type: OMFMessageType, action: OMFMessageAction, omf_message_json: dict[str, str]):
        """
        Base OMF request function
        :param message_type: OMF message type
        :param action: OMF action
        :param omf_message_json: OMF message
        :return: Http response
        """

        if not self.VerifySSL:
            print('You are not verifying the certificate of the end point. This is not advised for any system as there are security issues with doing this.')

        if self.__logging_enabled:
            logging.warn(f'You are not verifying the certificate of the end point. This is not advised for any system as there are security issues with doing this.')

        msg_body = gzip.compress(bytes(json.dumps(omf_message_json), 'utf-8'))
        msg_headers = {
            'messagetype': message_type.value,
            'action': action.value,
            'messageformat': 'JSON',
            'omfversion': self.OMFVersion,
            'compression': 'gzip',
            'x-requested-with': 'xmlhttprequest'
        }

        return self.__session.post(
            self.OMFEndpoint,
            headers=msg_headers,
            data=msg_body,
            verify=self.VerifySSL,
            timeout=10000,
            auth=(self.Username, self.Password)
        )
