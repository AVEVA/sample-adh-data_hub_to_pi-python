from datetime import datetime, timedelta
import json
import random
import unittest

from adh_sample_library_preview import ADHClient, SdsType, SdsStream
from PIOMFClient import OMFMessageAction, OMFMessageType, PIOMFClient
from program import main, getAppsettings, convertType, convertContainer


class SampleE2ETests(unittest.TestCase):

    @classmethod
    def test_main(cls):

        create_test_environment()

        main(test=True)

        cleanup_test_environment()


test_adh_type = {
    "Id": "DataHubToPIType",
    "Name": "DataHubToPIType",
    "SdsTypeCode": 1,
    "Properties": [
                {
                    "Id": "Timestamp",
                    "Name": "Timestamp",
                    "IsKey": True,
                    "SdsType": {
                            "Name": "DateTime",
                            "SdsTypeCode": 16,
                    },
                },
            {
                    "Id": "Value",
                    "Name": "Value",
                    "SdsType": {
                            "Name": "NullableDouble",
                            "SdsTypeCode": 114,
                    },
            }
    ]
}

test_adh_stream = {
    "TypeId": "DataHubToPIType",
    "Id": "DataHubToPIStream",
    "Name": "DataHubToPIStream"
}


def suppress_error(function):
    """Suppress an error"""
    try:
        function()
    except Exception as error:
        print(f'Encountered Error: {error}')


def get_current_time(offset=0):
    ''' Returns the current time'''
    return (datetime.utcnow() - timedelta(seconds=offset)).isoformat() + 'Z'


def create_test_environment():

    # Get appsettings
    appsettings = getAppsettings()
    data_hub_appsettings = appsettings.get('DataHub')

    # Create an ADH client
    sds_client = ADHClient(
        data_hub_appsettings.get('ApiVersion'),
        data_hub_appsettings.get('TenantId'),
        data_hub_appsettings.get('Resource'),
        data_hub_appsettings.get('ClientId'),
        data_hub_appsettings.get('ClientSecret'))
    namespace_id = data_hub_appsettings.get('NamespaceId')

    # Create a type
    sds_client.Types.getOrCreateType(
        namespace_id, SdsType.fromJson(test_adh_type))

    # Create a stream
    sds_client.Streams.getOrCreateStream(
        namespace_id, SdsStream.fromJson(test_adh_stream))

    # Add test data
    data = []
    data_count = 1000
    for i in range(data_count):
        data.append({
            'Timestamp': get_current_time(i * 5),
            'Value': 100*random.random()
        })
    sds_client.Streams.updateValues(
        namespace_id, test_adh_stream.get('Id'), json.dumps(data))

    print('Environment Created!')


def cleanup_test_environment():

    # Get appsettings
    appsettings = getAppsettings()
    data_hub_appsettings = appsettings.get('DataHub')
    pi_appsettings = appsettings.get('PI')

    # Create an ADH client
    sds_client = ADHClient(
        data_hub_appsettings.get('ApiVersion'),
        data_hub_appsettings.get('TenantId'),
        data_hub_appsettings.get('Resource'),
        data_hub_appsettings.get('ClientId'),
        data_hub_appsettings.get('ClientSecret'))
    namespace_id = data_hub_appsettings.get('NamespaceId')

    # Create an OMF client
    pi_omf_client = PIOMFClient(
        pi_appsettings.get('Resource'),
        pi_appsettings.get('DataArchiveName'),
        pi_appsettings.get('Username'),
        pi_appsettings.get('Password'),
        pi_appsettings.get('OMFVersion', '1.2'),
        pi_appsettings.get('VerifySSL', True)
    )

    # Generate a prefix for later
    namespaces = sds_client.Namespaces.getNamespaces()
    namespace = [n for n in namespaces if n.Id == namespace_id]
    prefix = f'{namespace[0].Description}'

    # Cleanup ADH stream
    suppress_error(lambda: sds_client.Streams.deleteStream(
        namespace_id, test_adh_stream.get('Id')))

    # Cleanup ADH type
    suppress_error(lambda: sds_client.Types.deleteType(
        namespace_id, test_adh_type.get('Id')))

    # Cleanup OMF container
    resolved_stream = SdsStream.fromJson(test_adh_stream)
    resolved_stream.Type = SdsType.fromJson(test_adh_type)
    suppress_error(lambda: pi_omf_client.omfRequest(OMFMessageType.Container, OMFMessageAction.Delete, [
                   convertContainer(resolved_stream, prefix)]))

    # Cleanup OMF type
    suppress_error(lambda: pi_omf_client.omfRequest(OMFMessageType.Type, OMFMessageAction.Delete, [
                   convertType(SdsType.fromJson(test_adh_type), prefix)]))


if __name__ == '__main__':

    unittest.main()
