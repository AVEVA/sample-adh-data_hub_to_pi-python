from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from enum import Enum
from queue import Queue
import traceback
import threading
import datetime
import json
import time

from adh_sample_library_preview import (ADHClient, SdsType, SdsStream)
from PIOMFClient import (PIOMFClient, OMFMessageAction, OMFMessageType)


class Mode(Enum):
    NORMAL = 0
    BACKFILL_ALL = 1
    BACKFILL_N_DAYS = 2


# Global variables
send_period = 30
max_events = 5000
data_request_period = 5
mode = Mode.BACKFILL_N_DAYS
days_to_backfill = 7

type_code_format = {
    3: None,
    7: 'int16',
    8: 'uint16',
    9: 'int32',
    10: 'int32',
    11: 'int64',
    12: 'uint64',
    13: 'float32',
    14: 'float64',
    16: 'date-time',
    18: None,
    103: 'boolean',
    107: 'int16',
    108: 'uint16',
    109: 'int32',
    110: 'int32',
    111: 'int64',
    112: 'uint64',
    113: 'float32',
    114: 'float64',
    116: 'date-time',
    118: None
}

type_code_type = {
    3: 'boolean',
    7: 'integer',
    8: 'integer',
    9: 'integer',
    10: 'integer',
    11: 'integer',
    12: 'integer',
    13: 'number',
    14: 'number',
    16: 'string',
    18: 'string',
    103: ['boolean', 'null'],
    107: ['integer', 'null'],
    108: ['integer', 'null'],
    109: ['integer', 'null'],
    110: ['integer', 'null'],
    111: ['integer', 'null'],
    112: ['integer', 'null'],
    113: ['number', 'null'],
    114: ['number', 'null'],
    116: 'string',
    118: 'string'
}


def getAppsettings():
    """Open and parse the appsettings.json file"""

    # Try to open the configuration file
    try:
        with open(
            'appsettings.json',
            'r',
        ) as f:
            appsettings = json.load(f)
    except Exception as error:
        print(f'Error: {str(error)}')
        print(f'Could not open/read appsettings.json')
        exit()

    return appsettings


def removeDuplicates(list):
    """Remove Duplicate entries from a list based on Ids"""

    id_set = set()
    reduced_list = []
    for item in list:
        if item.Id not in id_set:
            reduced_list.append(item)
            id_set.add(item.Id)

    return reduced_list


def convertType(type: SdsType):
    """Convert an SdsType into an OMF Type"""

    omf_type = {
        'id': type.Id,
        'name': type.Name,
        'classification': 'dynamic',
        'type': 'object',
        'description': type.Description,
        'properties': {
        }
    }

    for property in type.Properties:
        omf_property = {
            property.Name: {
                "type": type_code_type.get(property.SdsType.SdsTypeCode.value)
            }
        }

        if (property.IsKey):
            omf_property.get(property.Name)['isindex'] = True

        if type_code_format.get(property.SdsType.SdsTypeCode.value) is not None:
            omf_property.get(property.Name)['format'] = type_code_format.get(
                property.SdsType.SdsTypeCode.value)

        if property.Description is not None:
            omf_property.get(property.Name)[
                'description'] = property.Description

        omf_type['properties'].update(omf_property)

    return omf_type


def convertContainer(stream: SdsStream):
    """Convert an SdsStream into an OMF Container"""

    return {
        'id': stream.Id,
        'name': stream.Name,
        'typeid': stream.TypeId,
        'description': stream.Description,
        'datasource': 'Data Hub'
    }


def convertData(container_id, data):
    """Convert an Sds data event into an OMF data message"""

    if isinstance(data, list):
        return {
            "containerid": container_id,
            "values": data
        }
    else:
        return {
            "containerid": container_id,
            "values": [data]
        }


def queueStreamData(queue, stream, type_index, namespace_id, sds_client, start_index, end_index):
    """Query for data from a stream and add it to the queue"""

    if start_index is None:
        return None

    new_start_index = start_index
    try:
        results_page = sds_client.Streams.getWindowValuesPaged(
            namespace_id, stream.Id, value_class=None, start=start_index, end=end_index, count=250000)

        for result in results_page.Results:
            if result[type_index] != start_index:
                queue.put(convertData(stream.Id, result))

        if results_page.Results != []:
            new_start_index = results_page.Results[-1][type_index]

        while not results_page.end():
            results_page = sds_client.Streams.getWindowValuesPaged(
                namespace_id, stream.Id, value_class=None, start=start_index, end=end_index, count=250000, continuation_token=results_page.ContinuationToken)

            for result in results_page.Results:
                queue.put(convertData(stream.Id, result))

            if results_page.Results != []:
                new_start_index = results_page.Results[-1][type_index]

    except Exception as ex:
        print((f"Encountered Error: {ex}"))
        print
        traceback.print_exc()
        print

    return new_start_index


def dataRetrievalTask(queue: Queue, mode: Mode, sds_client: ADHClient, namespace_id: str, streams: list[SdsStream]):
    """Task for retrieving data from Data Hub and adding it to the queue"""

    # get the start index and type index for each stream
    start_indexes = []
    type_indexes = []

    # get index
    for stream in streams:
        type = sds_client.Types.getType(namespace_id, stream.TypeId)
        for property in type.Properties:
            # if property.IsKey:
            type_indexes.append(property.Id)

    if mode == Mode.BACKFILL_ALL:
        for index, stream in enumerate(streams):
            first_value = sds_client.Streams.getFirstValue(
                namespace_id, stream.Id)
            if first_value is not None:
                start_indexes.append(first_value[type_indexes[index]])
            else:
                start_indexes.append(None)
            # push the first value
            queue.put(convertData(stream.Id, first_value))
    elif mode == Mode.BACKFILL_N_DAYS:
        for stream in streams:
            start_indexes.append((datetime.datetime.utcnow(
            ) - datetime.timedelta(days=days_to_backfill)).isoformat() + 'Z')
    else:
        for stream in streams:
            start_indexes.append(
                (datetime.datetime.utcnow()).isoformat() + 'Z')

    # retrieve data and add to queue forever
    while True:

        results = []
        with ThreadPoolExecutor() as pool:
            results = pool.map(queueStreamData, repeat(queue), streams, type_indexes, repeat(namespace_id), repeat(
                sds_client), start_indexes, repeat(datetime.datetime.utcnow().isoformat() + 'Z'))

        for index, result in enumerate(results):
            start_indexes[index] = result

        time.sleep(data_request_period)


def dataSendingTask(queue: Queue, pi_omf_client: PIOMFClient):
    """Task for getting data from the queue and sending it to PI"""

    timer = time.time()
    event_count = 0

    while True:
        if time.time() - timer > send_period or queue.qsize() >= max_events:
            # Read data from queue
            data = []
            for _ in range(max_events):
                if (queue.empty()):
                    break
                data.append(queue.get())
            event_count = len(data)

            # consolidate list by container id
            consolidated_data = []
            container_dictionary = {}
            for datum in data:
                if datum.get('containerid') in container_dictionary:
                    container_dictionary[datum.get(
                        'containerid')] += datum.get('values')
                else:
                    container_dictionary[datum.get(
                        'containerid')] = datum.get('values')

            for k, v in container_dictionary.items():
                consolidated_data.append({'containerid': k, 'values': v})

            # Send events
            while data != []:
                try:
                    response = pi_omf_client.omfRequest(
                        OMFMessageType.Data, OMFMessageAction.Update, data)
                    pi_omf_client.checkResponse(
                        response, 'Error updating data')
                    data = []
                except Exception as ex:
                    print((f"Encountered Error: {ex}"))
                    print
                    traceback.print_exc()
                    print

            # Reset timer and data
            print(f'Queue size: {queue.qsize()}, Events/second: {event_count/(time.time()-timer)}')
            timer = time.time()

            print(queue.qsize())


if __name__ == "__main__":
    print('Starting!')

    # Read appsettings
    print('Reading appsettings...')
    appsettings = getAppsettings()
    data_hub_appsettings = appsettings.get('DataHub')
    pi_appsettings = appsettings.get('PI')
    queries = appsettings.get('Queries')

    # Create PI OMF client
    print('Creating a PI OMF client...')
    pi_omf_client = PIOMFClient(
        pi_appsettings.get('Resource'),
        pi_appsettings.get('DataArchiveName'),
        pi_appsettings.get('Username'),
        pi_appsettings.get('Password'),
        pi_appsettings.get('OMFVersion', '1.2'),
        pi_appsettings.get('VerifySSL', True)
    )

    # Create a Data Hub client
    print('Creating a Data Hub client...')
    sds_client = ADHClient(
        data_hub_appsettings.get('ApiVersion'),
        data_hub_appsettings.get('TenantId'),
        data_hub_appsettings.get('Resource'),
        data_hub_appsettings.get('ClientId'),
        data_hub_appsettings.get('ClientSecret'))
    namespace_id = data_hub_appsettings.get('NamespaceId')

    # Collect a list of streams to transfer
    print('Collecting a list of streams to transfer...')
    streams = []
    for query in queries:
        streams_result = sds_client.Streams.getStreams(
            namespace_id, query.get('Value'), count=1)
        i = 1
        while streams_result != []:
            streams += streams_result
            streams_result = sds_client.Streams.getStreams(
                namespace_id, query.get('Value'), skip=1*i, count=1)
            i += 1

    streams = removeDuplicates(streams)

    # Create types if they do not exist
    print('Creating types...')

    # Collect a list of unique type Ids
    type_id_set = set()
    for stream in streams:
        if stream.TypeId not in type_id_set:
            type_id_set.add(stream.TypeId)

    types = []
    for type_id in type_id_set:
        types.append(convertType(
            sds_client.Types.getType(namespace_id, type_id)))

    response = pi_omf_client.omfRequest(
        OMFMessageType.Type, OMFMessageAction.Create, types)
    pi_omf_client.checkResponse(response, 'Error creating types')

    # Create containers
    print('Creating containers...')
    containers = []
    for stream in streams:
        containers.append(convertContainer(stream))

    response = pi_omf_client.omfRequest(
        OMFMessageType.Container, OMFMessageAction.Create, containers)
    pi_omf_client.checkResponse(response, 'Error creating containers')

    # Continuously send data
    print('Sending data...')
    queue = Queue(maxsize=0)
    t1 = threading.Thread(target=dataRetrievalTask, args=(
        queue, mode, sds_client, namespace_id, streams,))
    t2 = threading.Thread(target=dataSendingTask, args=(
        queue, pi_omf_client))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
