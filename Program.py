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

from adh_sample_library_preview import (
    ADHClient, SdsResolvedStream, SdsType, SdsBoundaryType)
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


def convertType(type: SdsType, prefix: str):
    """Convert an SdsType into an OMF Type"""

    omf_type = {
        'id': f'{prefix}_{type.Id}',
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


def convertContainer(stream: SdsResolvedStream):
    """Convert an SdsStream into an OMF Container"""

    return {
        'id': f'{stream.Prefix}_{stream.Id}',
        'name': stream.Name,
        'typeid': f'{stream.Prefix}_{stream.Type.Id}',
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


def queueStreamData(queue, resolved_stream, type_index, namespace_id, community_id, sds_client: ADHClient, start_index, end_index, start_boundary):
    """Query for data from a stream and add it to the queue"""

    if start_index is None:
        return (None, start_boundary)

    new_start_index = start_index
    try:
        if community_id:
            results_page = sds_client.SharedStreams.getWindowValuesPaged(
                resolved_stream.TenantId, resolved_stream.NamespaceId, community_id, resolved_stream.Id, start=start_index, end=end_index, count=250000, startBoundaryType=start_boundary, endBoundaryType=SdsBoundaryType.Exact)
        else:
            results_page = sds_client.Streams.getWindowValuesPaged(
                namespace_id, resolved_stream.Id, start=start_index, end=end_index, count=250000, startBoundaryType=start_boundary, endBoundaryType=SdsBoundaryType.Exact)

        for result in results_page.Results:
            stream_id = f'{resolved_stream.Prefix}_{resolved_stream.Id}'
            if result[type_index] != start_index:
                queue.put(convertData(stream_id, result))

        if results_page.Results != []:
            new_start_index = results_page.Results[-1][type_index]
            start_boundary = SdsBoundaryType.Inside

        while not results_page.end():
            if community_id:
                results_page = sds_client.SharedStreams.getWindowValuesPaged(
                    resolved_stream.TenantId, resolved_stream.NamespaceId, community_id, resolved_stream.Id, start=start_index, end=end_index, count=250000, continuation_token=results_page.ContinuationToken)
            else:
                results_page = sds_client.Streams.getWindowValuesPaged(
                    namespace_id, resolved_stream.Id, start=start_index, end=end_index, count=250000, continuation_token=results_page.ContinuationToken)

            for result in results_page.Results:
                queue.put(convertData(stream_id, result))

            if results_page.Results != []:
                new_start_index = results_page.Results[-1][type_index]

    except Exception as ex:
        print((f"Encountered Error: {ex}"))
        print
        traceback.print_exc()
        print

    return (new_start_index, start_boundary)


def dataRetrievalTask(queue: Queue, mode: Mode, sds_client: ADHClient, namespace_id: str, community_id: str, resolved_streams: list[SdsResolvedStream]):
    """Task for retrieving data from Data Hub and adding it to the queue"""

    start_indexes = []
    type_indexes = []
    start_boundaries = [SdsBoundaryType.Exact] * len(resolved_streams)

    # Get indexes from types
    for resolved_stream in resolved_streams:
        for property in resolved_stream.Type.Properties:
            if property.IsKey:
                type_indexes.append(property.Id)

    # Get start indexes for each stream
    if mode == Mode.BACKFILL_ALL:
        for index, resolved_stream in enumerate(resolved_streams):
            first_value = sds_client.SharedStreams.getFirstValue(resolved_stream.TenantId, resolved_stream.NamespaceId, community_id,
                                                                 resolved_stream.Id) if community_id else sds_client.Streams.getFirstValue(namespace_id, resolved_stream.Id)
            if first_value is not None:
                start_indexes.append(first_value[type_indexes[index]])
            else:
                start_indexes.append(None)
    elif mode == Mode.BACKFILL_N_DAYS:
        for resolved_stream in resolved_streams:
            start_indexes.append((datetime.datetime.utcnow(
            ) - datetime.timedelta(days=days_to_backfill)).isoformat() + 'Z')
    else:
        for resolved_stream in resolved_streams:
            start_indexes.append(
                (datetime.datetime.utcnow()).isoformat() + 'Z')

    # Retrieve data and add to queue forever
    while True:

        results = []
        with ThreadPoolExecutor() as pool:
            results = pool.map(queueStreamData, repeat(queue), resolved_streams, type_indexes, repeat(namespace_id), repeat(community_id), repeat(
                sds_client), start_indexes, repeat(datetime.datetime.utcnow().isoformat() + 'Z'), start_boundaries)

        for index, result in enumerate(results):
            start_indexes[index] = result[0]
            start_boundaries[index] = result[1]

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

            # Consolidate list by container id
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
            sent = False
            while not sent:
                try:
                    response = pi_omf_client.omfRequest(
                        OMFMessageType.Data, OMFMessageAction.Update, consolidated_data)
                    pi_omf_client.checkResponse(
                        response, 'Error updating data')
                    sent = True
                except Exception as ex:
                    print((f"Encountered Error: {ex}"))
                    print
                    traceback.print_exc()
                    print

            print(
                f'Queue size: {queue.qsize()}, Events/second: {event_count/(time.time()-timer)}')

            # Reset timer
            timer = time.time()


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
    community_id = data_hub_appsettings.get('CommunityId')

    # Get Tenants and Namespaces
    tenants = []
    namespaces = []
    namespaces = sds_client.Namespaces.getNamespaces()
    if community_id:
        communities = sds_client.Communities.getCommunities()
        for community in communities:
            tenants += community.Tenants
    removeDuplicates(tenants)

    # Collect a list of streams to transfer
    print('Collecting a list of streams to transfer...')
    resolved_streams = []
    if community_id:
        for query in queries:
            streams_result = sds_client.Communities.getCommunityStreams(
                community_id, query.get('Value'), count=100)
            i = 1
            while streams_result != []:
                for stream_search_result in streams_result:
                    resolved_stream = sds_client.SharedStreams.getResolvedStream(
                        stream_search_result.TenantId, stream_search_result.NamespaceId, stream_search_result.CommunityId, stream_search_result.Id)
                    resolved_stream.TenantId = stream_search_result.TenantId
                    resolved_stream.NamespaceId = stream_search_result.NamespaceId
                    # Create prefix to avoid collisions
                    resolved_stream.Prefix = f'{stream_search_result.TenantName}'
                    resolved_streams.append(resolved_stream)
                streams_result = sds_client.Communities.getCommunityStreams(
                    community_id, query.get('Value'), skip=100*i, count=100)
                i += 1
    else:
        for query in queries:
            streams_result = sds_client.Streams.getStreams(
                namespace_id, query.get('Value'), count=100)
            i = 1
            while streams_result != []:
                for stream in streams_result:
                    resolved_stream = sds_client.Streams.getResolvedStream(
                        namespace_id, stream.Id)
                    # Create prefix to avoid collisions
                    namespace = [n for n in namespaces if n.Id ==
                                 data_hub_appsettings.get("NamespaceId")]
                    resolved_stream.Prefix = f'{namespace[0].Description}'
                    resolved_streams.append(resolved_stream)
                streams_result = sds_client.Streams.getStreams(
                    namespace_id, query.get('Value'), skip=100*i, count=100)
                i += 1

    resolved_streams = removeDuplicates(resolved_streams)

    # Create types if they do not exist
    print('Creating types...')

    types = []
    type_id_set = set()
    for resolved_stream in resolved_streams:
        if f'{resolved_stream.Prefix}_{resolved_stream.Type.Id}' not in type_id_set:
            types.append(convertType(
                resolved_stream.Type, resolved_stream.Prefix))
            type_id_set.add(resolved_stream.Type.Id)

    response = pi_omf_client.omfRequest(
        OMFMessageType.Type, OMFMessageAction.Create, types)
    pi_omf_client.checkResponse(response, 'Error creating types')

    # Create containers
    print('Creating containers...')
    containers = []
    for resolved_stream in resolved_streams:
        containers.append(convertContainer(resolved_stream))

    response = pi_omf_client.omfRequest(
        OMFMessageType.Container, OMFMessageAction.Create, containers)
    pi_omf_client.checkResponse(response, 'Error creating containers')

    # Continuously send data
    print('Sending data...')
    queue = Queue(maxsize=0)
    t1 = threading.Thread(target=dataRetrievalTask, args=(
        queue, mode, sds_client, namespace_id, community_id, resolved_streams))
    t2 = threading.Thread(target=dataSendingTask, args=(
        queue, pi_omf_client))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
