from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from itertools import repeat
from queue import Queue
from typing import Any
import datetime
import json
import logging
import threading
import time
import traceback

from adh_sample_library_preview import (
    ADHClient, Namespace, SdsBoundaryType, SdsResultPage, SdsType)
from CustomResolvedStream import CustomResolvedStream
from PIOMFClient import (PIOMFClient, OMFMessageAction, OMFMessageType)


class Mode(Enum):
    NORMAL = 0
    BACKFILL_ALL = 1
    BACKFILL_N_DAYS = 2


# Global variables
send_period = 30               # maximum amount of time to wait before sending the next OMF data message
max_events = 5000              # maximum number of events to send per OMF data message
data_request_period = 5        # number of seconds to wait before next request for data from Cds
mode = Mode.BACKFILL_N_DAYS    # backfill mode
days_to_backfill = 7           # number of days to backfill if mode is BACKFILL_N_DAYS
max_send_retries = -1          # maximum number of send retries to attempt. Set to -1 for endless retries
retries_before_throttle = 3    # number of repeat requests to attempt before throttling
max_throttle_time = 60         # seconds to throttle for repeat requests
throw_on_bad = True            # whether to throw an exception on a bad response
log_file_name = 'logfile.txt'  # log file name
#level = logging.DEBUG         # use to troubleshoot the sample
level = logging.INFO           # use for record keeping

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
        logging.ERROR(f'Error: {str(error)}')
        logging.ERROR(f'Could not open/read appsettings.json')
        exit()

    return appsettings

def output(level, message):
    """Prints the given message to the console as well as the log file, with the given log level"""

    logging.log(level, message)
    print(message)

def removeDuplicates(list: list[Any]):
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


def convertContainer(stream: CustomResolvedStream, prefix: str):
    """Convert an SdsStream into an OMF Container"""

    return {
        'id': f'{prefix}_{stream.Id}',
        'name': stream.Name,
        'typeid': f'{prefix}_{stream.Type.Id}',
        'description': stream.Description,
        'datasource': 'Data Hub'
    }


def convertData(container_id: str, data: Any):
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


def getResolvedStreams(sds_client: ADHClient, query: str, namespace_id: str = None, namespaces: list[Namespace] = None, community_id: str = None, count_per_page: int = 100) -> list[CustomResolvedStream]:
    """Helper function for getting a list of resolved streams from a query"""
    resolved_streams = []

    if community_id:
        streams_result = sds_client.Communities.getCommunityStreams(
            community_id, query.get('Value'), count=count_per_page)
        i = 1
        while streams_result != []:
            for stream_search_result in streams_result:
                resolved_stream = sds_client.SharedStreams.getResolvedStream(
                    stream_search_result.TenantId, stream_search_result.NamespaceId, stream_search_result.CommunityId, stream_search_result.Id)
                resolved_stream = CustomResolvedStream.fromResolvedStream(
                    resolved_stream)

                # Append extra properties to stream that will be used later
                resolved_stream.TenantId = stream_search_result.TenantId
                resolved_stream.TenantName = stream_search_result.TenantName
                resolved_stream.NamespaceId = stream_search_result.NamespaceId
                resolved_stream.CommunityId = stream_search_result.CommunityId
                for property in resolved_stream.Type.Properties:
                    if property.IsKey:
                        resolved_stream.IndexId = property.Id

                # Create prefix to avoid collisions
                resolved_stream.Prefix = f'{stream_search_result.TenantName}'
                resolved_streams.append(resolved_stream)
            streams_result = sds_client.Communities.getCommunityStreams(
                community_id, query.get('Value'), skip=count_per_page*i, count=count_per_page)
            i += 1
    else:
        streams_result = sds_client.Streams.getStreams(
            namespace_id, query.get('Value'), count=count_per_page)
        i = 1
        while streams_result != []:
            for stream in streams_result:
                resolved_stream = sds_client.Streams.getResolvedStream(
                    namespace_id, stream.Id)
                resolved_stream = CustomResolvedStream.fromResolvedStream(
                    resolved_stream)

                # Append extra properties to stream that will be used later
                resolved_stream.NamespaceId = namespace_id
                for property in resolved_stream.Type.Properties:
                    if property.IsKey:
                        resolved_stream.IndexId = property.Id

                # Create prefix to avoid collisions
                namespace = [n for n in namespaces if n.Id == namespace_id]
                resolved_stream.Prefix = f'{namespace[0].Description}'
                resolved_streams.append(resolved_stream)
            streams_result = sds_client.Streams.getStreams(
                namespace_id, query.get('Value'), skip=count_per_page*i, count=count_per_page)
            i += 1

    return resolved_streams


def getWindowValuesPaged(sds_client: ADHClient, resolved_stream: CustomResolvedStream, start: str,
                         end: str, count: int, continuation_token: str = '', value_class: type = None,
                         filter: str = '', boundary_type: SdsBoundaryType = None, start_boundary_type: SdsBoundaryType = None,
                         end_boundary_type: SdsBoundaryType = None) -> SdsResultPage:
    """Helper function for calling the appropriate version of getWindowValuesPaged"""
    if resolved_stream.CommunityId:
        return sds_client.SharedStreams.getWindowValuesPaged(
            resolved_stream.TenantId, resolved_stream.NamespaceId, resolved_stream.CommunityId, resolved_stream.Id, start, end, count, continuation_token, value_class, filter, boundary_type, start_boundary_type, end_boundary_type)
    else:
        return sds_client.Streams.getWindowValuesPaged(
            resolved_stream.NamespaceId, resolved_stream.Id, start, end, count, continuation_token, value_class, filter, boundary_type, start_boundary_type, end_boundary_type)


def queueStreamData(queue: Queue, resolved_stream: CustomResolvedStream, sds_client: ADHClient, start_index: str, end_index: str, start_boundary: SdsBoundaryType, test: bool):
    """Query for data from a stream and add it to the queue"""

    if start_index is None:
        return (None, start_boundary)

    new_start_index = start_index
    try:
        results_page = getWindowValuesPaged(sds_client, resolved_stream, start=start_index, end=end_index,
                                            count=250000, start_boundary_type=start_boundary, end_boundary_type=SdsBoundaryType.Exact)

        for result in results_page.Results:
            stream_id = f'{resolved_stream.Prefix}_{resolved_stream.Id}'
            if result[resolved_stream.IndexId] != start_index:
                queue.put(convertData(stream_id, result))

        if results_page.Results != []:
            new_start_index = results_page.Results[-1][resolved_stream.IndexId]
            start_boundary = SdsBoundaryType.Inside

        while not results_page.end():
            results_page = getWindowValuesPaged(sds_client, resolved_stream, start=start_index,
                                                end=end_index, count=250000, continuation_token=results_page.ContinuationToken)

            for result in results_page.Results:
                queue.put(convertData(stream_id, result))

            if results_page.Results != []:
                new_start_index = results_page.Results[-1][resolved_stream.IndexId]

    except Exception as ex:
        logging.ERROR((f"Encountered Error: {ex}"))
        traceback.print_exc()
        if test:
            raise ex

    return (new_start_index, start_boundary)


def dataRetrievalTask(queue: Queue, mode: Mode, sds_client: ADHClient, resolved_streams: list[CustomResolvedStream], test: bool):
    """Task for retrieving data from Data Hub and adding it to the queue"""

    start_indexes = []
    start_boundaries = [SdsBoundaryType.Exact] * len(resolved_streams)

    # Get start indexes for each stream
    for resolved_stream in resolved_streams:
        if mode == Mode.BACKFILL_ALL:
            first_value = sds_client.SharedStreams.getFirstValue(resolved_stream.TenantId, resolved_stream.NamespaceId, resolved_stream.CommunityId,
                                                                 resolved_stream.Id) if resolved_stream.CommunityId else sds_client.Streams.getFirstValue(resolved_stream.NamespaceId, resolved_stream.Id)
            if first_value is not None:
                start_indexes.append(first_value[resolved_stream.IndexId])
            else:
                start_indexes.append(None)
        elif mode == Mode.BACKFILL_N_DAYS:
            start_indexes.append((datetime.datetime.utcnow()
                                  - datetime.timedelta(days=days_to_backfill)).isoformat() + 'Z')
        else:
            start_indexes.append(
                (datetime.datetime.utcnow()).isoformat() + 'Z')

    # Retrieve data and add to queue forever
    count = 0
    while (not test) or (count == 0):

        results = []
        with ThreadPoolExecutor() as pool:
            results = pool.map(queueStreamData, repeat(queue), resolved_streams, repeat(sds_client),
                               start_indexes, repeat(datetime.datetime.utcnow().isoformat() + 'Z'), start_boundaries, repeat(test))

        for index, result in enumerate(results):
            start_indexes[index] = result[0]
            start_boundaries[index] = result[1]

        time.sleep(data_request_period)
        count += 1


def dataSendingTask(queue: Queue, pi_omf_client: PIOMFClient, test: bool):
    """Task for getting data from the queue and sending it to PI"""

    timer = time.time()
    event_count = 0

    while (not test) or (queue.qsize() > 0 or event_count == 0):
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
            throttle_time = 0
            retries = 0
            while not sent:
                try:
                    response = pi_omf_client.omfRequest(
                        OMFMessageType.Data, OMFMessageAction.Update, consolidated_data)
                    pi_omf_client.verifySuccessfulResponse(
                        response, 'Error updating data', throw_on_bad)
                    sent = True

                    # Throttle if failing
                    time.sleep(throttle_time)
                    retries += 1
                    throttle_time = throttle_time + 5 \
                        if retries > retries_before_throttle and throttle_time < max_throttle_time \
                        else throttle_time
                    
                    # Exit if max retries exceeded and not disabled
                    if max_send_retries != -1 and retries > max_send_retries:
                        break
                except Exception as ex:
                    logging.ERROR((f"Encountered Error: {ex}"))
                    traceback.print_exc()
                    if test:
                        raise ex

            logging.debug(
                f'Queue size: {queue.qsize()}, Events/second: {event_count/(time.time()-timer)}')

            # Reset timer
            timer = time.time()


def main(test=False):

    output(logging.INFO, 'Starting!')

    # Read appsettings
    output(logging.INFO, 'Reading appsettings...')
    appsettings = getAppsettings()
    data_hub_appsettings = appsettings.get('DataHub')
    pi_appsettings = appsettings.get('PI')
    queries = appsettings.get('Queries')

    # Create PI OMF client
    output(logging.INFO, 'Creating a PI OMF client...')
    pi_omf_client = PIOMFClient(
        pi_appsettings.get('Resource'),
        pi_appsettings.get('DataArchiveName'),
        pi_appsettings.get('Username'),
        pi_appsettings.get('Password'),
        pi_appsettings.get('OMFVersion', '1.2'),
        pi_appsettings.get('VerifySSL', True),
        logging_enabled=True
    )

    # Create a Data Hub client
    output(logging.INFO, 'Creating a Data Hub client...')
    sds_client = ADHClient(
        data_hub_appsettings.get('ApiVersion'),
        data_hub_appsettings.get('TenantId'),
        data_hub_appsettings.get('Resource'),
        data_hub_appsettings.get('ClientId'),
        data_hub_appsettings.get('ClientSecret'),
        logging_enabled=True)
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
    output(logging.INFO, 'Collecting a list of streams to transfer...')
    resolved_streams = []
    for query in queries:
        resolved_streams += getResolvedStreams(
            sds_client, query, namespace_id, namespaces, community_id)

    resolved_streams = removeDuplicates(resolved_streams)

    # Create types if they do not exist
    output(logging.INFO, 'Creating types...')
    types = []
    type_id_set = set()
    for resolved_stream in resolved_streams:
        if f'{resolved_stream.Prefix}_{resolved_stream.Type.Id}' not in type_id_set:
            types.append(convertType(
                resolved_stream.Type, resolved_stream.Prefix))
            type_id_set.add(resolved_stream.Type.Id)

    response = pi_omf_client.omfRequest(
        OMFMessageType.Type, OMFMessageAction.Create, types)
    pi_omf_client.verifySuccessfulResponse(response, 'Error creating types', throw_on_bad)

    # Create containers
    output(logging.INFO, 'Creating containers...')
    containers = []
    for resolved_stream in resolved_streams:
        containers.append(convertContainer(
            resolved_stream, resolved_stream.Prefix))

    response = pi_omf_client.omfRequest(
        OMFMessageType.Container, OMFMessageAction.Create, containers)
    pi_omf_client.verifySuccessfulResponse(
        response, 'Error creating containers', throw_on_bad)

    # Continuously send data
    output(logging.INFO, 'Sending data...')
    queue = Queue(maxsize=0)
    t1 = threading.Thread(target=dataRetrievalTask, args=(
        queue, mode, sds_client, resolved_streams, test), daemon=True)
    t2 = threading.Thread(target=dataSendingTask, args=(
        queue, pi_omf_client, test), daemon=True)
    t1.start()
    t2.start()

    while t1.is_alive() and t2.is_alive():
        if not test and input('Type "exit" to quit application: ').casefold() == 'exit':
            break


if __name__ == "__main__":

    # Set up the logger
    logging.basicConfig(filename=log_file_name, encoding='utf-8', level=level, datefmt='%Y-%m-%d %H:%M:%S', format= '%(asctime)s %(module)16s,line: %(lineno)4d %(levelname)8s | %(message)s')
    
    main()
