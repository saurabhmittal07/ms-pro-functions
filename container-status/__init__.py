from dotenv import dotenv_values
from commons import AzureContainerUtil
from mysql import connector as con
import azure.functions as func
from datetime import datetime
import json

# If container is 'running' or container is 'waiting'
# return ProcessState.PROCESSING
# If container is DEAD
#    If normal exit: return ProcessState.SUCCESS
#    If abnormal exist: return ProcessState.FAILED
class ProcessState:
    PROCESSING = 'PROCESSING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    UNKNOWN = 'UNKNOWN'

def get_process_state(acu, container_name):
    container = acu.get_container(container_name)
    container_state = container.instance_view.current_state
    state = container_state.state.lower()
    if state == 'terminated':
        if container_state.exit_code != 0:
            # NZEC from integration load API
            return ProcessState.FAILED
        return ProcessState.SUCCESS
    # still running
    return ProcessState.PROCESSING

def get_status(container_group, container_name, task_id):
    acu = AzureContainerUtil(dotenv_values('loadapi.conf')['RESOURCE_GROUP_NAME'], container_group)
    process_state = get_process_state(acu, container_name)
    # Nothing to do if processing or successful execution
    if process_state == ProcessState.PROCESSING:
        return process_state

    # container is dead so clean the container group
    acu.cleanup()

    # no update required if the container executed successfullly
    if process_state == ProcessState.SUCCESS:
        return process_state

    # container execution failed
    conf = dotenv_values('.env')
    cxn = con.connect(
        host = conf['MASTER_HOST'],
        user = conf['MASTER_USERNAME'],
        passwd = conf['MASTER_PASSWORD'],
        database = conf['MASTER_DB'],
        charset = 'ascii'
    )
    csr = cxn.cursor()
    # container finished execution
    csr.execute(f'SELECT task_status FROM subtask where task_id = {task_id}')
    row = csr.fetchone()
    if row is None:
        raise Exception('Invalid task_id')

    db_status = row[0]
    if db_status == 'PROCESSING' or db_status == 'SUCCESS':
        query = f'''UPDATE subtask SET
        task_status = "{process_state}",
        end_time = "{str(datetime.now())}",
        reason = "Container execution failed",
        reason_details = "Container exited with non-zero exit code"
        where task_id = {task_id}'''
        csr.execute(query)
    '''
    else:
        container failed after updating subtask state
        dont overwrite error message since message from container would be more
        useful than anything from here
    '''

    csr.execute(f'UPDATE task SET status = "FAILED" where id = {task_id}')
    cxn.commit()
    return process_state


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        params = dict(req.get_json())
    except Exception as e:
        return func.HttpResponse('Unsupported request format', status_code=400)

    task_id = params.get('task_id')
    container_group = params.get('container_group')
    container = params.get('container')

    missing = set()
    if task_id is None:
        missing.add('task_id')
    if container_group is None:
        missing.add('container_group')
    if container is None:
        missing.add('container')
    if len(missing) > 0:
        return func.HttpResponse(f'Missing {",".join(missing)}', status_code=400)

    try:
        status = get_status(container_group, container, task_id)
    except:
        status = ProcessState.UNKNOWN
    finally:
        resp = json.dumps({
            'task_id': task_id,
            'status': status,
        })
    if status == ProcessState.FAILED:
        return func.HttpResponse(resp, status_code=500)
    return func.HttpResponse(resp, status_code=200)
