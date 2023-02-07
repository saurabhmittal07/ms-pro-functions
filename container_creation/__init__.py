import logging
import json
import azure.functions as func

# from .container_creation import container_creation
from commons.azure_container_util import *
from commons.auth_provider import *

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        params = dict(req.get_json())
    except Exception as e:
        logging.exception(e)
        return func.HttpResponse('Unsupported request format', status_code=400)
    
    container_group = params.get('container_group')
    resource_group = params.get('resource_group')
    memory = params.get('memory')
    cpu = params.get('cpu')
    registry_server = params.get('registry_server')
    registry_username = params.get('registry_username')
    registry_password = params.get('registry_password')
    container_img = params.get('container_img')
    container_run_cmd = params.get('conatiner_run_cmd')

    missing = set()
    if container_group is None:
        missing.add('container_group')
    if resource_group is None:
        missing.add('resource_group')
    if memory is None:
        missing.add('memory')
    if cpu is None:
        missing.add('cpu')
    if registry_server is None:
        missing.add('registry_server')
    if registry_username is None:
        missing.add('registry_username')
    if registry_password is None:
        missing.add('registry_password')
    if container_img is None:
        missing.add('container_img')
    if container_run_cmd is None:
        missing.add('container_run_cmd')

    if len(missing) > 0:
        return func.HttpResponse(f"Missing {', '.join(missing)}", status_code=400)
    try:
        res={"msg":"Container spawning and killing successful!"}
        azure_obj = AzureContainerUtil(resource_group,container_group)
        AuthProvider.set_registry_creds(registry_server,registry_username,registry_password)
        azure_obj.create(container_group,memory,cpu,container_img,container_run_cmd)
        res['container_group'] = container_group
        res['container_name'] = container_group
        # azure_obj.delete()
        return func.HttpResponse(json.dumps(res))
    except Exception as e:
        print(e)
        return func.HttpResponse("Container cannot be created", status_code=400)

        