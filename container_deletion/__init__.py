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
    
    missing = set()
    if container_group is None:
        missing.add('container_group')
    if resource_group is None:
        missing.add('resource_group')
    

    if len(missing) > 0:
        return func.HttpResponse(f"Missing {', '.join(missing)}", status_code=400)
    try:
        res={"msg":"Container deletion successful!"}
        azure_obj = AzureContainerUtil(resource_group,container_group)
        azure_obj.delete()
        return func.HttpResponse(json.dumps(res))
    except Exception as e:
        print(e)
        return func.HttpResponse("Container cannot be deleted", status_code=400)

        