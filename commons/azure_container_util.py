from azure.common.client_factory import get_client_from_auth_file
from azure.common.client_factory import get_client_from_auth_file
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (ContainerGroup,
                                                 Container,
                                                 ImageRegistryCredential,
                                                 ContainerGroupNetworkProtocol,
                                                 ContainerGroupRestartPolicy,
                                                 ContainerPort,
                                                 EnvironmentVariable,
                                                 IpAddress,
                                                 Port,
                                                 ResourceRequests,
                                                 ResourceRequirements,
                                                 OperatingSystemTypes)
from .auth_provider import AuthProvider
import os
class AzureContainerUtil:
    AUTH_FILE_NAME = 'my.azureauth'
    def __init__(self, resource_group, container_group):
        secrets = AzureContainerUtil.AUTH_FILE_NAME
        if not os.path.exists(secrets):
            raise Exception("Auth file doesn't exist")
        os.environ['AZURE_AUTH_LOCATION'] = secrets
        self._aci_client = get_client_from_auth_file(ContainerInstanceManagementClient)
        self._resource_group = resource_group
        self._container_group = container_group
        

    @property
    def container_group(self) -> ContainerGroup:
        return self._aci_client.container_groups.get(self._resource_group, self._container_group)

    @container_group.setter
    def container_group(self, name):
        self._container_group = name

    @property
    def containers(self) -> list[Container]:
        return self.container_group.containers

    @property
    def client(self) -> ContainerInstanceManagementClient:
        return self._aci_client

    def get_container(self, container_name) -> Container:
        print('Container:', container_name)
        for container in self.container_group.containers:
            if container.name == container_name:
                return container
        raise ValueError(f'Container {container_name} is not found in group {self._container_group}')

    def get_conatiner_log(self,container_name):
        return self._aci_client.container.list_logs(self._resource_group,self._container_group,container_name)

    def cleanup(self):
        self._aci_client.container_groups.delete(self._resource_group, self._container_group)
    
    def check_container_creation_status(self,container_name):
        status=""
        while status!="succeeded" and status!="failed":
            container_group_obj = self.container_group
            status = container_group_obj.provisioning_state.lower()
        print(f"status: {status}")
        if status == 'succeeded':
            print(f"\nCreation of container '{container_name}' succeeded.")
            logs = self.get_conatiner_log(container_name)
            print(f"Logs for container '{container_name}':")
            print(f"{logs.content}")
            return True
        elif status == 'failed':
            print(f"\nCreation of container '{container_name}' failed. Provisioning state is: {status}")
            return False

    def run_task_based_container(self,container_name,memory,cpu,container_img,command):
        print(f"Creating container {container_name} inside container group '{self._container_group}' with start command '{command}'")

        registry_server,registry_username,registry_password = AuthProvider.registry_creds

        # Configure the container
        container_resource_requests = ResourceRequests(memory_in_gb=memory, cpu=cpu)
        container_resource_requirements = ResourceRequirements(requests=container_resource_requests)
        container = Container(name=container_name,
                            image=registry_server+"/"+container_img,
                            resources=container_resource_requirements,
                            command=command.split())
        credentials = ImageRegistryCredential(server=registry_server, username=registry_username, password=registry_password)
        # Configure the container group
        group = ContainerGroup(location='centralindia',
                            containers=[container],
                            os_type=OperatingSystemTypes.linux,
                            restart_policy=ContainerGroupRestartPolicy.never,
                            image_registry_credentials=[credentials])
        # Create the container group
        self._aci_client.container_groups.create_or_update(self._resource_group,self._container_group,group)

    def create(self,container_name,memory,cpu,container_img,command):
        self.run_task_based_container(container_name,memory,cpu,container_img,command)
        if self.check_container_creation_status(container_name)==False:
            raise Exception("Container creation Failed")