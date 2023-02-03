class AuthProvider:
    '''
    Currently this is a static class for which the creds are initialized from outside (currently from the http request body).
    If we use Azure Key Vault later, `AuthProvider` can directly talk to the Key vault to get the required credentials.
    '''
    __storage_acc_name = ''
    __storage_acc_key = ''
    __sql_user = ''
    __sql_pwd = ''
    __sql_host = ''
    __registry_server = ''
    __registry_username = ''
    __registry_password = ''



    @classmethod
    def initialize(cls, storage_acc_name, storage_acc_key, sql_host, sql_user, sql_pwd):
        cls.__storage_acc_name = storage_acc_name
        cls.__storage_acc_key = storage_acc_key
        cls.__sql_user = sql_user
        cls.__sql_pwd = sql_pwd
        cls.__sql_host = sql_host

    @classmethod
    @property
    def storage_account_creds(cls):
        creds = (cls.__storage_acc_name, cls.__storage_acc_key)
        if any(map(lambda x: x == '', creds)):
            raise Exception('AuthProvider uninitialised')
        return creds

    @classmethod
    @property
    def sql_server_creds(cls):
        creds = (cls.__sql_host, cls.__sql_user, cls.__sql_pwd)
        if any(map(lambda x: x == '', creds)):
            raise Exception('AuthProvider uninitialised')
        return creds


    @classmethod
    def set_registry_creds(cls,reg_server,reg_usr,reg_pwd):
        cls.__registry_server = reg_server
        cls.__registry_username = reg_usr
        cls.__registry_password = reg_pwd
        
    @classmethod
    @property
    def registry_creds(cls):
        creds = (cls.__registry_server, cls.__registry_username,cls.__registry_password)
        if any(map(lambda x: x == '', creds)):
            raise Exception('AuthProvider uninitialised')
        return creds