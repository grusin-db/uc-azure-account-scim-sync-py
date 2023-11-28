# Azure AAD to Databricks Account SCIM Sync

An example of end to end synchronization of the whitelisted Azure Active Directory groups and their members into the Databricks Account.

This python based application supports synchronization of:

- Users
- Service Principals
- Groups and their members

Yes, that means that group in a group, a.k.a. **nested groups are supported**!

When doing synchronization no users, service principals or groups are ever deleted. Synchronization only adds new security principals, or updates their attributes (like display name, or active flag) of already existing ones. Group members are fully sychronized to match what is present in AAD.

## Authentication

Sync tool needs authentication in order to connect to **AAD**, **Azure Databricks Account**, and **Azure Datalake Storage Gen2** for cache purpose.

It's highly recommended to first trying running the tool from your local user account, directly from your laptop/pc/mac/rasperypi :) and once it works switch over to devops and there run it as service principal. 

The authentication is decoupled from the sync logic, hence it's possible to change way of authentication (user vs. service principal auth) via change of enviroment settings/variables without need of changing any code. Please read more about it in respective sections below.

### Azure Active Directory or Entra (via graph API) Authentication

The list of security permission you will need:

- [User.Read.All](https://learn.microsoft.com/en-us/graph/permissions-reference#userreadall): gives access to read all the users

- [Group.Read.All](https://learn.microsoft.com/en-us/graph/permissions-reference#groupreadall): giving access to read all the group members

- [Application.Read.All](https://learn.microsoft.com/en-us/graph/permissions-reference#applicationreadall): giving access to read all the service principals
- or more broad [Directory.Read.All](https://learn.microsoft.com/en-us/graph/permissions-reference#directoryreadall): giving access to read all objects in AAD

In most - non heavy regulated - organizations, any user is by default given access to read whole AAD (this may vary if you are a contractor). Service Principals by default have no such access, hence request needs to be filed in in order to run the tool from devops.
  
The easiest way to check if your user account has access, is to go to [Azure Portal](http://portal.azure.com) and look for `Microsoft Entra ID`, there on the left, you should see `Users`, `Groups` and `App registrations`. If you are allowed to browse and use search funcitonality, that means that your user has all the needed access to query graph API!

To check if service principal has enough rights, please find your service principals inside of the `App registrations` and then click on the `API permissions` button on the left. In the blade on the right you should see list of the permissions, and status. If permissions are not listed there, or status says `Not granted...` that means you need to request access for your service principal.

Auth uses `azure-identity` python package which offsers [variety of authentication methods](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential), the two common ones used are:

- **Azure CLI**, allowing user to authenticate via device code, or via browser auth. Without need of storing any credentials in enviroment, or any config files. This is the best practice method for authentication from local machine.

  - To authenticate, run: `az login --tenant <TenantID>`. The auth **might** work if you just run `az login`, but then you may face issues with MFA related errors when you user account has access to multiple azure tenants.
  - Azure CLI installation manuals: [macOS](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-macos), [windows](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows?tabs=azure-cli), [linux](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-linux?pivots=apt). 
  - Note: when running from `azure devops`, hosted agents have it already preinstalled.
  - Troubleshoot auth errors:
    - Once authenticated, run `az account get-access-token --resource https://graph.microsoft.com/`, it should return `json` document with token. If you get MFA errors make sure you include  `--tenant` parameter in your `az login`

- **Environment** allowing authentication of service principals via environment variables. Auth method mainly used in devops. Set following [variables](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#environment-variables):
  - `AZURE_CLIENT_ID` - ID of a Microsoft Entra application
  - `AZURE_TENANT_ID` - ID of the application's Microsoft Entra tenant
  - `AZURE_CLIENT_SECRET` - one of the application's client secrets

The Environment variables take precedence over the Azure CLI auth.

### Databricks Account Authentication

You need to be logged in as [Account admin](https://docs.databricks.com/en/administration-guide/users-groups/index.html#who-can-manage-identities-in-databricks) in order to modify the identities in databricks account.

In most cases - if you are reading this manual - you should be already account admin. If in doubt, the easiest way to check if you are the admin is to go to [Account Console](https://accounts.azuredatabricks.net/) - if you are able to log in, you are admin

Auth uses `databricks-sdk` python package which offers [variety of authentication methods](https://github.com/databricks/databricks-sdk-py#authentication), the two common ones used are:

- **Azure CLI**, refer to section above (ADD auth) for details
  - Once logged in, additionally you will need to configure two environment variables:
    - `DATABRICKS_ACCOUNT_ID` - the ID of your databricks account (you can find it in [Account Console](https://accounts.azuredatabricks.net/))
    - `DATABRICKS_HOST` - endpoint url, static value always pointing to `https://accounts.azuredatabricks.net/`
  - Troubleshoot auth errors:
    - Once authenticated, run `az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d` (the `2f...c1d` is the ID of Azure Databricks Service, [not a secret value in anyway](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/service-prin-aad-token#--get-a-microsoft-entra-id-access-token-with-the-azure-cli)), it should return `json` document with token. If you get MFA errors make sure you include  `--tenant` parameter in your `az login`.
- **Environment** allowing authentication of service principals via environment variables. Auth method mainly used in devops. Set following [variables](https://github.com/databricks/databricks-sdk-py#azure-native-authentication):
  - `DATABRICKS_ACCOUNT_ID` - the ID of your databricks account (you can find it in [Account Console](https://accounts.azuredatabricks.net/))
  - `DATABRICKS_HOST` - endpoint url, static value always pointing to `https://accounts.azuredatabricks.net/`
  - `ARM_CLIENT_ID` - ID of a Microsoft Entra application
  - `ARM_TENANT_ID` - ID of the application's Microsoft Entra tenant
  - `ARM_CLIENT_SECRET` - one of the application's client secrets

The Environment variables take precedence over the Azure CLI auth.

## Azure Datalake Storage Gen2 (cache storage) Authentication

Sync tools uses cache of translation of AAD/Entra object_ids (example: 748fa79a-aaaa-40a5-9597-1b50cbb9a392) to Databricks Account IDs (1236734578586). This cache is automatically maintained and is self healing, hence it does not have any side effects except allowing scim sync tool to run 10x faster with it, than without.

For storing the cache tools needs to have access to a container and optionally a subfolder, where it can write it's cache files.

## Sync: Dry run

The sync tool offers two dry run modes, allowing to first see, and then approve changes:

- `--dry-run-security-principals`: allows to see which users, service principals and groups (not members!) would be added, or changed. At this point, if any changes are present the group membership synchornization will be skipped, and only can be continued once changes are applied
- `--dry-run-members`: applies any pending changes from above, and displays any group members that would be added or removed. In order to apply thse changes, run without any dry run modes!

## How to run syncing

### Configure authentication

Authentication approach dependson on what you are trying to do, and from which place. If you are planning to run it from your laptop (highly recommended for first few dry runs)

- use `az login` to get credentials for graph, databricks and storage
- set environment variables for databricks account: `DATABRICKS_ACCOUNT_ID` and `DATABRICKS_HOST` (most likely it will be `https://accounts.azuredatabricks.net/` if you are on azure)
- optinally, if you dont want to use `az login` credentials for specific resource, set enviroment variables, detail below:
  - (optional) set environment variables for graph api access:
    - `GRAPH_ARM_TENANT_ID`, `GRAPH_ARM_CLIENT_ID` and `GRAPH_ARM_CLIENT_SECRET`
    - the SPN will need to have Active Directory rights to read users, groups and service principals. Write rights are not required.
  - (optional) set environment variables for storage cache of graph/aad api ids to databricks ids
    - `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_CONTAINER`, `AZURE_STORAGE_TENANT_ID`, `AZURE_STORAGE_CLIENT_ID`, `AZURE_STORAGE_CLIENT_SECRET`
    - the defined SPN will need to have blob storage contributor rights on container.
    - `AZURE_STORAGE_CONTAINER` must point to valid container, can be followed by subpaths, for example: `datalake/some_folder/` would store cache in container `datalake` in folder `some_folder`
    - if not set, local file system will be used and cache data needs to be persisted by external tooling
  - (optional) set environment variables for databricks account access:
    - `DATABRICKS_ARM_CLIENT_ID`, `DATABRICKS_ARM_CLIENT_SECRET`,
  - in case both `GRAPH_ARM_...` and `DATABRICKS_ARM_...` credentials are the same, you can just use typical `ARM_...` env variables

- create JSON file containing the list of AAD groups you would like to sync, and save it to `groups_to_sync.json` (for reference, see `examples/groups_to_sync.json`)
-withut need of using specifc ones for graph and databricks account.
- `pip install azure_dbr_scim_sync` to install this package, you should pin in version number to maintain stability of the interface
- read the manual:

```shell
$ azure_dbr_scim_sync --help
Usage: azure_dbr_scim_sync [OPTIONS]

Options:
  --groups-json-file TEXT  list of AAD groups to sync (json formatted)
                           [required]
  --verbose                verbose information about changes
  --debug                  show API call
  --dry-run                dont make any changes, just display
  --help                   Show this message and exit.
```

- dry run: `azure_dbr_scim_sync --groups-json-file groups_to_sync.json --dry-run`
- if everything works ok, run the command again, but this time without `--dry-run`.
- feel free to increase verbosity with `--verbose`, or even debug API calls with `--debug`

## Limitations

- AAD disabled users or service principals are not being disabled in databricks account (needs change capture sync feature)
- AAD deleted users or service principals are not being deleted from databricks account (needs change capture sync feature)
- Only full sync is supported (needs change capture sync feature, obviously)

## Near time roadmap

- enable incremental graph api change capture, so that only group members and users who changed since last ran would be synced
- enable logging of changes into JSON and DELTA format (running from databricks workflow would be required)
- enable ability to run directly from databricks workflows, with simple installer

## Local development

- run `make dev`
- set env variables `export ARM_...=123`, or if you are using VS Code tests ASLO create `.envs` file:

```sh
# common for everything
ARM_TENANT_ID=...

# graph api access creds
GRAPH_ARM_CLIENT_ID=...
GRAPH_ARM_CLIENT_SECRET=...

# to keep cache of aad id <> dbr id mapping
AZURE_STORAGE_TENANT_ID=...
AZURE_STORAGE_CLIENT_ID=...
AZURE_STORAGE_CLIENT_SECRET=...
AZURE_STORAGE_ACCOUNT_NAME=...

# databricks account details
DATABRICKS_ACCOUNT_ID=...
DATABRICKS_HOST="https://accounts.azuredatabricks.net/"
```

- run `make install && make test` to install package locally and run tests
- run `your favorite text editor` and make code changes
- run `make fmt && make lint && make install && make test` to format, install package locally and test your changes
- `git commit && git push`
