# Azure AAD to Databricks Account SCIM Sync

An example of end to end synchronization of the whitelisted Azure Active Directory groups and their members into the Databricks Account.

This python based application supports synchronization of:

- Users
- Service Principals
- Groups and their members

Yes, that means that group in a group, a.k.a. **nested groups are supported**!

When doing synchronization no users, service principals or groups are ever deleted. Synchronization only adds new security principals, or updates their attributes (like display name, or active flag) of already existing ones. Group members are fully sychronized to match what is present in AAD.

## Running Sync

Synchronization is based on a list of groups that you would like to sync. The list of groups for syncing can vary from one run to other, hence it's possible to just selectively sync few groups at a time, or run sync of all the groups in scope of your application. It goes without saying that sync of 5 groups (and their members) will take few seconds, while syncing of all users, service principals, and groups, can take few minutes.

Normally there are two usecases/patterns I have observed:

- Selective sync of newly to be onboarded groups, usually of adhoc nature, always needed when onboarding a new team to Unity Catalog. Normally you would like to sync all team groups and their members before running the onboarding process. This way that all the access could be set by your CI/CD automation (looking at you here [databricks terraform provider](https://registry.terraform.io/providers/databricks/databricks/latest/docs)). Without doing this step automation would most likely fail because the groups or other identities would not be yet presentin databricks account.
- Full sync, running on a schedule very few hours, that will be synchronizing all already onboarded teams and their groups.

The interface to faciciliate these two usecases is the same, the only difference is the list of groups, and time needed to perform the sync. 

To run the sync follow these steps:

- Authenticate: [Authentication steps described in section below](#authentication)
- Create `json` file containing the list of AAD groups names you would like to sync, and save it to `groups_to_sync.json` (for reference, see `examples/groups_to_sync.json`). Normally for the first run you should chose few groups only, it will make experience better.
- Run sync with dry run first: `azure_dbr_scim_sync --groups-json-file groups_to_sync.json --dry-run-security-principals --dry-run-members`.
- To get more information about the process add:
  - `--verbose` (logs information also about identities that did not change, by default only changes are logged) 
  - or `--debugg` (very detail, incl. api calls)
- Follow the prompts on the screen with regards to how to proceed with the [dry run](#dry-run-sync) levels.
  - If suggested list of changes look like what you would expect run without proposed `--dry-run-...` parameter(s)
- Repeat the steps again, but on bigger list of groups.

Reference of command line:

```shell
$ ✗ azure_dbr_scim_sync --help
Usage: azure_dbr_scim_sync [OPTIONS]

Options:
  --groups-json-file TEXT         list of AAD groups to sync (json formatted)
                                  [required]
  --verbose                       verbose information about changes
  --debug                         show API call
  --dry-run-security-principals   dont make any changes to users, groups or
                                  service principals, just display changes
  --dry-run-members               dont make any changes to group members, just
                                  display changes
  --worker-threads INTEGER        [default: 10]
  --save-graph-response-json TEXT
  --help                          Show this message and exit.
```

### Dry run sync

The sync tool offers two dry run modes, allowing to first see, and then approve changes:

- `--dry-run-security-principals`: allows to see which users, service principals and groups (not members!) would be added, or changed. At this point, if any changes are present the group membership synchronization will be skipped, and only can be continued once changes are applied.
- `--dry-run-members`: applies any pending changes from above, and displays any group members that would be added or removed. In order to apply group members changes, run without any dry run modes.

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
  - `DATABRICKS_ACCOUNT_ID` - see azure cli section
  - `DATABRICKS_HOST` - see azure cli section
  - `ARM_TENANT_ID` - ID of the application's Microsoft Entra tenant
  - `ARM_CLIENT_ID` - ID of a Microsoft Entra application
  - `ARM_CLIENT_SECRET` - one of the application's client secrets

The Environment variables take precedence over the Azure CLI auth.

## Azure Datalake Storage Gen2 (cache storage) Authentication

Sync tools uses cache of translation of AAD/Entra object_ids (example: 748fa79a-aaaa-40a5-9597-1b50cbb9a392) to Databricks Account IDs (1236734578586). This cache is automatically maintained and is self healing, hence it does not have any side effects except allowing scim sync tool to run 10x faster with it, than without.

For storing the cache tools needs to have access to a container and optionally a subfolder, where it can write it's cache files.

Auth uses `azure-identity` python package which offers [variety of authentication methods](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential), the two common ones used are:

- **Azure CLI**, refer to section above (ADD auth) for details:
  - Once logged in, additionally you will need to configure two environment variables:
    - `AZURE_STORAGE_ACCOUNT_NAME` - the name of the azure storage account, for example: `myscimsync`
    - `AZURE_STORAGE_CONTAINER` - the name of the container, for example: `data`. The name can be followed by a subfolder for example `data\some\folder` will cause cache to be written to container `data` and then placed in `some\folder` folder
- **Environment** allowing authentication of service principals via environment variables. Auth method mainly used in devops. Set following [variables](https://github.com/fsspec/adlfs#setting-credentials):
  - `AZURE_STORAGE_ACCOUNT_NAME` - see azure cli section
  - `AZURE_STORAGE_CONTAINER` - see azure cli section
  - `AZURE_STORAGE_TENANT_ID` - ID of the application's Microsoft Entra tenant
  - `AZURE_STORAGE_CLIENT_ID` - ID of a Microsoft Entra application
  - `AZURE_STORAGE_CLIENT_SECRET` - one of the application's client secrets

The Environment variables take precedence over the Azure CLI auth.

## Limitations

- AAD disabled Users and Service Principals are only disabled in account console when they are being synced, as in being member of the group that is curently being synced. Hence if disabled user gets also removed from the groups, then these users wont be synced back to account console anymore.
  - Workaround for this is to disable User or Service Princial in AAD and keep them as members of groups they used to be in. This way next full sync will disable them in account console.

## Near time roadmap

- enable incremental pgraph api change feed(https://learn.microsoft.com/en-us/graph/webhooks), so that only group members and users/spns who changed since last ran would be synced
- enable logging of changes into JSON and DELTA format (running from databricks workflow would be required)
- enable ability to run directly from databricks workflows, with simple installer

## Building a package / Local development

Currently package for this code is not being distributed, you need to build it yourself, follow these steps to do so:

- run `make dev` (will put you in `.venv`)
- run `make dist`
- in `dist` folder package placed will be
- run `make install` to install package
- if you are in `.venv`, you should be able to run `azure_dbr_scim_sync --help`
- if you are not in `.venv` follow on screen instructions regarding placement of the cli command(s)

