from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
print("....get token....")
print("got token", credential.get_token('https://graph.microsoft.com/.default'))
