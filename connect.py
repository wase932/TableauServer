import tableauserverclient as TSC
import os

#load values from environmental variables:
username = os.environ.get('TableauUser')
password = os.environ.get('TableauPassword')
server = os.environ.get('TableauServer')

print(username)
print(password)

tableau_auth = TSC.TableauAuth(username, password)
server = TSC.Server(server)
server.add_http_options({'verify': False})

#sign in
server.auth.sign_in(tableau_auth)
print("\nSuccessfully authenticated {} to {} ".format(username, server.baseurl))

#all sites
all_sites, pagination_item = server.sites.get()

# print all the site names and ids
# print("id", "name", "content_url", "state")
for site in all_sites:
    print(site.id, site.name, site.content_url, site.state)

# with server.auth.sign_in(tableau_auth):
#     all_datasources, pagination_item = server.datasources.get()
#     print("\nThere are {} datasources on site: ".format(pagination_item.total_available))
#     print([datasource.name for datasource in all_datasources])