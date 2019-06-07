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
allSites, pagination_item = server.sites.get()
print("\nThere are {} sites on the server ".format(pagination_item.total_available))

for site in allSites:
    print(site.id, site.name, site.content_url, site.state)

#all users
allUsers, pagination_item = server.users.get()
print("\nThere are {} users on site ".format(pagination_item.total_available))
print("UserId, UserName, UserEmail, LastLogin")
for user in allUsers:
    print(user.id, user.name, user.email, user.last_login)


#all projects
allProjects, pagination_item = server.projects.get()
print("\nThere are {} projects on site ".format(pagination_item.total_available))
for project in allProjects:
    print(project._id, project.name, project.description, project.content_permissions, project.parent_id)

#all schedules
allSchedules, pagination_item = server.schedules.get()
print("\nThere are {} schedules on site ".format(pagination_item.total_available))
for schedule in allSchedules:
    print(schedule.id, schedule.name, schedule._created_at, schedule.schedule_type, schedule._state, schedule._next_run_at, schedule._end_schedule_at)

allDataSources, pagination_item = server.datasources.get()
print("\nThere are {} datasources on site ".format(pagination_item.total_available))
for dataSource in allDataSources:
    print(dataSource._id, dataSource.name, dataSource._content_url, dataSource._created_at, dataSource._updated_at, dataSource._project_id, dataSource._project_name, dataSource._datasource_type)

allViews, pagination_item = server.views.get()
print("\nThere are {} views on site ".format(pagination_item.total_available))
for view in allViews:
    print(view._id, view._name, view._content_url, view._total_views, view._image, view._preview_image, view._workbook_id, view._csv)

# with server.auth.sign_in(tableau_auth):
#     all_datasources, pagination_item = server.datasources.get()
#     print("\nThere are {} datasources on site: ".format(pagination_item.total_available))
#     print([datasource.name for datasource in all_datasources])