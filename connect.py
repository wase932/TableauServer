import tableauserverclient as TSC
import os
import numpy as np
import pandas as pd

#load values from environmental variables:
username = os.environ.get('TableauUser')
password = os.environ.get('TableauPassword')
oldServer = os.environ.get('TableauOldServer')
newServer = os.environ.get('TableauNewServer')
siteName = os.environ.get('TableauSite')

#utility class
def header(msg):
    print(msg)

def getServerInstance(serverUrl, user, pwd, site):
    tableau_auth = TSC.TableauAuth(user, pwd, site)
    server = TSC.Server(serverUrl)
    server.add_http_options({'verify': False})
    #sign in
    server.auth.sign_in(tableau_auth)
    header("\nSuccessfully authenticated {} to {} ".format(user, server.baseurl))
    return server

class TableauServerData:
    def __init__(self, sites, users, projects, schedules, views, dataSources, workbooks):
        self.sites = sites
        self.users = users
        self.projects = projects
        self.schedules = schedules
        self.views = views
        self.dataSources = dataSources
        self.workbooks = workbooks

def getData(server):
    #all sites
    allSites, pagination_item = server.sites.get()
    header("\nThere are {} sites on the server ".format(pagination_item.total_available))
    columnsDF = ["SiteId", "SiteName", "SiteContentUrl", "SiteState"]
    allSitesDF = pd.DataFrame(columns=columnsDF)
    for site in allSites:
        allSitesDF.loc[len(allSitesDF)] = [site.id, site.name, site.content_url, site.state]

    # print(allSitesDF)

    #all users
    allUsers, pagination_item = server.users.get()
    print("\nThere are {} users on the server ".format(pagination_item.total_available))
    columnsDF = ["UserId", "UserName", "UserEmail", "LastLogin", "SiteRole"]
    allUsersDF = pd.DataFrame(columns=columnsDF)
    for user in allUsers:
        allUsersDF.loc[len(allUsersDF)] = [user.id, user.name, user.email, user.last_login, user.site_role]

    # print(allUsersDF)

    #all projects
    allProjects, pagination_item = server.projects.get()
    print("\nThere are {} projects on the server ".format(pagination_item.total_available))
    columnsDF = ["ProjectId", "ProjectName", "ProjectDescription", "ProjectContentPermissions", "ProjectParentId"]
    allProjectsDF = pd.DataFrame(columns=columnsDF)
    for project in allProjects:
        allProjectsDF.loc[len(allProjectsDF)] = [project._id, project.name, project.description, project.content_permissions, project.parent_id]

    # print(allProjectsDF)

    #all schedules
    allSchedules, pagination_item = server.schedules.get()
    print("\nThere are {} schedules on the server".format(pagination_item.total_available))
    columnsDF = ["ScheduleId", "ScheduleName", "ScheduleCreatedAt", "ScheduleType", "ScheduleState", "ScheduleNextRun", "ScheduleEndAt"]
    allSchedulesDF = pd.DataFrame(columns=columnsDF)
    for schedule in allSchedules:
        allSchedulesDF.loc[len(allSchedulesDF)] = [schedule.id, schedule.name, schedule._created_at, schedule.schedule_type, schedule._state, schedule._next_run_at, schedule._end_schedule_at]

    # print(allSchedulesDF)

    #views

    allViews, pagination_item = server.views.get(req_options=None, usage=False)
    print("\nThere are {} views on the server ".format(pagination_item.total_available))
    columnsDF = ["ViewId","ViewName", "ViewContentUrl", "TotalViews", "ViewImage", "ViewPreviewImage", "ViewWorkbookId", "ViewCSV"]
    allViewsDF = pd.DataFrame(columns=columnsDF)
    for view in allViews:
        allViewsDF.loc[len(allViewsDF)] = [view._id, view._name, view._content_url, view._total_views, view._image, view._preview_image, view._workbook_id, view._csv]

    #dataSources
    allDataSources, pagination_item = server.datasources.get(req_options=None)
    print("\nThere are {} datasources on the server".format(pagination_item.total_available))
    columnsDF = ["DataSourceName", "DataSourceProjectId", "DataSourceTags", "DataSourceCertified", "DataSourceContentUrl"]
    allDataSourcesDF = pd.DataFrame(columns=columnsDF)
    for dataSource in allDataSources:
        allDataSourcesDF.loc[len(allDataSourcesDF)] = [dataSource.name, dataSource.project_id, dataSource.tags, dataSource.certified, dataSource._content_url]

    #workbooks
    allWorkbooks, pagination_item = server.workbooks.get(req_options=None)
    print("\nThere are {} workbooks on the server".format(pagination_item.total_available))
    columnsDF = ["WorkbookId","WorkbookName","WorkbookOwnerId", "WorkbookConnections", "WorkbookProjectId"]
    allWorkbooksDF = pd.DataFrame(columns=columnsDF)
    for workbook in allWorkbooks:
        allWorkbooksDF.loc[len(allWorkbooksDF)] = [workbook._id, workbook.name, workbook.owner_id, workbook._connections, workbook.project_id]
        


    #SaveData
    result = TableauServerData(
                               allSitesDF
                              ,allUsersDF
                              ,allProjectsDF
                              ,allSchedulesDF
                              ,allViewsDF
                              ,allDataSourcesDF
                              ,allWorkbooksDF
                              )
    return result

oldServerPathToSave = "Data/NewServer/"
newServerPathToSave = "Data/OldServer/"

if not os.path.exists(oldServerPathToSave):
    os.makedirs(oldServerPathToSave)
if not os.path.exists(newServerPathToSave):
    os.makedirs(newServerPathToSave)

oldServer = getData(getServerInstance(oldServer, username, password, siteName))
oldServer.sites .to_csv(oldServerPathToSave + "Sites.csv")
oldServer.users.to_csv(oldServerPathToSave + "Users.csv")
oldServer.projects.to_csv(oldServerPathToSave + "Projects.csv")
oldServer.schedules.to_csv(oldServerPathToSave + "Schedules.csv")
oldServer.views.to_csv(oldServerPathToSave + "Views.csv")
oldServer.workbooks.to_csv(oldServerPathToSave + "Workbooks.csv")
oldServer.dataSources.to_csv(oldServerPathToSave + "Datasources.csv")

newServer = getData(getServerInstance(newServer, username, password, siteName))
newServer.sites .to_csv(newServerPathToSave + "Sites.csv")
newServer.users.to_csv(newServerPathToSave + "Users.csv")
newServer.projects.to_csv(newServerPathToSave + "Projects.csv")
newServer.schedules.to_csv(newServerPathToSave + "Schedules.csv")
newServer.views.to_csv(newServerPathToSave + "Views.csv")
newServer.workbooks.to_csv(newServerPathToSave + "Workbooks.csv")
newServer.dataSources.to_csv(newServerPathToSave + "Datasources.csv")
