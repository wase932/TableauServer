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
    # requestOptions = TSC.RequestOptions(pagesize=1000)
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
    allSites = list(TSC.Pager(server.sites))
    header("\nThere are {} sites on the server ".format(len(allSites)))
    columnsDF = ["SiteId", "SiteName", "SiteContentUrl", "SiteState"]
    allSitesDF = pd.DataFrame(columns=columnsDF)
    for site in allSites:
        allSitesDF.loc[len(allSitesDF)] = [site.id, site.name, site.content_url, site.state]

    # print(allSitesDF)

    #all users
    allUsers = list(TSC.Pager(server.users))
    print("\nThere are {} users on the site ".format(len(allUsers)))
    columnsDF = ["UserId", "UserName", "UserEmail", "LastLogin", "SiteRole"]
    allUsersDF = pd.DataFrame(columns=columnsDF)
    for user in allUsers:
        allUsersDF.loc[len(allUsersDF)] = [user.id, user.name, user.email, user.last_login, user.site_role]

    # print(allUsersDF)

    #all projects
    allProjects = list(TSC.Pager(server.projects))
    print("\nThere are {} projects on the site ".format(len(allProjects)))
    columnsDF = ["ProjectId", "ProjectName", "ProjectDescription", "ProjectContentPermissions", "ProjectParentId"]
    allProjectsDF = pd.DataFrame(columns=columnsDF)
    for project in allProjects:
        allProjectsDF.loc[len(allProjectsDF)] = [project._id, project.name, project.description, project.content_permissions, project.parent_id]

    # print(allProjectsDF)

    #all schedules
    allSchedules = list(TSC.Pager(server.schedules))
    print("\nThere are {} schedules on the site ".format(len(allSchedules)))
    columnsDF = ["ScheduleId", "ScheduleName", "ScheduleCreatedAt", "ScheduleType", "ScheduleState", "ScheduleNextRun", "ScheduleEndAt"]
    allSchedulesDF = pd.DataFrame(columns=columnsDF)
    for schedule in allSchedules:
        allSchedulesDF.loc[len(allSchedulesDF)] = [schedule.id, schedule.name, schedule._created_at, schedule.schedule_type, schedule._state, schedule._next_run_at, schedule._end_schedule_at]

    # print(allSchedulesDF)

    #views

    allViews = list(TSC.Pager(server.views))
    print("\nThere are {} views on the site ".format(len(allViews)))
    columnsDF = ["ViewId","ViewName", "ViewContentUrl", "TotalViews", "ViewImage", "ViewPreviewImage", "ViewWorkbookId", "ViewCSV"]
    allViewsDF = pd.DataFrame(columns=columnsDF)
    for view in allViews:
        allViewsDF.loc[len(allViewsDF)] = [view._id, view._name, view._content_url, view._total_views, view._image, view._preview_image, view._workbook_id, view._csv]

    #dataSources
    allDataSources = list(TSC.Pager(server.datasources))
    print("\nThere are {} datasources on the site ".format(len(allDataSources)))
    columnsDF = ["DataSourceName", "DataSourceProjectId", "DataSourceTags", "DataSourceCertified", "DataSourceContentUrl"]
    allDataSourcesDF = pd.DataFrame(columns=columnsDF)
    for dataSource in allDataSources:
        allDataSourcesDF.loc[len(allDataSourcesDF)] = [dataSource.name, dataSource.project_id, dataSource.tags, dataSource.certified, dataSource._content_url]

    #workbooks
    allWorkbooks = list(TSC.Pager(server.workbooks))
    print("\nThere are {} workbooks on the site ".format(len(allWorkbooks)))
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
