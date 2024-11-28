from django.urls import path
from . import views

urlpatterns = [
    path('snowflake-login/', views.snowflake_login, name='snowflake_login'),
    path('SSMS_Login_And_FetchData/', views.SSMS_Login_And_FetchData , name='SSMS_Login_And_FetchData'),
    path('load_to_snowflake/', views.load_to_snowflake, name='load_to_snowflake')
]
