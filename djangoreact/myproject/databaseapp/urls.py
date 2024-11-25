from django.urls import path
from . import views

urlpatterns = [
    path('snowflake-login/', views.snowflake_login, name='snowflake_login'),
    path('ssms-login/', views.SSMS_Login , name='SSMS_Login')
]
