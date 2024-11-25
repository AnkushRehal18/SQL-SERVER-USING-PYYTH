from django.contrib import admin
from django.urls import path, include
from django.http import HttpResponse

# Optional simple view for the home page
def home(request):
    return HttpResponse("Welcome to the homepage! Visit /api/snowflake-login/ to login.")

urlpatterns = [
    path('admin/', admin.site.urls),  # Admin URL
    path('', home, name='home'),  # Home URL
    path('api/', include('databaseapp.urls')),  # Include URLs from `databaseapp`
]
