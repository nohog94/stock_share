from django.urls import path
from . import views

app_name = 'trade_user'
urlpatterns = [
    #path('daily_price_update', views.daily_price_update.as_view()),  # User에 관한 API를 처리하는 view로 Request를 넘김
    path('', views.UserView.as_view()),  # User에 관한 API를 처리하는 view로 Request를 넘김
    path('etf_price_update/', views.UserView.as_view(), name='etf_price_update'),
    #path('daily_price_update', views.daily_price_update.as_view()),  # User에 관한 API를 처리하는 view로 Request를 넘김
]
