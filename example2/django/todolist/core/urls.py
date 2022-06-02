from django.urls import path
from core.views import TodoListCreate, TodoRetrieveUpdateDestroy, home

urlpatterns = [
    path('api/todo/', TodoListCreate.as_view()),
    path('api/todo/<uuid:pk>/', TodoRetrieveUpdateDestroy.as_view()),
    path('', home)
]