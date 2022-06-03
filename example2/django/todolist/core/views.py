from django.shortcuts import render
from rest_framework import generics

from core.models import Todo
from core.serializers import TodoSerializer


class TodoListCreate(generics.ListCreateAPIView):
    queryset = Todo.objects.order_by("date")
    serializer_class = TodoSerializer

class TodoRetrieveUpdateDestroy(generics.RetrieveUpdateDestroyAPIView):
    queryset = Todo.objects.order_by("date")
    serializer_class = TodoSerializer

def home(request):
    return render(request, "core/home.html")
