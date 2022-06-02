from django.db import models

class Todo(models.Model):
    id = models.UUIDField(primary_key=True)
    text = models.TextField(null=False)
    done = models.BooleanField(default=False)
