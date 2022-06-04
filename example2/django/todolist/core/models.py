from django.db import models
from django.utils import timezone


class Todo(models.Model):
    id = models.UUIDField(primary_key=True)
    text = models.TextField(null=False)
    done = models.BooleanField(default=False)
    date = models.DateTimeField(null=False)

    def save(self, *args, **kwargs):
        if not self.date:
            self.date = timezone.now()
        super().save(*args, **kwargs)
