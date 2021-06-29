from django.db import models

# Create your models here.
class Todo(models.Model):
    id = models.UUIDField(primary_key=True)
    text = models.TextField(null=False)
    done = models.BooleanField(null=False)
    date_created = models.DateTimeField(null=False)

    def __repr__(self):  # pragma: no cover
        return f"Todo(id={self.id}, text={self.text}, done={self.done}, date_created={self.date_created})"
