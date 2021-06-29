from django.db import models

# Create your models here.
class Item(models.Model):
	id = models.UUIDField(primary_key=True)
	name = models.TextField(null=False)
	version = models.TextField(null=False)

	def __repr__(self): # pragma: no cover
		return f"Item(id={self.id}, name={self.name}, version={self.version})"
