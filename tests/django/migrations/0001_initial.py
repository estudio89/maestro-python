# Generated by Django 3.1.3 on 2021-06-22 22:17

from django.db import migrations, models
from typing import List, Tuple


class Migration(migrations.Migration):

    initial = True

    dependencies: "List[Tuple[str, str]]" = []

    operations = [
        migrations.CreateModel(
            name="Item",
            fields=[
                ("id", models.UUIDField(primary_key=True, serialize=False)),
                ("name", models.TextField()),
                ("version", models.TextField()),
            ],
        ),
    ]
