# Generated by Django 3.2.13 on 2022-06-03 22:42

import datetime
from django.db import migrations, models
from django.utils.timezone import utc


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='todo',
            name='date',
            field=models.DateTimeField(default=datetime.datetime(2022, 6, 3, 22, 42, 5, 263221, tzinfo=utc)),
            preserve_default=False,
        ),
    ]
