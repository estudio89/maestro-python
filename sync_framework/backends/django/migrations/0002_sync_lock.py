# Generated by Django 3.1.3 on 2021-06-29 18:44

from django.db import migrations
from sync_framework.backends.django.contrib.migrate import commit_model_changes


def forwards_func(apps, schema_editor):
    SyncLockRecord = apps.get_model("sync_framework", "SyncLockRecord")
    SyncLockRecord.objects.create(key="sync_running")


def reverse_func(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("sync_framework", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(forwards_func, reverse_func),
    ]
